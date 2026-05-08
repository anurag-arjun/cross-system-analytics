"""Etherscan V2 unified API client.

Verified 2026-05-08: a single ETHERSCAN_API_KEY works across Ethereum, Base,
Arbitrum, Optimism, Polygon (and many other chains) at api.etherscan.io/v2/api
with the chainid query parameter selecting the chain.

Free tier rate limit: 5 req/sec. We throttle to 4 rps default to leave headroom.

DNS note: some Linux setups (systemd-resolved with negative caching) return
NXDOMAIN for api.etherscan.io while resolving other hosts fine. If you hit
"Name or service not known" errors, either reconfigure your local resolver
to use 1.1.1.1 / 8.8.8.8, or add a static /etc/hosts entry for the
api.etherscan.io IPs (e.g. 217.79.243.34).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

V2_BASE = "https://api.etherscan.io/v2/api"

# Chain slug -> EIP-155 chainId. Only the chains in BD scope.
CHAIN_IDS: dict[str, int] = {
    "ethereum": 1,
    "base": 8453,
    "arbitrum": 42161,
    "optimism": 10,
    "polygon": 137,
}


class EtherscanError(RuntimeError):
    pass


class NotVerified(EtherscanError):
    """Raised when Etherscan reports the contract source is not verified.

    The fetcher should fall through to Sourcify in this case.
    """


class EtherscanV2Client:
    def __init__(
        self,
        api_key: str,
        rate_limit_rps: float = 4.0,
        timeout_s: float = 30.0,
    ) -> None:
        self._key = api_key
        self._client = httpx.Client(timeout=timeout_s)
        self._min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0
        self._last_call = 0.0
        self.calls = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "EtherscanV2Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        delta = time.monotonic() - self._last_call
        if delta < self._min_interval:
            time.sleep(self._min_interval - delta)
        self._last_call = time.monotonic()

    def get_abi(self, chain: str, address: str) -> str:
        """Return the JSON-encoded ABI string for *address* on *chain*.

        Raises NotVerified if the contract source isn't verified, EtherscanError
        on transport / API errors.
        """
        chainid = CHAIN_IDS.get(chain.lower())
        if chainid is None:
            raise EtherscanError(f"unsupported chain: {chain}")

        params = {
            "chainid": chainid,
            "module": "contract",
            "action": "getabi",
            "address": address,
            "apikey": self._key,
        }

        for attempt in range(4):
            self._throttle()
            self.calls += 1
            try:
                resp = self._client.get(V2_BASE, params=params)
            except httpx.ConnectError as e:
                # DNS NXDOMAIN, refused, network unreachable — retrying is
                # pointless within the same process, and "Name or service
                # not known" specifically indicates a broken local resolver
                # (see module docstring for the workaround).
                raise EtherscanError(f"connect error: {e}") from e
            except httpx.HTTPError as e:
                if attempt == 3:
                    raise EtherscanError(f"transport error: {e}") from e
                time.sleep(2**attempt)
                continue

            if resp.status_code == 429:
                wait = 2**attempt
                logger.warning("etherscan 429, backing off %.1fs", wait)
                time.sleep(wait)
                continue

            try:
                data = resp.json()
            except ValueError as e:
                raise EtherscanError(f"non-JSON response (status {resp.status_code})") from e

            status = str(data.get("status", ""))
            result = data.get("result", "")
            if status == "1":
                return result  # JSON-encoded ABI string
            if isinstance(result, str) and "not verified" in result.lower():
                raise NotVerified(f"{chain}/{address}: {result}")
            if isinstance(result, str) and "rate limit" in result.lower():
                wait = 2**attempt
                logger.warning("etherscan rate limit message, backing off %.1fs", wait)
                time.sleep(wait)
                continue
            raise EtherscanError(f"{chain}/{address}: status={status} result={result}")

        raise EtherscanError(f"{chain}/{address}: exhausted retries")

"""Sourcify fallback for verified ABIs.

Sourcify is a free, no-auth verified-contract repository. We use it when
Etherscan reports the source is not verified or our key is not configured.

API shape: https://sourcify.dev/server/files/any/{chainid}/{address}
returns a JSON envelope listing files; we parse `metadata.json` for the ABI.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SOURCIFY_BASE = "https://sourcify.dev/server"


class SourcifyError(RuntimeError):
    pass


class NotFound(SourcifyError):
    """Sourcify has no record of this contract."""


class SourcifyClient:
    def __init__(self, timeout_s: float = 30.0) -> None:
        self._client = httpx.Client(timeout=timeout_s)
        self.calls = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SourcifyClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def get_abi(self, chainid: int, address: str) -> str:
        """Return JSON-encoded ABI string. Raises NotFound if not in Sourcify."""
        url = f"{SOURCIFY_BASE}/files/any/{chainid}/{address}"
        self.calls += 1
        resp = self._client.get(url)
        if resp.status_code == 404:
            raise NotFound(f"{chainid}/{address}")
        resp.raise_for_status()

        envelope = resp.json()
        files = envelope.get("files", [])
        for entry in files:
            name = entry.get("name", "")
            if name.endswith("metadata.json"):
                try:
                    metadata = json.loads(entry["content"])
                except (KeyError, ValueError) as e:
                    raise SourcifyError(f"malformed metadata for {address}: {e}") from e
                abi = metadata.get("output", {}).get("abi")
                if abi:
                    return json.dumps(abi)
        raise SourcifyError(f"no ABI in metadata for {address}")

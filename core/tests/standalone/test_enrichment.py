from decimal import Decimal

import pytest

from core.adapters.prices.coingecko import CoinGeckoPriceAdapter, TokenPrice
from core.enrichment.metadata import TokenMetadata, TokenMetadataLoader
from core.enrichment.prices import PriceFetcher


class TestTokenMetadataLoader:
    def test_load_hardcoded(self):
        loader = TokenMetadataLoader()
        tokens = loader.load_hardcoded()
        assert len(tokens) > 0

        weth = next((t for t in tokens if t.symbol == "WETH" and t.chain == "ethereum"), None)
        assert weth is not None
        assert weth.decimals == 18
        assert weth.address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

    def test_hardcoded_usdc(self):
        loader = TokenMetadataLoader()
        tokens = loader.load_hardcoded()
        usdc = next((t for t in tokens if t.symbol == "USDC" and t.chain == "base"), None)
        assert usdc is not None
        assert usdc.decimals == 6

    def test_is_stablecoin(self):
        loader = TokenMetadataLoader()
        assert loader.is_stablecoin("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        assert loader.is_stablecoin("0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48")
        assert not loader.is_stablecoin("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")

    def test_is_stablecoin_unknown(self):
        loader = TokenMetadataLoader()
        assert not loader.is_stablecoin("0xunknown")

    def test_token_metadata_dataclass(self):
        token = TokenMetadata(
            address="0xabc",
            chain="base",
            symbol="TEST",
            decimals=18,
            name="Test Token",
        )
        assert token.symbol == "TEST"
        assert token.decimals == 18


class MockCoinGeckoAdapter:
    def __init__(self, responses: dict[str, dict]):
        self.responses = responses
        self.calls: list[tuple[str, list[str]]] = []

    def fetch_prices(self, chain: str, addresses: list[str]) -> list[TokenPrice]:
        self.calls.append((chain, addresses))
        key = f"{chain}:{','.join(addresses)}"
        return self.responses.get(key, [])

    def close(self):
        pass


class MockClickHouseClient:
    def __init__(self):
        self.inserts: list[tuple[str, list]] = []

    def insert(self, table: str, data: list) -> None:
        self.inserts.append((table, data))

    def close(self) -> None:
        pass


class TestPriceFetcher:
    def test_update_prices(self):
        adapter = MockCoinGeckoAdapter(
            {
                "ethereum:0xaaa": [
                    TokenPrice(
                        token_address="0xaaa",
                        chain="ethereum",
                        timestamp=None,
                        price_usd=Decimal("3500.00"),
                        source="coingecko",
                        volume_24h_usd=Decimal("1000000"),
                    )
                ]
            }
        )
        mock_ch = MockClickHouseClient()
        fetcher = PriceFetcher(adapter=adapter, client=mock_ch)
        count = fetcher.update_prices("ethereum", ["0xaaa"])
        assert count == 1
        assert len(adapter.calls) == 1
        assert adapter.calls[0] == ("ethereum", ["0xaaa"])
        assert len(mock_ch.inserts) == 1
        assert mock_ch.inserts[0][0] == "token_prices"

    def test_update_all_chains(self):
        adapter = MockCoinGeckoAdapter(
            {
                "ethereum:0xaaa": [
                    TokenPrice(
                        token_address="0xaaa",
                        chain="ethereum",
                        timestamp=None,
                        price_usd=Decimal("3500.00"),
                        source="coingecko",
                        volume_24h_usd=None,
                    )
                ],
                "base:0xbbb": [
                    TokenPrice(
                        token_address="0xbbb",
                        chain="base",
                        timestamp=None,
                        price_usd=Decimal("1.00"),
                        source="coingecko",
                        volume_24h_usd=None,
                    )
                ],
            }
        )
        mock_ch = MockClickHouseClient()
        fetcher = PriceFetcher(adapter=adapter, client=mock_ch)
        tokens = {
            "ethereum": ["0xaaa"],
            "base": ["0xbbb"],
        }
        count = fetcher.update_all_chains(tokens)
        assert count == 2
        assert len(adapter.calls) == 2
        assert len(mock_ch.inserts) == 2


class TestCoinGeckoPriceAdapter:
    def test_fetch_prices_mock(self, monkeypatch):
        import json
        from datetime import datetime, timezone

        class MockResponse:
            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self._data

        class MockClient:
            def __init__(self):
                self.requests = []

            def get(self, url, params=None):
                self.requests.append((url, params))
                return MockResponse(
                    {
                        "0xaaa": {"usd": 3500.00, "usd_24h_vol": 1000000.00},
                        "0xbbb": {"usd": 1.00},
                    }
                )

        adapter = CoinGeckoPriceAdapter()
        mock_client = MockClient()
        monkeypatch.setattr(adapter, "_client", mock_client)

        prices = adapter.fetch_prices("ethereum", ["0xaaa", "0xbbb"])

        assert len(prices) == 2
        aaa = next(p for p in prices if p.token_address == "0xaaa")
        assert aaa.price_usd == Decimal("3500.00")
        assert aaa.volume_24h_usd == Decimal("1000000.00")

        bbb = next(p for p in prices if p.token_address == "0xbbb")
        assert bbb.price_usd == Decimal("1.00")
        assert bbb.volume_24h_usd is None

    def test_fetch_prices_empty_response(self, monkeypatch):
        class MockResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {}

        class MockClient:
            def get(self, url, params=None):
                return MockResponse()

        adapter = CoinGeckoPriceAdapter()
        monkeypatch.setattr(adapter, "_client", MockClient())

        prices = adapter.fetch_prices("ethereum", ["0xaaa"])
        assert prices == []

    def test_fetch_prices_unknown_chain(self):
        adapter = CoinGeckoPriceAdapter()
        prices = adapter.fetch_prices("unknown_chain", ["0xaaa"])
        assert prices == []

    def test_fetch_prices_empty_addresses(self):
        adapter = CoinGeckoPriceAdapter()
        prices = adapter.fetch_prices("ethereum", [])
        assert prices == []

    def test_token_price_dataclass(self):
        from datetime import datetime, timezone

        price = TokenPrice(
            token_address="0xaaa",
            chain="ethereum",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            price_usd=Decimal("3500.00"),
            source="coingecko",
            volume_24h_usd=Decimal("1000000"),
        )
        assert price.token_address == "0xaaa"
        assert price.price_usd == Decimal("3500.00")


from core.enrichment.enrich import PriceEnrichment


class MockClickHouseClientForEnrich:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries: list[tuple[str, dict]] = []
        self.inserts: list[tuple[str, list]] = []

    def query(self, sql: str, parameters: dict = None):
        self.queries.append((sql, parameters or {}))
        from core.tests.standalone.test_trajectory_engine import MockQueryResult
        return MockQueryResult(self.rows)

    def insert(self, table: str, data: list) -> None:
        self.inserts.append((table, data))


class TestPriceEnrichment:
    def test_compute_usd_eth(self):
        enrich = PriceEnrichment()
        result = enrich._compute_usd(
            raw_amount=1500000000000000000,
            token_address="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            decimals=18,
            price_usd=3500.00,
        )
        assert result == Decimal("5250.000000000000000000")

    def test_compute_usd_usdc(self):
        enrich = PriceEnrichment()
        result = enrich._compute_usd(
            raw_amount=1500000,
            token_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            decimals=6,
            price_usd=1.00,
        )
        assert result == Decimal("1.500000")

    def test_compute_usd_stablecoin_hardcode(self):
        enrich = PriceEnrichment()
        result = enrich._compute_usd(
            raw_amount=1500000,
            token_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            decimals=6,
            price_usd=0.99,
        )
        assert result == Decimal("1.500000")

    def test_compute_usd_no_price(self):
        enrich = PriceEnrichment()
        result = enrich._compute_usd(
            raw_amount=1500000000000000000,
            token_address="0xunknown",
            decimals=18,
            price_usd=None,
        )
        assert result is None

    def test_compute_usd_no_amount(self):
        enrich = PriceEnrichment()
        result = enrich._compute_usd(
            raw_amount=None,
            token_address="0xaaa",
            decimals=18,
            price_usd=1.00,
        )
        assert result is None

    def test_enrich_batch_no_rows(self):
        mock = MockClickHouseClientForEnrich(rows=[])
        enrich = PriceEnrichment(client=mock)
        from datetime import datetime, timezone
        count = enrich.enrich_batch(
            start=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end=datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        )
        assert count == 0
        assert len(mock.queries) == 1

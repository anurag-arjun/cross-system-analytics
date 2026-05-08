# Makefile for common tasks
.PHONY: up down test test-standalone test-fake test-ext schema lint import-spellbook

up:
	docker-compose up -d

down:
	docker-compose down

test:
	cd core && pytest tests/ -v

test-standalone:
	cd core && pytest tests/standalone/ -v

test-fake:
	cd core && pytest tests/fake_integrator/ -v

test-ext:
	cd core && pytest tests/extensibility/ -v

schema:
	cat core/schemas/*.sql | docker-compose exec -T clickhouse clickhouse-client

lint:
	cd core && ruff check .
	cd core && mypy .

arch:
	./ops/ci/weekly_arch_tests.sh

# Import contract -> protocol registry from a Dune Spellbook clone.
# Set SPELLBOOK_PATH to override the default sibling location.
# Set PROTOCOL_CONTRACTS_DSN to persist to Postgres; otherwise runs in-memory.
import-spellbook:
	PYTHONPATH=. python ops/import_spellbook_contracts.py --verbose

# Import contract -> protocol registry + contract labels from Dune.
# Requires DUNE_API_KEY env var. Runs pre-flight count for each phase,
# aborts if cumulative bytes exceed --byte-cap-mb (default 50 MB).
# Set PROTOCOL_CONTRACTS_DSN to persist; otherwise runs in-memory.
import-dune:
	PYTHONPATH=. python ops/import_dune_contracts.py --verbose

# Fetch ABIs for every contract in protocol_contracts via Etherscan V2
# (with bytecode-hash dedup + EIP-1967 proxy resolution). Falls back to
# Sourcify when Etherscan reports the contract is not verified.
# Requires ETHERSCAN_API_KEY for full coverage; without it, Sourcify-only.
fetch-abis:
	PYTHONPATH=. python ops/fetch_abis.py --verbose

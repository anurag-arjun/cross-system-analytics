# Makefile for common tasks
.PHONY: up down test test-standalone test-fake test-ext schema lint

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

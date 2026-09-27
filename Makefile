.PHONY: setup up down init seed run inspect test lint reset airflow

setup:
	cp .env.example .env

up:
	docker compose up -d source-db

down:
	docker compose down

init:
	docker compose run --rm pipeline python -m railops.cli init

seed:
	docker compose run --rm pipeline python -m railops.cli seed --rows 100

run:
	docker compose run --rm pipeline python -m railops.cli run

inspect:
	docker compose run --rm pipeline python -m railops.cli inspect

test:
	docker compose run --rm pipeline pytest -q

lint:
	docker compose run --rm pipeline ruff check .

airflow:
	docker compose --profile orchestration up -d airflow

reset:
	docker compose down -v
	rm -f data/warehouse.duckdb data/warehouse.duckdb.wal


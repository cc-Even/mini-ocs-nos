.DEFAULT_GOAL := build

.PHONY: bootstrap configure build cpp-test python-sync python-test lint test up redis-test redis-integration-test down

bootstrap:
	./scripts/bootstrap.sh

configure:
	cmake --preset dev

build: configure python-sync
	cmake --build --preset dev

cpp-test: build
	ctest --preset dev

python-sync:
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv sync --frozen --python 3.12

python-test: python-sync
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv run --frozen pytest

lint: python-sync
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv run --frozen ruff check python

test: cpp-test python-test lint

up:
	docker compose up -d --wait redis

redis-test:
	test "$$(docker compose exec -T redis redis-cli ping)" = "PONG"

redis-integration-test: build
	./scripts/test-redis-contracts.sh

down:
	docker compose down

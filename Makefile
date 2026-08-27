.DEFAULT_GOAL := build

.PHONY: bootstrap configure build cpp-test sanitizer-test sanitizer-integration-test generate-protos python-sync python-test lint test image up redis-test redis-integration-test e2e down

bootstrap:
	./scripts/bootstrap.sh

configure:
	cmake --preset dev

build: configure python-sync
	cmake --build --preset dev

cpp-test: build
	ctest --preset dev

sanitizer-test:
	cmake --preset sanitizer
	cmake --build --preset sanitizer
	ctest --preset sanitizer

sanitizer-integration-test:
	cmake --preset sanitizer
	cmake --build --preset sanitizer
	OCS_CTEST_DIR=build/sanitizer \
	OCS_RUN_PYTHON_INTEGRATION=0 \
	OCS_TEST_LOG_DIR=artifacts/test-logs/sanitizer-integration \
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
	UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	./scripts/test-redis-contracts.sh

generate-protos: python-sync
	./scripts/generate-protos.sh

python-sync:
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv sync --frozen --python 3.12

python-test: python-sync
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv run --frozen pytest

lint: python-sync
	UV_CACHE_DIR=$${UV_CACHE_DIR:-/tmp/mini-ocs-uv-cache} uv run --frozen ruff check python

test: cpp-test python-test lint

image:
	docker compose build

up:
	docker compose up -d --build --wait

redis-test:
	test "$$(docker compose exec -T redis redis-cli ping)" = "PONG"

redis-integration-test: build
	./scripts/test-redis-contracts.sh

e2e: python-sync
	./scripts/test-compose-e2e.sh

down:
	docker compose down

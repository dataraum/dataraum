.PHONY: help up down logs dev-cockpit test-engine build install

help:
	@echo "DataRaum monorepo — common workspace tasks"
	@echo ""
	@echo "  make up           Bring up the full stack (postgres + engine + cockpit)"
	@echo "  make down         Stop the stack"
	@echo "  make logs         Tail compose logs"
	@echo "  make dev-cockpit  Run cockpit dev server with hot reload (outside docker)"
	@echo "  make test-engine  Run engine unit tests (testmon-cached)"
	@echo "  make build        Build all docker images"
	@echo "  make install      Install engine + cockpit deps"

up:
	docker compose -f packages/infra/docker-compose.yml --env-file packages/infra/.env up -d --wait

down:
	docker compose -f packages/infra/docker-compose.yml down

logs:
	docker compose -f packages/infra/docker-compose.yml logs -f

build:
	docker compose -f packages/infra/docker-compose.yml build

dev-cockpit:
	cd packages/cockpit && bun run dev

test-engine:
	cd packages/engine && uv run pytest --testmon tests/unit -q

install:
	cd packages/engine && uv sync --group dev
	cd packages/cockpit && bun install

# CartPulse shortcuts - `make help` lists them.
COMPOSE = docker compose

.PHONY: help up ps logs down clean batch open topics peek sql-shop sql-wh redis test

help:          ## List commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

up:            ## Build and start everything
	$(COMPOSE) up -d --build

ps:            ## Service status
	$(COMPOSE) ps

logs:          ## Follow logs, e.g. make logs S=stream
	$(COMPOSE) logs -f --tail=100 $(S)

down:          ## Stop (data is kept)
	$(COMPOSE) down

clean:         ## Stop and delete ALL data (volumes + ./data)
	$(COMPOSE) down -v --remove-orphans
	rm -rf data

batch:         ## Run the batch DAG now instead of waiting 30 minutes
	$(COMPOSE) exec airflow airflow dags trigger cartpulse_batch

topics:        ## List Kafka topics
	$(COMPOSE) exec redpanda rpk topic list

peek:          ## Show 3 events, e.g. make peek T=shop.public.orders
	$(COMPOSE) exec redpanda rpk topic consume $(or $(T),shop.clickstream) -n 3 -f '%v\n'

sql-shop:      ## psql into the shop (OLTP) database
	$(COMPOSE) exec postgres psql -U shop -d shop

sql-wh:        ## psql into the warehouse
	$(COMPOSE) exec postgres psql -U loader -d warehouse

redis:         ## redis-cli into the live store
	$(COMPOSE) exec redis redis-cli

test:          ## Run the test suite (needs Java 17 + requirements-dev.txt)
	cd app && python -m pytest -q

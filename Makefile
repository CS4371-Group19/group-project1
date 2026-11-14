.PHONY: up logs down status topic1 topic2

ARTICLE_TOPIC ?= topic1
ENTITY_COUNT_TOPIC ?= topic2
COMPOSE := docker compose -f compose.yaml -f compose.override.yaml -f compose.apps.yaml

up:
	$(COMPOSE) up --build -d

logs:
	$(COMPOSE) logs -f producer spark

down:
	$(COMPOSE) down

status:
	$(COMPOSE) ps

topic1:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $(ARTICLE_TOPIC) --from-beginning

topic2:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $(ENTITY_COUNT_TOPIC) --from-beginning --property print.key=true

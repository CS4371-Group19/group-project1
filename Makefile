.PHONY: up logs down status topic1 topic2

ARTICLE_TOPIC ?= topic1
ENTITY_COUNT_TOPIC ?= topic2

up:
	docker compose up --build -d

logs:
	docker compose logs -f producer spark

down:
	docker compose down

status:
	docker compose ps

topic1:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $(ARTICLE_TOPIC) --from-beginning

topic2:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $(ENTITY_COUNT_TOPIC) --from-beginning --property print.key=true

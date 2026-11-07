# CS4371 Group Project 1 — article stream and entity counts

The producer polls NewsAPI's live US top headlines, normalizes each article, and publishes unseen IDs to Kafka `topic1`. Spark Structured Streaming reads `topic1`, extracts named entities with spaCy, maintains running mention counts, and publishes updated totals to `topic2`.

## Run with Docker Compose

Prerequisite: Docker with Compose and a NewsAPI developer key.

1. Copy `.env.example` to `.env` and replace the placeholder `NEWS_API_KEY`. Do not commit `.env` or paste a key in source code.
2. Run `make up` from this directory. The first build downloads Spark and spaCy's English model; the first Spark start resolves its Kafka connector, so it takes longer than later starts.
3. Run `make status`, then `make logs`. Both `producer` and `spark` should stay running. Compose waits for Kafka to become healthy and creates `topic1` and `topic2` automatically.
4. Run `make topic1` or `make topic2` to inspect records. Press Ctrl+C to stop following a topic. Run `make down` to stop the stack while retaining Kafka data, seen IDs, and Spark checkpoints.

If your Kafka volume already contains Guardian test messages, choose fresh topic names in `.env` (for example `ARTICLE_TOPIC=newsapi_topic1` and `ENTITY_COUNT_TOPIC=newsapi_topic2`) before `make up`. This keeps old articles and entity counts out of the new run. To inspect those custom topics, use `make topic1 ARTICLE_TOPIC=newsapi_topic1` and `make topic2 ENTITY_COUNT_TOPIC=newsapi_topic2`. The producer's seen-ID file and Spark checkpoint paths follow the article topic name.

Inside Compose, services connect to `kafka:19092`. From the host, use `localhost:9092`. The Docker image uses Spark 4.0.1, matching the `spark-sql-kafka-0-10_2.13:4.0.1` connector. Both Python scripts run in separate containers from the same image.

### Local development without the app containers

Start Kafka and topics with `docker compose up -d kafka topics`. Set `NEWS_API_KEY`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, and `ARTICLE_TOPIC=topic1` in your environment. Set `SEEN_IDS_FILE` to a writable local file, then run `python scripts/news_producer.py` from a Python environment with `requirements.txt` installed.

For Spark, also set `ENTITY_COUNT_TOPIC=topic2` and `CHECKPOINT_DIR` to a local directory, install the English model using `python -m spacy download en_core_web_sm`, and run:

```bash
spark-submit --master 'local[*]' --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 scripts/entity_stream.py
```

Do not run the Docker app containers and the same host scripts at the same time unless you intend to run duplicate producers or streaming queries.

## Message contracts

The `topic1` Kafka key is `newsapi:` followed by the article URL. The JSON value contains `id`, `source` (publisher name), `text` (title, description, and available content), `published_at`, `ingested_at`, and `url`. NewsAPI limits `content` to 200 characters; it may overlap with the description. The producer saves acknowledged IDs so restarts do not republish the latest headlines.

The `topic2` Kafka key is the entity text. Its JSON value is `{"entity":"OpenAI","count":12}`. This is the **latest running total of mentions**, not an increment. A downstream Elasticsearch index should replace the previous count for the same entity rather than sum the successive totals. Spark checkpoints its offsets and counts in the `spark-checkpoints` volume.

The producer polls every 15 minutes: a full day of one-page polling uses 96 requests against the NewsAPI developer plan's 100-request daily limit. Extra testing or fetching more pages uses additional requests. `/v2/top-headlines` is a curated feed, not a complete archive, and a single response contains at most 100 articles. For the assignment's 1,000-message batch requirement, measure your distinct `topic1` article count; you may need a separate backfill strategy. `make down` stops polling when you are finished.

## Notes for the group

- Each teammate runs the stack locally and uses their own NewsAPI key. The Compose network name `kafka` is available only inside that teammate's Docker stack.
- The named Docker volumes retain data and checkpoints across `make down`. Do not use `docker compose down -v` casually; it deletes those volumes. Use a fresh checkpoint when intentionally replaying a new input topic.
- `topic2` currently includes every spaCy entity label, including numeric and date expressions. Decide as a group which labels to display in the top-ten chart.
- Part 2's AG News classifier, Elasticsearch, Logstash, and Kibana still need to be integrated. Document AI assistance and submit the required transcripts as specified in the assignment.

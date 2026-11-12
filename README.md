# CS4371 Group Project 1: Article Stream, Entity Counts, and Text Classification

The producer polls NewsAPI's live US top headlines, normalizes each article, and publishes unseen IDs to Kafka `topic1`. Spark Structured Streaming reads `topic1`, extracts named entities with spaCy, maintains running mention counts, and publishes updated totals to `topic2`.

The project also uses a Spark MLlib text-classification pipeline trained on the AG News dataset. The saved model classifies streamed news articles into World, Sports, Business, and Sci/Tech categories.

## Run with Docker Compose

Prerequisite: Docker with Compose and a NewsAPI developer key.

1. Copy `.env.example` to `.env` and replace the placeholder `NEWS_API_KEY`. Do not commit `.env` or paste a key in source code.
2. Run `make up` from this directory. The first build downloads Spark and spaCy's English model; the first Spark start resolves its Kafka connector, so it takes longer than later starts.
3. Run `make status`, then `make logs`. Both `producer` and `spark` should stay running. Compose waits for Kafka to become healthy and creates the required Kafka topics.
4. Run `make topic1` or `make topic2` to inspect records. Press Ctrl+C to stop following a topic. Run `make down` to stop the stack while retaining Kafka data, seen IDs, and Spark checkpoints.

The Spark application uses these Kafka topics:

- `topic1`: Incoming news articles.
- `topic2`: Named entity counts.
- `topic3`: Predicted news-category counts.

Inside Compose, services connect to `kafka:19092`. From the host, use `localhost:9092`. The Docker image uses Spark 4.0.1, matching the `spark-sql-kafka-0-10_2.13:4.0.1` connector.

If your Kafka volume already contains older test messages, choose fresh topic names before running the stack. This keeps old articles and entity counts out of a new run.

### Local development without the app containers

Start Kafka and topics with:

```bash
docker compose up -d kafka topics
```

Set `NEWS_API_KEY`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, and `ARTICLE_TOPIC=topic1` in your environment. Set `SEEN_IDS_FILE` to a writable local file, then run `python scripts/news_producer.py` from a Python environment with `requirements.txt` installed.

For Spark, also set `ENTITY_COUNT_TOPIC=topic2`, `CLASSIFICATION_COUNT_TOPIC=topic3`, and `CHECKPOINT_DIR` to a local directory. Install the English model using:

```bash
python -m spacy download en_core_web_sm
```

Run the streaming application with:

```bash
spark-submit --master 'local[*]' --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 scripts/entity_stream.py
```

Do not run the Docker app containers and the same host scripts at the same time unless you intend to run duplicate producers or streaming queries.

## Part 1: Named Entity Extraction and Streaming

The `topic1` Kafka key is `newsapi:` followed by the article URL. The JSON value contains `id`, `source`, `text`, `published_at`, `ingested_at`, and `url`.

The `topic2` Kafka key is the entity text. Its JSON value is:

```json
{"entity":"OpenAI","count":12}
```

This is the latest running total of mentions, not an increment. A downstream Elasticsearch index should replace the previous count for the same entity rather than sum successive totals.

Spark checkpoints its offsets and counts in the `spark-checkpoints` volume.

The producer polls every 15 minutes. A full day of one-page polling uses 96 requests against the NewsAPI developer plan's 100-request daily limit. `/v2/top-headlines` is a curated feed, not a complete archive, and a single response contains at most 100 articles.

## Part 2: Text Classification with Spark MLlib

### 2.1 AG News Dataset

The project uses the AG News training dataset from Hugging Face:

https://huggingface.co/datasets/fancyzhx/ag_news

The training split contains 120,000 labeled articles with `text` and `label` columns.

The dataset can be downloaded using:

```bash
python download_ag_news.py
```

The resulting training file is stored at:

```text
data/ag_news/train.parquet
```

The four class labels are:

| Label | Category |
|---:|---|
| 0 | World |
| 1 | Sports |
| 2 | Business |
| 3 | Sci/Tech |

### 2.2 Model Training

The classification pipeline uses these Spark MLlib stages:

1. RegexTokenizer: converts text into lowercase tokens and removes punctuation.
2. StopWordsRemover: removes common words.
3. HashingTF: converts tokens into term-frequency features.
4. IDF: applies inverse document frequency weighting.
5. LogisticRegression: predicts one of the four AG News categories.

The fitted PipelineModel is saved in:

```text
models/ag_news_lr_model
```

The same saved model is reused for batch classification and real-time streaming. It does not need to be retrained for each classification run.

### 2.3 Batch Classification

The batch classifier loads the saved PipelineModel and applies it to the collected streamed news messages.

Example Docker command:

```powershell
docker compose -f compose.yaml -f compose.apps.yaml run --rm --no-deps -v "${PWD}/output:/app/output" --entrypoint /opt/spark/bin/spark-submit spark --master "local[*]" /app/scripts/classify_batch.py --input /app/streamed_news.json --model /app/models/ag_news_lr_model --output /app/output/classified_news
```

The output contains the predicted category and predicted probability for every processed message.

The program also reports the number of messages assigned to each category and displays five example messages per category.

### 2.3(b) Manual Evaluation

A random sample of 50 streamed messages is selected for manual labeling.

The labels are:

- World
- Sports
- Business
- Sci/Tech
- None

The evaluation compares the manual labels with the model predictions. Accuracy is calculated only for messages belonging to one of the four AG News categories. Messages labeled None are reported separately.

To generate the sample and evaluate the labels, use:

```powershell
docker compose -f compose.yaml -f compose.apps.yaml run --rm --no-deps -v "${PWD}/output:/app/output" --entrypoint /opt/spark/bin/spark-submit spark --master "local[*]" /app/scripts/sample_for_manual_labels.py
```

After completing the labels in `output/manual_label_sample/manual_labels.csv`, run:

```powershell
docker compose -f compose.yaml -f compose.apps.yaml run --rm --no-deps -v "${PWD}/output:/app/output" --entrypoint /opt/spark/bin/spark-submit spark --master "local[*]" /app/scripts/evaluate_manual_labels.py
```

The evaluation produces the overall accuracy, class-level results, and a confusion matrix.

### 2.3(c) Real-Time Classification

The existing Structured Streaming application has a second query that applies the saved AG News PipelineModel to incoming messages from `topic1`.

It maintains running counts for each predicted category and publishes updates to Kafka `topic3`.

The JSON output has the following structure:

```json
{"predicted_class":"Sci/Tech","count":10}
```

The count represents the latest running total for that predicted category.

The Spark service uses these environment variables:

```text
ARTICLE_TOPIC=topic1
ENTITY_COUNT_TOPIC=topic2
CLASSIFICATION_COUNT_TOPIC=topic3
```

To inspect the classification output:

```bash
docker compose -f compose.yaml -f compose.apps.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic3 --from-beginning
```

The consumer displays classification-count updates published by Spark.

## Part 2 Results

The batch classification processed 1,077 unique nonempty streamed messages.

| Predicted category | Messages |
|---|---:|
| World | 171 |
| Sports | 201 |
| Business | 328 |
| Sci/Tech | 377 |
| **Total** | **1,077** |

The manual evaluation used 50 sampled messages:

- Messages labeled None: 11
- Messages belonging to the four AG News categories: 39
- Correct predictions: 23
- Accuracy among the four categories: 58.97%

These results provide an initial assessment of how well the AG News model transfers to the collected NewsAPI articles.

## Named Entity Visualization: Part 1, Step 5

This component reads named entity counts from Kafka topic2 using Logstash, stores them in Elasticsearch, and displays the top 10 entities in Kibana.

### Requirements

- Docker Desktop with Docker Compose
- The project's Kafka service
- Entity count messages in topic2, produced by the Spark application

### Start the visualization services

```bash
docker compose -f compose.yaml -f compose.override.yaml -f compose.apps.yaml up -d elasticsearch logstash kibana
```

Open http://localhost:5601 after Kibana finishes starting.

### Import the dashboard

1. Open Stack Management > Saved Objects.
2. Click Import.
3. Select `kibana/newsapi-dashboard.ndjson`.
4. Open Dashboards > NewsAPI Entity Counts Dashboard.

The export contains dashboard settings, not the underlying news data. Data must be available in the entity-counts Elasticsearch index.

### How counts are displayed

Logstash uses each entity name as the Elasticsearch document ID. New counts replace the previous count for that entity.

The chart uses:

- Horizontal axis: Top 10 values of entity.keyword
- Vertical axis: Maximum of count
- Ranking: Maximum of count, descending
- Other group: disabled

### Results

Graph screenshots are stored in the screenshots folder. Each screenshot records the actual elapsed time of the run.

### Local Testing Limitation

On the test computer, Docker had approximately 3.65 GiB of memory. Spark and Kibana were run alternately to reduce memory usage. Continuous operation of all services together has not yet been verified on this computer.

## Notes for the Group

- Each teammate runs the stack locally and uses their own NewsAPI key.
- The Compose network name `kafka` is available only inside that teammate's Docker stack.
- The named Docker volumes retain data and checkpoints across `make down`. Do not use `docker compose down -v` casually; it deletes those volumes.
- Use a fresh checkpoint when intentionally replaying a new input topic.
- Document AI assistance and submit the required transcripts as specified in the assignment.

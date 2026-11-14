import requests
import json
import re
import html
from datetime import datetime, timezone
import time
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

def fetch_latest_news() -> list[dict]:
    if os.environ["NEWS_API_KEY"] == "":
        raise Exception("Missing NEWS_API_KEY")

    response = requests.get(
        "https://newsapi.org/v2/top-headlines",
        params={"country": "us", "pageSize": 100},
        headers={"X-Api-Key": os.environ["NEWS_API_KEY"]},
        timeout=15,
    )
    response.raise_for_status()
    return [
        normalize_article(item)
        for item in response.json()["articles"]
        if item.get("url") and item.get("title")
    ]


def clean_text(value: str) -> str:
    without_tags = re.sub(r"<[^>]*>", " ", html.unescape(value))
    return " ".join(without_tags.split())


def normalize_article(item: dict) -> dict:
    title = clean_text(item["title"])
    summary = clean_text(item.get("description") or "")
    content = clean_text(item.get("content") or "")
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    return {
        "id": "newsapi:" + item["url"],
        "source": (item.get("source") or {}).get("name") or "unknown",
        "text": "\n".join(part for part in (title, summary, content) if part),
        "published_at": item["publishedAt"],
        "ingested_at": ingested_at,
        "url": item["url"],
    }


if __name__ == "__main__":
    if os.environ["KAFKA_BOOTSTRAP_SERVERS"] == "":
        raise Exception("Missing KAFKA_BOOTSTRAP_SERVERS")
    if os.environ["ARTICLE_TOPIC"] == "":
        raise Exception("Missing ARTICLE_TOPIC")

    kafka_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
    article_topic = os.environ["ARTICLE_TOPIC"]

    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        with open("seen_article_ids.txt", "a+") as file:
            file.seek(0)
            seen_article_ids = {line.strip() for line in file}

            while True:
                count = 0
                article_list = fetch_latest_news()
                for article in article_list:
                    if article["id"] in seen_article_ids:
                        continue

                    producer.send(article_topic, key=article["id"], value=article).get(timeout=15)
                    seen_article_ids.add(article["id"])
                    file.write(article["id"] + "\n")
                    file.flush()
                    count += 1

                print(f"Published {count} articles successfully")
                print("Going to sleep 15 minutes...")
                time.sleep(900)
    finally:
        producer.flush()
        producer.close()

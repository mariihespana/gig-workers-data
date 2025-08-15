import argparse
import logging
import random
import time

import feedparser
from newspaper import Article
import pandas as pd


def fetch_articles(rss_url: str, max_articles: int):
    feed = feedparser.parse(rss_url)
    articles = []
    for entry in feed.entries[:max_articles]:
        url = entry.link
        title = entry.title
        published = entry.get("published", "")
        logging.info("Fetching %s", url)
        try:
            art = Article(url)
            art.download()
            art.parse()
            articles.append({
                "title": title,
                "published": published,
                "url": url,
                "text": art.text,
            })
        except Exception as exc:
            logging.error("Failed to fetch %s: %s", url, exc)
        time.sleep(random.uniform(1, 2))
    return articles


def main():
    parser = argparse.ArgumentParser(description="Download Google News RSS articles to CSV")
    parser.add_argument("rss_url", help="Google News RSS feed URL")
    parser.add_argument("--max", dest="max_articles", type=int, default=5,
                        help="Maximum number of articles to fetch")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    rows = fetch_articles(args.rss_url, args.max_articles)
    pd.DataFrame(rows).to_csv("google_news_articles.csv", index=False)
    logging.info("Saved %d articles", len(rows))


if __name__ == "__main__":
    main()

"""Download news from an RSS feed and store them in BigQuery.

This script parses an RSS feed using ``feedparser`` and writes the
resulting entries to a BigQuery table.  Each row in the table contains the
title, description, link, published date and summary of a news item.
"""

from __future__ import annotations

import feedparser
from dateutil import parser
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/mariana/.config/gcloud/application_default_credentials.json"



def extract_news(feed_url: str) -> list[dict[str, str]]:
    """Fetch news entries from ``feed_url``.

    Parameters
    ----------
    feed_url:
        The URL of the RSS feed to parse.

    Returns
    -------
    list of dict
        A list where each element contains the keys ``title``,
        ``description``, ``link``, ``published`` and ``summary``.
    """

    feed = feedparser.parse(feed_url)
    news_items: list[dict[str, str]] = []

    for entry in feed.entries:
        news_items.append(
            {
                "title": entry.title,
                "description": getattr(entry, "description", ""),
                "link": entry.link,
                "published": parser.parse(getattr(entry, "published", "")).isoformat(),
                "summary": getattr(entry, "summary", ""),
            }
        )

    return news_items


def main() -> None:
    # Replace with the actual URL of the RSS feed you want to parse
    feed_url = (
        "https://news.google.com/rss/search?q=entregadores&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    )

    # Replace with your project, dataset and table path
    table_id = "mlops-project-430120.STAGING_DATA.gig_workers_news_brazil"

    client = bigquery.Client()
    news_items = extract_news(feed_url)

    rows_added = 0
    for item in news_items:
        errors = client.insert_rows_json(table_id, [item])
        if errors:
            print(f"Failed to add row for '{item['title']}': {errors}")
        else:
            rows_added += 1
            print(f"Added row {rows_added}: {item['title']}")

    print(f"Total rows added: {rows_added}")


if __name__ == "__main__":
    main()


# gig-workers-data

Scripts to gather news articles about gig workers.

## google_news_scraper.py

Download and store full article text from a Google News RSS feed.

```
python google_news_scraper.py "<rss_url>" --max 5
```

Results are saved to `google_news_articles.csv`.

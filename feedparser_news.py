import feedparser

# Replace with the actual URL of the RSS feed you want to parse
feed_url = "https://news.google.com/rss/search?q=entregadores&hl=pt-BR&gl=BR&ceid=BR:pt-419" 

# Parse the RSS feed
feed = feedparser.parse(feed_url)

# Accessing feed metadata (e.g., title, description, link)
print("Feed Title:", feed.feed.title)
print("Feed Description:", feed.feed.description)
print("Feed Link:", feed.feed.link)

# Iterating through feed entries (articles or items)
print("\nEntries:")
for entry in feed.entries:
    print("  Title:", entry.title)
    print("  Link:", entry.link)
    print("  Published Date:", entry.published)
    print("  Summary:", entry.summary)
    print("-" * 20)
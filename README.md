This repository contains a collection of utilities for market research and trading automation.

## Existing utilities
- Scripts for tracking M5 bounces and D1 AVWAP bounces.
- Logging utilities for existing trading bots.

## New data collection workflow
The `scrapers/` directory now includes modular scrapers for different data sources:
- `econscraper.py` – Trading Economics calendar data with configurable look-ahead windows.
- `earningsscraper.py` – Nasdaq earnings calendar with support for upcoming days.
- `twitterscraper.py` – Base implementation using `snscrape` to follow specific Twitter handles.
- `redditscraper.py` – Helpers built on `praw` for monitoring subreddits and users.

Use `generate_report.py` to consolidate the scraped information into a Markdown file suitable for a local LLM or external summarisation. Run `python generate_report.py --help` for usage details. Examples:

```bash
# Daily report with a Twitter and Reddit watchlist
python generate_report.py \
    --output reports/daily.md \
    --twitter-handles MarketWatch wsjmarkets \
    --subreddits wallstreetbets stocks \
    --reddit-users Asktraders

# Look a full week ahead for economic and earnings events only
python generate_report.py --output reports/week_ahead.md --days-ahead 7
```

Each scraper can also be executed directly for quick experiments:

```bash
# Fetch tweets for the provided handles (requires `pip install snscrape`)
python scrapers/twitterscraper.py --handles MarketWatch wsjmarkets

# Fetch Reddit content (requires `pip install praw` and Reddit API credentials)
python scrapers/redditscraper.py \
    --subreddits wallstreetbets stocks \
    --users Asktraders \
    --reddit-client-id YOUR_ID \
    --reddit-client-secret YOUR_SECRET
```

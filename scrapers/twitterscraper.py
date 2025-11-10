"""Lightweight helpers for collecting tweets for specified handles.

The module can be imported from other scripts or executed directly.
When run as a script it accepts a list of handles (without the ``@``)
and prints the fetched tweets in Markdown format. This mirrors the
behaviour used by :mod:`generate_report` and makes it easy to test a
watchlist from the command line:

.. code-block:: bash

   python scrapers/twitterscraper.py --handles MarketWatch wsjmarkets

The optional :mod:`snscrape` dependency must be installed separately:

.. code-block:: bash

   pip install snscrape
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

try:
    import snscrape.modules.twitter as sntwitter
except ImportError:  # pragma: no cover - optional dependency
    sntwitter = None  # type: ignore


@dataclass
class Tweet:
    """Representation of a tweet returned by :mod:`snscrape`."""

    username: str
    content: str
    link: str
    date: datetime


def fetch_tweets(
    handles: Iterable[str],
    limit: int = 20,
    since: Optional[datetime] = None,
) -> List[Tweet]:
    """Fetch the latest tweets for the provided handles.

    Parameters
    ----------
    handles:
        Iterable of Twitter handles (without the ``@`` symbol).
    limit:
        Maximum number of tweets to retrieve per handle.
    since:
        Optional datetime to use as a cut-off. Only tweets newer than this value
        are returned.
    """

    if sntwitter is None:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "snscrape is required for Twitter scraping. Install it with"
            " 'pip install snscrape'."
        )

    tweets: List[Tweet] = []
    for handle in handles:
        query = f"from:{handle}"
        if since is not None:
            query += f" since:{since.strftime('%Y-%m-%d')}"

        scraper = sntwitter.TwitterSearchScraper(query)
        for i, tweet in enumerate(scraper.get_items()):
            if i >= limit:
                break
            tweets.append(
                Tweet(
                    username=handle,
                    content=getattr(tweet, "content", ""),
                    link=f"https://twitter.com/{handle}/status/{tweet.id}",
                    date=getattr(tweet, "date", datetime.utcnow()),
                )
            )

    return tweets


def format_tweets_for_markdown(tweets: Iterable[Tweet]) -> str:
    """Convert tweets to Markdown bullet points."""

    lines: List[str] = []
    for tweet in tweets:
        timestamp = tweet.date.strftime("%Y-%m-%d %H:%M")
        lines.append(
            f"- **@{tweet.username}** ({timestamp} UTC): {tweet.content}\n  "
            f"[{tweet.link}]({tweet.link})"
        )
    return "\n".join(lines)


__all__ = [
    "Tweet",
    "fetch_tweets",
    "format_tweets_for_markdown",
]


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch tweets for handles")
    parser.add_argument(
        "--handles",
        nargs="+",
        help="Twitter handles to scrape (omit the @ prefix).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum tweets to retrieve per handle (default: 10).",
    )
    return parser.parse_args(argv)


def _main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    if not args.handles:
        raise SystemExit("No handles provided. Use --handles to specify accounts.")

    tweets = fetch_tweets(handles=args.handles, limit=args.limit)
    print(format_tweets_for_markdown(tweets))


if __name__ == "__main__":  # pragma: no cover - convenience CLI
    _main()

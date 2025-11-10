"""Generate a consolidated Markdown report from multiple data sources."""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from scrapers.econscraper import fetch_economic_calendar, format_events_for_markdown
from scrapers.earningsscraper import fetch_earnings_calendar, format_events_for_markdown as format_earnings

try:  # Optional dependencies
    from scrapers.twitterscraper import fetch_tweets, format_tweets_for_markdown
except RuntimeError:  # pragma: no cover - optional dependency
    fetch_tweets = None  # type: ignore
    format_tweets_for_markdown = None  # type: ignore

try:  # Optional dependencies
    from scrapers.redditscraper import (
        create_reddit_client,
        fetch_subreddit_posts,
        fetch_user_posts,
        format_posts_for_markdown,
    )
except RuntimeError:  # pragma: no cover - optional dependency
    create_reddit_client = None  # type: ignore
    fetch_subreddit_posts = None  # type: ignore
    fetch_user_posts = None  # type: ignore
    format_posts_for_markdown = None  # type: ignore


def _section(title: str, body: str) -> str:
    return f"## {title}\n\n{body}\n\n" if body else f"## {title}\n\n_No data available._\n\n"


def generate_report(
    output: Path,
    days_ahead: int = 0,
    twitter_handles: Optional[Iterable[str]] = None,
    subreddit_names: Optional[Iterable[str]] = None,
    reddit_users: Optional[Iterable[str]] = None,
    reddit_client_id: Optional[str] = None,
    reddit_client_secret: Optional[str] = None,
    reddit_user_agent: Optional[str] = None,
) -> None:
    """Create the consolidated report and write it to ``output``."""

    output.parent.mkdir(parents=True, exist_ok=True)

    economic_events = fetch_economic_calendar(days_ahead=days_ahead)
    earnings_events = fetch_earnings_calendar(days_ahead=days_ahead)

    sections: List[str] = []

    sections.append(_section("Economic Calendar", format_events_for_markdown(economic_events)))
    sections.append(_section("Earnings Calendar", format_earnings(earnings_events)))

    if twitter_handles and fetch_tweets and format_tweets_for_markdown:
        try:
            tweets = fetch_tweets(twitter_handles)
            sections.append(_section("Twitter Watchlist", format_tweets_for_markdown(tweets)))
        except Exception as exc:  # pragma: no cover - network dependent
            sections.append(_section("Twitter Watchlist", f"Error fetching tweets: {exc}"))

    if (subreddit_names or reddit_users) and create_reddit_client:
        try:
            reddit = create_reddit_client(
                client_id=reddit_client_id,
                client_secret=reddit_client_secret,
                user_agent=reddit_user_agent,
            )
            reddit_posts: List[str] = []
            if subreddit_names and fetch_subreddit_posts:
                subreddit_posts = fetch_subreddit_posts(reddit, subreddit_names)
                reddit_posts.append(format_posts_for_markdown(subreddit_posts))
            if reddit_users and fetch_user_posts:
                user_posts = fetch_user_posts(reddit, reddit_users)
                reddit_posts.append(format_posts_for_markdown(user_posts))

            sections.append(_section("Reddit Highlights", "\n".join(filter(None, reddit_posts))))
        except Exception as exc:  # pragma: no cover - network dependent
            sections.append(_section("Reddit Highlights", f"Error fetching Reddit data: {exc}"))

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    header = f"# Market Update\n\n_Generated on {timestamp}_\n\n"

    with output.open("w", encoding="utf-8") as fh:
        fh.write(header)
        for section in sections:
            fh.write(section)


def _parse_args(args: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the market report.")
    parser.add_argument("--output", type=Path, default=Path("market_report.md"))
    parser.add_argument("--days-ahead", type=int, default=0, help="How many days ahead to include.")
    parser.add_argument(
        "--twitter-handles",
        nargs="*",
        default=None,
        help="Twitter handles (without @) to include in the report.",
    )
    parser.add_argument(
        "--subreddits",
        nargs="*",
        default=None,
        help="Subreddits to monitor for new posts.",
    )
    parser.add_argument(
        "--reddit-users",
        nargs="*",
        default=None,
        help="Reddit users to monitor for submissions.",
    )
    parser.add_argument("--reddit-client-id")
    parser.add_argument("--reddit-client-secret")
    parser.add_argument("--reddit-user-agent")
    return parser.parse_args(args)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    generate_report(
        output=args.output,
        days_ahead=args.days_ahead,
        twitter_handles=args.twitter_handles,
        subreddit_names=args.subreddits,
        reddit_users=args.reddit_users,
        reddit_client_id=args.reddit_client_id,
        reddit_client_secret=args.reddit_client_secret,
        reddit_user_agent=args.reddit_user_agent,
    )


if __name__ == "__main__":
    main()

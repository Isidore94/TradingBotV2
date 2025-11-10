"""Helpers for fetching Reddit content for specific subreddits and users.

The module supports both programmatic imports and a small command line
interface that mirrors :mod:`generate_report`. When run directly you can
specify a list of subreddits and/or user names to monitor:

.. code-block:: bash

   python scrapers/redditscraper.py \
       --subreddits wallstreetbets stocks \
       --users Asktraders

To authenticate with Reddit provide credentials via command line flags or
``REDDIT_CLIENT_ID``/``REDDIT_CLIENT_SECRET`` environment variables. The
``praw`` dependency must also be installed separately:

.. code-block:: bash

   pip install praw
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

try:
    import praw
except ImportError:  # pragma: no cover - optional dependency
    praw = None  # type: ignore


@dataclass
class RedditPost:
    """Lightweight container for a Reddit submission."""

    source: str
    title: str
    url: str
    created_utc: float
    author: Optional[str]
    score: int

    def created_datetime(self) -> datetime:
        return datetime.utcfromtimestamp(self.created_utc)


def create_reddit_client(
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    user_agent: Optional[str] = None,
):
    """Create a :class:`praw.Reddit` client using provided or env credentials."""

    if praw is None:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "praw is required for Reddit scraping. Install it with"
            " 'pip install praw'."
        )

    client_id = client_id or os.getenv("REDDIT_CLIENT_ID")
    client_secret = client_secret or os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = user_agent or os.getenv("REDDIT_USER_AGENT") or "market-bot"

    if not client_id or not client_secret:
        raise ValueError(
            "Reddit credentials are required. Provide client_id/client_secret or"
            " set the REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET environment variables."
        )

    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )


def fetch_subreddit_posts(
    reddit,
    subreddits: Iterable[str],
    limit: int = 10,
) -> List[RedditPost]:
    """Fetch the latest submissions for the provided subreddits."""

    posts: List[RedditPost] = []
    for subreddit in subreddits:
        for submission in reddit.subreddit(subreddit).new(limit=limit):
            posts.append(
                RedditPost(
                    source=f"r/{subreddit}",
                    title=submission.title,
                    url=submission.url,
                    created_utc=float(submission.created_utc),
                    author=getattr(submission, "author", None) and submission.author.name,
                    score=int(getattr(submission, "score", 0)),
                )
            )
    return posts


def fetch_user_posts(
    reddit,
    users: Iterable[str],
    limit: int = 5,
) -> List[RedditPost]:
    """Fetch the latest submissions made by the specified users."""

    posts: List[RedditPost] = []
    for username in users:
        redditor = reddit.redditor(username)
        for submission in redditor.submissions.new(limit=limit):
            posts.append(
                RedditPost(
                    source=f"u/{username}",
                    title=submission.title,
                    url=submission.url,
                    created_utc=float(submission.created_utc),
                    author=username,
                    score=int(getattr(submission, "score", 0)),
                )
            )
    return posts


def format_posts_for_markdown(posts: Iterable[RedditPost]) -> str:
    """Convert Reddit posts into Markdown bullet points."""

    lines: List[str] = []
    for post in posts:
        timestamp = post.created_datetime().strftime("%Y-%m-%d %H:%M")
        author = f" by u/{post.author}" if post.author else ""
        lines.append(
            f"- **{post.source}** ({timestamp} UTC{author}, score {post.score}):"
            f" [{post.title}]({post.url})"
        )
    return "\n".join(lines)


__all__ = [
    "RedditPost",
    "create_reddit_client",
    "fetch_subreddit_posts",
    "fetch_user_posts",
    "format_posts_for_markdown",
]


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Reddit posts")
    parser.add_argument(
        "--subreddits",
        nargs="*",
        default=None,
        help="Subreddits to read from (e.g. wallstreetbets).",
    )
    parser.add_argument(
        "--users",
        nargs="*",
        default=None,
        help="Reddit usernames to fetch submissions for.",
    )
    parser.add_argument("--limit", type=int, default=5, help="Posts per source (default: 5).")
    parser.add_argument("--reddit-client-id")
    parser.add_argument("--reddit-client-secret")
    parser.add_argument("--reddit-user-agent")
    return parser.parse_args(argv)


def _main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    if not (args.subreddits or args.users):
        raise SystemExit("Provide --subreddits and/or --users to fetch posts.")

    reddit = create_reddit_client(
        client_id=args.reddit_client_id,
        client_secret=args.reddit_client_secret,
        user_agent=args.reddit_user_agent,
    )

    chunks: List[str] = []
    if args.subreddits:
        subreddit_posts = fetch_subreddit_posts(reddit, args.subreddits, limit=args.limit)
        chunks.append(format_posts_for_markdown(subreddit_posts))
    if args.users:
        user_posts = fetch_user_posts(reddit, args.users, limit=args.limit)
        chunks.append(format_posts_for_markdown(user_posts))

    print("\n\n".join(filter(None, chunks)))


if __name__ == "__main__":  # pragma: no cover - convenience CLI
    _main()

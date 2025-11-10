"""Utilities for scraping the Trading Economics calendar."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Optional

import requests

BASE_URL = "https://api.tradingeconomics.com/calendar"
DEFAULT_KEY = "guest:guest"


@dataclass
class EconomicEvent:
    """Container for a single economic calendar entry."""

    date: str
    country: str
    category: str
    event: str
    actual: Optional[str]
    previous: Optional[str]
    forecast: Optional[str]
    importance: Optional[str]

    @classmethod
    def from_api_row(cls, row: dict) -> "EconomicEvent":
        """Build an :class:`EconomicEvent` from a Trading Economics API row."""

        return cls(
            date=row.get("Date", ""),
            country=row.get("Country", ""),
            category=row.get("Category", ""),
            event=row.get("Event", ""),
            actual=row.get("Actual"),
            previous=row.get("Previous"),
            forecast=row.get("Forecast"),
            importance=row.get("Importance"),
        )


def _determine_api_key(api_key: Optional[str]) -> str:
    """Return the API key to use, falling back to environment defaults."""

    return api_key or os.getenv("TRADING_ECONOMICS_KEY") or DEFAULT_KEY


def fetch_economic_calendar(
    days_ahead: int = 0,
    countries: Optional[Iterable[str]] = None,
    importance: Optional[Iterable[str]] = None,
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[EconomicEvent]:
    """Fetch economic calendar entries from Trading Economics.

    Parameters
    ----------
    days_ahead:
        Number of days ahead of today to include. ``0`` only retrieves today's
        events, while ``7`` would include the week ahead.
    countries:
        Optional iterable of country names to filter the response to.
    importance:
        Optional iterable of importance levels (``Low``, ``Medium``, ``High``).
    api_key:
        Trading Economics API key. If omitted the value from the
        ``TRADING_ECONOMICS_KEY`` environment variable is used, falling back to
        the public ``guest:guest`` key.
    session:
        Optional :class:`requests.Session` object to reuse connections.
    """

    session = session or requests.Session()
    key = _determine_api_key(api_key)

    today = date.today()
    end_date = today + timedelta(days=days_ahead)

    params = {
        "d1": today.strftime("%Y-%m-%d"),
        "d2": end_date.strftime("%Y-%m-%d"),
        "key": key,
        "format": "json",
    }

    if countries:
        params["country"] = ",".join(countries)
    if importance:
        params["importance"] = ",".join(importance)

    response = session.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if isinstance(payload, dict):
        rows = payload.get("data") or payload.get("calendar") or payload.get("Events")
    else:
        rows = payload

    if not rows:
        return []

    events = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        events.append(EconomicEvent.from_api_row(row))
    return events


def format_events_for_markdown(events: Iterable[EconomicEvent]) -> str:
    """Render a collection of events as a Markdown bullet list."""

    lines = []
    for event in events:
        details = []
        if event.actual:
            details.append(f"Actual: {event.actual}")
        if event.forecast:
            details.append(f"Forecast: {event.forecast}")
        if event.previous:
            details.append(f"Previous: {event.previous}")
        if event.importance:
            details.append(f"Importance: {event.importance}")

        detail_str = f" ({'; '.join(details)})" if details else ""
        lines.append(
            f"- **{event.date}** — {event.country} — {event.category}: "
            f"{event.event}{detail_str}"
        )

    return "\n".join(lines)


__all__ = [
    "EconomicEvent",
    "fetch_economic_calendar",
    "format_events_for_markdown",
]

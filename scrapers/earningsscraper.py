"""Scraper utilities for pulling the Nasdaq earnings calendar."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Optional

import requests

BASE_URL = "https://api.nasdaq.com/api/calendar/earnings"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nasdaq.com/",
}


@dataclass
class EarningsEvent:
    """Representation of a Nasdaq earnings calendar entry."""

    date: str
    symbol: str
    name: str
    eps_estimate: Optional[str]
    eps_actual: Optional[str]
    time: Optional[str]

    @classmethod
    def from_api_row(cls, row: dict, event_date: str) -> "EarningsEvent":
        return cls(
            date=event_date,
            symbol=row.get("symbol", "").upper(),
            name=row.get("company", ""),
            eps_estimate=row.get("epsForecast") or row.get("epsEstimate"),
            eps_actual=row.get("epsActual"),
            time=row.get("when"),
        )


def fetch_earnings_calendar(
    days_ahead: int = 0,
    session: Optional[requests.Session] = None,
) -> List[EarningsEvent]:
    """Retrieve the Nasdaq earnings calendar for the upcoming days."""

    session = session or requests.Session()

    today = date.today()
    end_date = today + timedelta(days=days_ahead)

    events: List[EarningsEvent] = []
    current = today

    while current <= end_date:
        formatted_date = current.strftime("%Y-%m-%d")
        response = session.get(
            BASE_URL,
            params={"date": formatted_date},
            headers=DEFAULT_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = []
        if isinstance(payload, dict):
            rows = (
                payload.get("data", {})
                .get("rows")
                or payload.get("data", {})
                .get("calendar", {})
                .get("rows", [])
            )
        if not rows:
            current += timedelta(days=1)
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue
            events.append(EarningsEvent.from_api_row(row, formatted_date))

        current += timedelta(days=1)

    return events


def format_events_for_markdown(events: Iterable[EarningsEvent]) -> str:
    """Render a collection of earnings events as Markdown."""

    lines: List[str] = []
    for event in events:
        details = []
        if event.eps_estimate:
            details.append(f"Est: {event.eps_estimate}")
        if event.eps_actual:
            details.append(f"Actual: {event.eps_actual}")
        if event.time:
            details.append(event.time)

        detail_str = f" ({', '.join(details)})" if details else ""
        lines.append(
            f"- **{event.date}** — {event.symbol} ({event.name}){detail_str}"
        )

    return "\n".join(lines)


__all__ = [
    "EarningsEvent",
    "fetch_earnings_calendar",
    "format_events_for_markdown",
]

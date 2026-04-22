#!/usr/bin/env python3
"""
FrontrowMD Dashboard Updater
Fetches demo data from Slack and deal data from Google Sheets,
then generates an updated index.html from the template.
"""

import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import google.auth.transport.requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SLACK_CHANNEL_ID = "C086BS5VD7A"
SLACK_CHANNEL_NAME = "demo-requests"
SEARCH_QUERY = '"booked a demo" in:#demo-requests'

SHEET_ID = "1tzaqix1CPQwzzatsFko3g-Wl2i6-4NepXDsxFJu3AoI"
SHEET_RANGE = "Signed customers"

# Names / substrings to treat as test data (case-insensitive)
TEST_FILTERS = [
    "test",
    "qa",
    "talar test",
    "tes tes",
    "bob more from testing",
    "talarish testing",
    "giacomo qa",
]

ET = timezone(timedelta(hours=-4))  # US Eastern Daylight Time (UTC-4)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def retry(func, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Call *func* with retries on transient errors."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_exc  # type: ignore[misc]


def ts_to_date(ts: str) -> str:
    """Convert a Slack message timestamp (epoch) to YYYY-MM-DD."""
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def is_test_message(text: str) -> bool:
    """Return True if the message text looks like a test/QA entry."""
    lower = text.lower()
    for pattern in TEST_FILTERS:
        if pattern.lower() in lower:
            return True
    return False


def parse_sheet_date(raw: str, default_year: int = 2026) -> str | None:
    """
    Parse dates that may appear as:
      - M/D/YYYY or MM/DD/YYYY
      - M/D  (no year -- assume *default_year*)
    Returns YYYY-MM-DD or None on failure.
    """
    raw = raw.strip()
    if not raw:
        return None

    # Try M/D/YYYY or MM/DD/YYYY
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Try M/D (no year)
    try:
        dt = datetime.strptime(raw, "%m/%d")
        dt = dt.replace(year=default_year)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Try ISO-ish formats just in case
    for fmt in ("%Y-%m-%d", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    log.debug("Could not parse date: %r", raw)
    return None


# ---------------------------------------------------------------------------
# Slack: fetch demo bookings
# ---------------------------------------------------------------------------
def fetch_demos_from_slack() -> dict[str, int]:
    """
    Use Slack search.messages to find all "booked a demo" messages in
    #demo-requests, filter to bot messages, exclude test names, and
    aggregate by day.
    """
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        log.error("SLACK_BOT_TOKEN environment variable is not set")
        sys.exit(1)

    client = WebClient(token=token)
    demos: dict[str, int] = defaultdict(int)

    page = 1
    total_fetched = 0

    while True:
        log.info("Slack search.messages â page %d â¦", page)

        response = retry(
            client.search_messages,
            query=SEARCH_QUERY,
            sort="timestamp",
            sort_dir="asc",
            count=100,
            page=page,
        )

        messages = response.get("messages", {})
        matches = messages.get("matches", [])

        if not matches:
            log.info("No more matches on page %d, stopping.", page)
            break

        for msg in matches:
            text = msg.get("text", "")

            # Only count bot messages
            is_bot = (
                msg.get("subtype") in ("bot_message",)
                or msg.get("bot_id")
                or msg.get("username") == "bot"
                or "bot_id" in msg
            )
            if not is_bot:
                continue

            # Must contain the trigger phrase
            if "booked a demo" not in text.lower():
                continue

            # Filter out test messages
            if is_test_message(text):
                continue

            date_str = ts_to_date(msg.get("ts", "0"))
            demos[date_str] += 1
            total_fetched += 1

        total_pages = messages.get("paging", {}).get("pages", page)
        log.info(
            "Page %d: %d matches (total pages: %d)", page, len(matches), total_pages
        )

        if page >= total_pages:
            break
        page += 1

    log.info("Total demo bookings fetched: %d across %d days", total_fetched, len(demos))
    return dict(sorted(demos.items()))


# ---------------------------------------------------------------------------
# Google Sheets: fetch deal sign-ups
# ---------------------------------------------------------------------------
def fetch_deals_from_sheets() -> dict[str, int]:
    """
    Read the "Signed customers" sheet and aggregate sign-up dates by day.
    """
    sa_json_raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not sa_json_raw:
        log.error("GOOGLE_SERVICE_ACCOUNT_JSON environment variable is not set")
        sys.exit(1)

    sa_info = json.loads(sa_json_raw)
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )

    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    log.info("Fetching Google Sheet %s, range '%s' â¦", SHEET_ID, SHEET_RANGE)
    result = retry(
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
        .execute
    )

    rows = result.get("values", [])
    if not rows:
        log.warning("Sheet returned no rows.")
        return {}

    # Find the "Date of sign up" column
    header = [h.strip().lower() for h in rows[0]]
    date_col = None
    for idx, col_name in enumerate(header):
        if "date" in col_name and "sign" in col_name:
            date_col = idx
            break

    if date_col is None:
        # Fallback: try first column that contains "date"
        for idx, col_name in enumerate(header):
            if "date" in col_name:
                date_col = idx
                break

    if date_col is None:
        log.error("Could not find a 'Date of sign up' column in header: %s", rows[0])
        sys.exit(1)

    log.info("Using column %d ('%s') for dates.", date_col, rows[0][date_col])

    deals: dict[str, int] = defaultdict(int)
    skipped = 0

    for row in rows[1:]:
        if date_col >= len(row):
            skipped += 1
            continue
        raw_date = row[date_col]
        parsed = parse_sheet_date(raw_date)
        if parsed is None:
            skipped += 1
            continue
        deals[parsed] += 1

    log.info(
        "Total deals fetched: %d across %d days (%d rows skipped)",
        sum(deals.values()),
        len(deals),
        skipped,
    )
    return dict(sorted(deals.items()))


# ---------------------------------------------------------------------------
# Generate index.html from template
# ---------------------------------------------------------------------------
def generate_html(demos: dict[str, int], deals: dict[str, int]) -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(script_dir, "template.html")
    output_path = os.path.join(script_dir, "index.html")

    log.info("Reading template from %s", template_path)
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    now_et = datetime.now(ET)
    update_date = now_et.strftime("%B %d, %Y at %I:%M %p ET").replace(" 0", " ")

    html = html.replace("__DEMOS_DATA__", json.dumps(demos, separators=(",", ":")))
    html = html.replace("__DEALS_DATA__", json.dumps(deals, separators=(",", ":")))
    html = html.replace("__UPDATE_DATE__", update_date)

    log.info("Writing output to %s", output_path)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    log.info("Dashboard updated successfully: %s", update_date)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=== FrontrowMD Dashboard Update Starting ===")

    demos = fetch_demos_from_slack()
    deals = fetch_deals_from_sheets()
    generate_html(demos, deals)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()

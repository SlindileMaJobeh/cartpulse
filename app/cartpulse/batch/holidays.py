"""REST API ingestion: South African public holidays from the Nager.Date API.

    GET https://date.nager.at/api/v3/PublicHolidays/{year}/ZA
    -> [{"date": "2026-09-24", "localName": "Heritage Day", "name": "Heritage Day", "countryCode": "ZA", ...}]

Used to flag holidays in dim_date, so sales can be compared on holidays vs
normal days. Falls back to a built-in list if the API is unreachable.
"""
from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
import requests

from cartpulse import catalog
from cartpulse.batch import warehouse

API = "https://date.nager.at/api/v3/PublicHolidays/{year}/ZA"
log = logging.getLogger(__name__)


def fetch_year(year: int, retries: int = 3) -> list[dict]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(API.format(year=year), timeout=10, headers={"User-Agent": "cartpulse/1.0"})
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("attempt %s/%s for %s failed: %s", attempt, retries, year, exc)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"holiday API unavailable for {year}")


def parse(payload: list[dict]) -> pd.DataFrame:
    rows = []
    for item in payload:
        if item.get("countryCode") not in (None, "ZA"):
            raise ValueError(f"unexpected country {item.get('countryCode')!r}")
        rows.append({"holiday_date": date.fromisoformat(item["date"]), "name": item.get("name") or item["localName"]})
    return pd.DataFrame(rows, columns=["holiday_date", "name"])


def collect(years: list[int]) -> pd.DataFrame:
    frames = []
    for year in years:
        try:
            frames.append(parse(fetch_year(year)).assign(source="nager.date"))
        except Exception as exc:  # noqa: BLE001
            log.error("falling back to built-in holidays for %s: %s", year, exc)
            fallback = [{"holiday_date": date.fromisoformat(d), "name": n}
                        for d, n in catalog.SA_HOLIDAYS_2026.items() if d.startswith(str(year))]
            frames.append(pd.DataFrame(fallback, columns=["holiday_date", "name"]).assign(source="fallback"))
    return pd.concat(frames, ignore_index=True)


def load(years: list[int] | None = None) -> int:
    this_year = date.today().year
    df = collect(years or [this_year - 1, this_year, this_year + 1])
    with warehouse.connect() as conn:
        return warehouse.upsert(conn, df, "raw.public_holidays", ["holiday_date"], update=True)

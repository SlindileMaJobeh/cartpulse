"""Courier delivery files: inbox -> data-quality checks -> warehouse (+ quarantine).

Each file in data/inbox/courier is processed exactly once (ledger in
ops.ingested_files), then moved to data/archive/courier. Rows that break a
critical rule go to data/lake/quarantine/courier/ with the reasons, so nothing
is silently dropped; fixable problems (status casing, a second date format)
are repaired on the way in.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from cartpulse import config
from cartpulse.batch import warehouse

COLUMNS = ["waybill", "order_id", "courier", "province", "collected_at", "delivered_at", "status", "attempts"]
STATUSES = {"DELIVERED", "FAILED_ATTEMPT", "RETURNED_TO_SENDER", "IN_TRANSIT"}


@dataclass(frozen=True)
class Rule:
    name: str
    dimension: str
    check: Callable[[pd.DataFrame], pd.Series]      # True = row is OK


def parse_ts(s: pd.Series) -> pd.Series:
    """Partner timestamps are SAST; two formats seen in the wild."""
    iso = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    dmy = pd.to_datetime(s, format="%d/%m/%Y %H:%M", errors="coerce")
    return iso.fillna(dmy).dt.tz_localize("Africa/Johannesburg")


def standardise(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    for c in COLUMNS:
        df[c] = df[c].fillna("").astype(str).str.strip()
    df["status"] = df["status"].str.upper()
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").astype("Int64")
    df["attempts"] = pd.to_numeric(df["attempts"], errors="coerce").astype("Int64")
    df["delivered_raw"] = df["delivered_at"]
    df["collected_at"] = parse_ts(df["collected_at"])
    df["delivered_at"] = parse_ts(df["delivered_at"])
    return df


RULES = [
    Rule("order_id_present", "completeness", lambda d: d["order_id"].notna()),
    Rule("waybill_present", "completeness", lambda d: d["waybill"] != ""),
    Rule("status_known", "validity", lambda d: d["status"].isin(STATUSES)),
    Rule("collected_at_valid", "validity", lambda d: d["collected_at"].notna()),
    Rule("delivered_at_valid", "validity", lambda d: (d["delivered_raw"] == "") | d["delivered_at"].notna()),
    Rule("delivered_has_timestamp", "completeness",
         lambda d: (d["status"] != "DELIVERED") | d["delivered_at"].notna()),
    Rule("delivered_after_collected", "consistency",
         lambda d: d["delivered_at"].isna() | (d["delivered_at"] >= d["collected_at"])),
    Rule("attempts_in_range", "validity", lambda d: d["attempts"].between(0, 5).fillna(False).astype(bool)),
]


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    """Returns (good rows, rejected rows with reasons, per-rule results)."""
    ok = pd.DataFrame({r.name: r.check(df).fillna(False).astype(bool) for r in RULES}, index=df.index)
    reasons = ok.apply(lambda row: ",".join(row.index[~row]), axis=1) if len(df) else pd.Series(dtype=str)
    rejected = df[reasons != ""].assign(failed_rules=reasons[reasons != ""])
    good = df[reasons == ""]
    deduped = good.drop_duplicates(subset=["waybill", "status"])
    results = [{"rule": r.name, "dimension": r.dimension, "checked": len(df), "failed": int((~ok[r.name]).sum())}
               for r in RULES]
    results.append({"rule": "unique_waybill_status", "dimension": "uniqueness", "checked": len(good),
                    "failed": len(good) - len(deduped)})
    return deduped, rejected, results


def pending_files(inbox: Path) -> list[Path]:
    return sorted(p for p in inbox.glob("*.csv") if not p.name.startswith("."))


def ingest(run_id: str, inbox: Path | None = None) -> dict:
    inbox = inbox or config.INBOX / "courier"
    archive = config.ARCHIVE / "courier"
    quarantine = config.QUARANTINE / "courier"
    archive.mkdir(parents=True, exist_ok=True)
    quarantine.mkdir(parents=True, exist_ok=True)
    summary = {"files": 0, "loaded": 0, "rejected": 0}

    with warehouse.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_name FROM ops.ingested_files WHERE pipeline = 'courier'")
            done = {r[0] for r in cur.fetchall()}

    for path in pending_files(inbox):
        if path.name in done:                # crashed after loading but before archiving last time
            shutil.move(str(path), archive / path.name)
            continue
        raw = pd.read_csv(path, dtype=str, keep_default_na=False)
        good, rejected, results = validate(standardise(raw))
        out = good[COLUMNS].assign(source_file=path.name)
        with warehouse.connect() as conn:    # load + ledger + DQ in ONE transaction
            loaded = warehouse.upsert(conn, out, "raw.deliveries", ["waybill", "status"])
            warehouse.record_dq(conn, run_id, "courier", results)
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO ops.ingested_files (file_name, pipeline, rows_loaded, rows_rejected)
                               VALUES (%s, 'courier', %s, %s)""", (path.name, loaded, len(rejected)))
        if len(rejected):
            rejected.drop(columns=["delivered_raw"]).to_csv(quarantine / f"{path.stem}.rejected.csv", index=False)
        shutil.move(str(path), archive / path.name)
        summary["files"] += 1
        summary["loaded"] += loaded
        summary["rejected"] += len(rejected)
    return summary

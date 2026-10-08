"""Loading into the warehouse, idempotently.

Pattern used everywhere: COPY the batch into a TEMP table (fast, no locks on
the real table), then one INSERT ... SELECT ... ON CONFLICT into raw.*, all in
one transaction. A re-run inserts nothing new; a crash leaves nothing behind.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pandas as pd
import psycopg

from cartpulse import config


@contextmanager
def connect(dsn: str | None = None):
    conn = psycopg.connect(dsn or config.WAREHOUSE_DSN)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _clean(v):
    if v is None or v is pd.NaT or v is pd.NA:
        return None
    if isinstance(v, float) and v != v:
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def upsert(conn, df: pd.DataFrame, table: str, key: list[str], update: bool = False) -> int:
    """Insert df into table; on key conflict do nothing (or update the other columns)."""
    if df.empty:
        return 0
    cols = list(df.columns)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE _stage (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        with cur.copy(f"COPY _stage ({', '.join(cols)}) FROM STDIN") as copy:
            for row in df.itertuples(index=False, name=None):
                copy.write_row([_clean(v) for v in row])
        if update:
            sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
            action = f"DO UPDATE SET {sets}, loaded_at = now()"
        else:
            action = "DO NOTHING"
        cur.execute(f"""INSERT INTO {table} ({', '.join(cols)})
                        SELECT DISTINCT ON ({', '.join(key)}) {', '.join(cols)} FROM _stage
                        ON CONFLICT ({', '.join(key)}) {action}""")
        n = cur.rowcount
        cur.execute("DROP TABLE _stage")
        return n


def record_dq(conn, run_id: str, dataset: str, results: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO ops.dq_results (run_id, dataset, rule_name, dimension, checked_rows, failed_rows, run_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            [(run_id, dataset, r["rule"], r["dimension"], r["checked"], r["failed"], datetime.now(timezone.utc))
             for r in results])

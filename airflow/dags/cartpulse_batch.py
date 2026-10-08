"""CartPulse batch layer, every 30 minutes.

    ingest_courier_files ─────────────────────────────┐
    spark_sessionize ──► load_sessions ───────────────┤
    load_cdc_events ──────────────────────────────────┼──► dbt_build
    load_abandoned_carts ─────────────────────────────┘

Every task is idempotent (file ledger, ON CONFLICT upserts, partition
overwrites, dbt full rebuilds), so a failed run is fixed by re-running it.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {"owner": "cartpulse", "retries": 2, "retry_delay": timedelta(minutes=1),
                "execution_timeout": timedelta(minutes=20)}


@dag(
    dag_id="cartpulse_batch",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cartpulse", "batch"],
    doc_md=__doc__,
)
def cartpulse_batch():
    @task
    def ingest_courier_files(run_id=None) -> dict:
        from cartpulse.batch import courier
        summary = courier.ingest(run_id)
        print(summary)
        return summary

    sessionize = BashOperator(
        task_id="spark_sessionize",
        bash_command="python -m cartpulse.batch.sessionize --days 2",
        pool="spark",
    )

    @task
    def load_sessions() -> int:
        from cartpulse.batch import lake_loader
        return lake_loader.load_sessions(days=2)

    @task
    def load_cdc_events() -> int:
        from cartpulse.batch import lake_loader
        return lake_loader.load_cdc(days=2)

    @task
    def load_abandoned_carts() -> int:
        from cartpulse.batch import lake_loader
        return lake_loader.load_abandoned()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/cartpulse/dbt && dbt build",
    )

    sessions = load_sessions()
    sessionize >> sessions
    [ingest_courier_files(), sessions, load_cdc_events(), load_abandoned_carts()] >> dbt_build


cartpulse_batch()

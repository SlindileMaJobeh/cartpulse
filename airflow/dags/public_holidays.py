"""Weekly: South African public holidays from a public REST API -> raw.public_holidays."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="public_holidays",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "cartpulse", "retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["cartpulse", "rest-api"],
    doc_md=__doc__,
)
def public_holidays():
    @task
    def load_holidays() -> int:
        from cartpulse.batch import holidays
        return holidays.load()

    load_holidays()


public_holidays()

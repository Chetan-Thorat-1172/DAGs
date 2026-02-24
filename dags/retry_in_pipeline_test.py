from dag_parser.dynamic.dag_context import DAG, PythonOperator
from datetime import datetime 


# ---------------------------
# Stable tasks
# ---------------------------
def simple_task(name):
    print(f"Executing {name}")


# ---------------------------
# Flaky task (fails first 2 times)
# ---------------------------
def unstable_task(**context):
    ti = context["ti"]

    print(f"[unstable_task] TRY_NUMBER: {ti.try_number}")

    if ti.try_number < 3:
        print("[unstable_task] Simulating failure...")
        raise Exception("Intentional failure to test retry logic")

    print("[unstable_task] Success after retries!")


with DAG(
    dag_id="retry_in_pipeline_test",
    schedule_interval=None,
    start_date=datetime(2026, 2, 23),
    catchup=False,
) as dag:

    # -----------------------------
    # Start
    # -----------------------------
    start = PythonOperator(
        task_id="start",
        python_callable=lambda **_: simple_task("start"),
    )

    # -----------------------------
    # Extract
    # -----------------------------
    extract = PythonOperator(
        task_id="extract",
        python_callable=lambda **_: simple_task("extract"),
    )

    # -----------------------------
    # Unstable Task (Retry Enabled)
    # -----------------------------
    unstable_transform = PythonOperator(
        task_id="unstable_transform",
        python_callable=unstable_task,
        retries=2,
        retry_delay_seconds=80,
    )

    # -----------------------------
    # Load
    # -----------------------------
    load = PythonOperator(
        task_id="load",
        python_callable=lambda **_: simple_task("load"),
    )

    # -----------------------------
    # Notify
    # -----------------------------
    notify = PythonOperator(
        task_id="notify",
        python_callable=lambda **_: simple_task("notify"),
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    start >> extract >> unstable_transform >> load >> notify

"""
Parent DAG — Demonstrates TriggerDagRunOperator.

This DAG:
1. Runs a preparation task (extract_config)
2. Triggers child_dag via TriggerDagRunOperator, passing conf parameters
3. Waits for the child DAG to complete (wait_for_completion=True)
4. Runs a post-processing task after the child succeeds

Flow:
    extract_config >> trigger_child >> post_process

This is the pi-flow equivalent of Airflow's TriggerDagRunOperator pattern.
The parent DAG does NOT call the child DAG's code directly. Instead:
    - The worker INSERTs a new DAG_RUN row for child_dag
    - The scheduler picks it up and creates task instances
    - The child DAG executes normally
    - The parent polls DAG_RUN.STATE until the child completes..
"""

from datetime import datetime
from dag_parser.dynamic.dag_context import PythonOperator, TriggerDagRunOperator
from dag_parser.dynamic.dag_context import DAG


def extract_config(**context):
    """Prepare configuration to pass to the child DAG."""
    config = {
        "APPCODE": "BIC_APP_001",
        "curation_database": "PROD_DB",
        "restartind": "N",
    }
    print(f"Extracted config for child DAG: {config}")
    return config


def post_process(**context):
    """Run after the child DAG has completed successfully."""
    ti = context["ti"]
    trigger_run_id = ti.xcom_pull(task_ids="trigger_child", key="trigger_run_id")
    print(f"=== Post-Processing ===")
    print(f"  Child DAG run completed: {trigger_run_id}")
    print(f"  Parent DAG post-processing done.")
    return "parent_complete"


with DAG(
    dag_id="parent_trigger_dag",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Parent DAG that triggers child_dag via TriggerDagRunOperator",
) as dag:

    t_extract_config = PythonOperator(
        task_id="extract_config",
        python_callable=extract_config,
    )

    t_trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        conf={
            "APPCODE": "BIC_APP_001",
            "curation_database": "PROD_DB",
            "restartind": "N",
        },
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    t_post_process = PythonOperator(
        task_id="post_process",
        python_callable=post_process,
    )

    t_extract_config >> t_trigger_child >> t_post_process

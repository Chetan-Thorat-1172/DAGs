from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.param import Param
from datetime import datetime, timedelta
import random
#testing

# -----------------------------
# Default args
# -----------------------------
default_args = {
    "owner": "pi-flow",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}


# -----------------------------
# Python callables
# -----------------------------
def start():
    print("🚀 Pipeline started")


def choose_branch(**context):
    params = context["dag_run"].conf or {}
    full_load = params.get("full_load", False)

    if full_load:
        print("👉 FULL LOAD selected")
        return "full_load_task"
    else:
        print("👉 INCREMENTAL LOAD selected")
        return "incremental_load_task"


def full_load():
    print("📦 Executing FULL LOAD logic")


def incremental_load():
    print("🔄 Executing INCREMENTAL LOAD logic")


def preprocess():
    print("🧹 Preprocessing data")


def dynamic_worker(task_number):
    print(f"⚙️ Processing chunk {task_number}")
    if random.random() < 0.1:
        print("⚠️ Simulated intermittent issue")


def aggregate():
    print("📊 Aggregating results")


def finalize():
    print("✅ Pipeline completed successfully")


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="complex_pipeline_demo",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    params={
        "full_load": Param(
            default=False,
            type="boolean",
            description="Whether to run full load"
        ),
        "batch_size": Param(
            default=5,
            type="integer",
            description="Number of dynamic tasks"
        ),
    },
    tags=["demo", "complex"],
) as dag:

    # -------------------------
    # Core flow
    # -------------------------
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start,
    )

    branching = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_branch,
    )

    full_load_task = PythonOperator(
        task_id="full_load_task",
        python_callable=full_load,
    )

    incremental_load_task = PythonOperator(
        task_id="incremental_load_task",
        python_callable=incremental_load,
    )

    join = EmptyOperator(
        task_id="join_branches",
        trigger_rule="none_failed_min_one_success",
    )

    # -------------------------
    # Task Group
    # -------------------------
    with TaskGroup("processing_group") as processing_group:

        preprocess_task = PythonOperator(
            task_id="preprocess",
            python_callable=preprocess,
        )

        # Dynamic tasks
        dynamic_tasks = []
        for i in range(5):   # static count for parser safety
            task = PythonOperator(
                task_id=f"dynamic_task_{i}",
                python_callable=dynamic_worker,
                op_args=[i],
            )
            dynamic_tasks.append(task)

        aggregate_task = PythonOperator(
            task_id="aggregate_results",
            python_callable=aggregate,
        )

        preprocess_task >> dynamic_tasks >> aggregate_task

    finalize_task = PythonOperator(
        task_id="finalize",
        python_callable=finalize,
    )

    # -------------------------
    # Dependencies
    # -------------------------
    start_task >> branching
    branching >> [full_load_task, incremental_load_task]
    full_load_task >> join
    incremental_load_task >> join
    join >> processing_group >> finalize_task

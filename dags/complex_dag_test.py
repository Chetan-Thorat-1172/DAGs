from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "pi-flow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}


def start():
    print("Pipeline started")


def choose_branch(**context):
    import random
    branch = random.choice(["branch_a", "branch_b"])
    print(f"Chosen branch: {branch}")
    return branch


def process_data(task_number, **context):
    print(f"Processing chunk {task_number}")
    return f"result_{task_number}"


def aggregate(**context):
    ti = context["ti"]
    results = []

    for i in range(5):
        res = ti.xcom_pull(task_ids=f"dynamic_task_{i}")
        results.append(res)

    print("Aggregated results:", results)


with DAG(
    dag_id="complex_test_dag",
    default_args=default_args,
    description="Complex DAG for Pi-Flow parser testing",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test", "complex"],
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start,
    )

    branching = BranchPythonOperator(
        task_id="branching_decision",
        python_callable=choose_branch,
        provide_context=True,
    )

    branch_a = EmptyOperator(task_id="branch_a")
    branch_b = EmptyOperator(task_id="branch_b")

    join = EmptyOperator(
        task_id="join_branches",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ✅ TaskGroup Example
    with TaskGroup("data_processing_group") as processing_group:

        preprocess = EmptyOperator(task_id="preprocess")

        dynamic_tasks = []
        for i in range(5):
            task = PythonOperator(
                task_id=f"dynamic_task_{i}",
                python_callable=process_data,
                op_kwargs={"task_number": i},
            )
            dynamic_tasks.append(task)

        aggregate_task = PythonOperator(
            task_id="aggregate_results",
            python_callable=aggregate,
            provide_context=True,
        )

        preprocess >> dynamic_tasks >> aggregate_task

    finalize = EmptyOperator(task_id="finalize")

    # ✅ Dependencies
    start_task >> branching
    branching >> branch_a >> join
    branching >> branch_b >> join
    join >> processing_group >> finalize

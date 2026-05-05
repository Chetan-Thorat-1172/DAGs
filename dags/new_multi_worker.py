from dag_parser.dynamic.dag_context import (
    DAG,
    PythonOperator,
)
from datetime import datetime


# -------------------------------------------------
# Dummy Task Logic
# -------------------------------------------------
def task_logic(name):
    print(f"Executing {name}")


with DAG(
    dag_id="parallel_20_tasks_dag",
    schedule_interval=None,
    start_date=datetime(2026, 3, 30),
    catchup=False,
) as dag:

    # =============================
    # PARALLEL TASKS
    # =============================
    tasks = []

    for i in range(1, 21):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=lambda i=i, **_: task_logic(f"task_{i}"),
        )
        tasks.append(task)

    # =============================
    # OPTIONAL FINAL TASK
    # =============================
    finalize = PythonOperator(
        task_id="finalize",
        python_callable=lambda **_: print("All parallel tasks completed"),
        trigger_rule="none_failed_min_one_success",
    )

    # -------------------------------------------------
    # Dependencies
    # -------------------------------------------------
    # All tasks run in parallel, then converge
    tasks >> finalize

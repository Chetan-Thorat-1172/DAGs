from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator, BranchPythonOperator
from datetime import datetime


# -----------------------------
# Branch Decision Function
# -----------------------------
def choose_pipeline(**context):
    dag_run = context["dag_run"]
    params = dag_run.conf or {}

    if params.get("full_load"):
        return "extract_full"
    else:
        return "extract_incremental"


# -----------------------------
# Dummy Task Logic
# -----------------------------
def task_logic(name):
    print(f"Executing {name}")


with DAG(
    dag_id="complex_branch_test_dag",
    schedule_interval=None,
    start_date=datetime(2026, 2, 13),
    catchup=False,
) as dag:

    # -----------------------------
    # Branch Task
    # -----------------------------
    decide_pipeline = BranchPythonOperator(
        task_id="decide_pipeline",
        python_callable=choose_pipeline,
    )

    # =============================
    # FULL LOAD BRANCH
    # =============================
    extract_full = PythonOperator(
        task_id="extract_full",
        python_callable=lambda **_: task_logic("extract_full"),
    )

    validate_full = PythonOperator(
        task_id="validate_full",
        python_callable=lambda **_: task_logic("validate_full"),
    )

    transform_full = PythonOperator(
        task_id="transform_full",
        python_callable=lambda **_: task_logic("transform_full"),
    )

    load_full = PythonOperator(
        task_id="load_full",
        python_callable=lambda **_: task_logic("load_full"),
    )

    audit_full = PythonOperator(
        task_id="audit_full",
        python_callable=lambda **_: task_logic("audit_full"),
    )

    # =============================
    # INCREMENTAL LOAD BRANCH
    # =============================
    extract_incremental = PythonOperator(
        task_id="extract_incremental",
        python_callable=lambda **_: task_logic("extract_incremental"),
    )

    validate_incremental = PythonOperator(
        task_id="validate_incremental",
        python_callable=lambda **_: task_logic("validate_incremental"),
    )

    transform_incremental = PythonOperator(
        task_id="transform_incremental",
        python_callable=lambda **_: task_logic("transform_incremental"),
    )

    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=lambda **_: task_logic("load_incremental"),
    )

    audit_incremental = PythonOperator(
        task_id="audit_incremental",
        python_callable=lambda **_: task_logic("audit_incremental"),
    )

    # =============================
    # JOIN TASK
    # =============================
    join_results = PythonOperator(
        task_id="join_results",
        python_callable=lambda **_: print("Joining branches"),
        trigger_rule="none_failed_min_one_success"
    )

    # =============================
    # FINAL TASK
    # =============================
    finalize = PythonOperator(
        task_id="finalize",
        python_callable=lambda **_: print("Pipeline Completed"),
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    decide_pipeline >> [extract_full, extract_incremental]

    extract_full >> validate_full >> transform_full >> load_full >> audit_full
    extract_incremental >> validate_incremental >> transform_incremental >> load_incremental >> audit_incremental

    audit_full >> join_results
    audit_incremental >> join_results

    join_results >> finalize

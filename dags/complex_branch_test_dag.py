from dag_parser.dynamic.dag_context import (  
    DAG,
    PythonOperator,
    BranchPythonOperator,
)
from dag_parser.dynamic.params import Param   
from datetime import datetime  


# -------------------------------------------------
# Branch Decision Function
# -------------------------------------------------
def choose_pipeline(**context):
    dag_run = context["dag_run"]
    params = dag_run.conf or {}

    full_load = params.get("full_load", False)

    print(f"Branch decision - full_load: {full_load}")

    if full_load:
        return "extract_full"
    else:
        return "extract_incremental"


# -------------------------------------------------
# Dummy Task Logic
# -------------------------------------------------
def task_logic(name):
    print(f"Executing {name}")


with DAG(
    dag_id="complex_branch_test_dag",
    schedule_interval=None,
    start_date=datetime(2026, 2, 13),
    catchup=False,

    params={
        "full_load": Param(
            type="boolean",
            default=False,
            description="If true then run full load branch. Else incremental."
        )
    },
) as dag:

    # =============================
    # BRANCH DECISION TASK
    # =============================
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
        trigger_rule="none_failed"
    )

    validate_full = PythonOperator(
        task_id="validate_full",
        python_callable=lambda **_: task_logic("validate_full"),
        trigger_rule="none_failed"
    )

    transform_full = PythonOperator(
        task_id="transform_full",
        python_callable=lambda **_: task_logic("transform_full"),
        trigger_rule="none_failed"
    )

    load_full = PythonOperator(
        task_id="load_full",
        python_callable=lambda **_: task_logic("load_full"),
        trigger_rule="none_failed"
    )

    audit_full = PythonOperator(
        task_id="audit_full",
        python_callable=lambda **_: task_logic("audit_full"),
        trigger_rule="none_failed"
    )

    # =============================
    # INCREMENTAL LOAD BRANCH
    # =============================
    extract_incremental = PythonOperator(
        task_id="extract_incremental",
        python_callable=lambda **_: task_logic("extract_incremental"),
        trigger_rule="none_failed"
    )

    validate_incremental = PythonOperator(
        task_id="validate_incremental",
        python_callable=lambda **_: task_logic("validate_incremental"),
        trigger_rule="none_failed"
    )

    transform_incremental = PythonOperator(
        task_id="transform_incremental",
        python_callable=lambda **_: task_logic("transform_incremental"),
        trigger_rule="none_failed"
    )

    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=lambda **_: task_logic("load_incremental"),
        trigger_rule="none_failed"
    )

    audit_incremental = PythonOperator(
        task_id="audit_incremental",
        python_callable=lambda **_: task_logic("audit_incremental"),
        trigger_rule="none_failed"
    )

    # =============================
    # JOIN TASK
    # =============================
    join_results = PythonOperator(
        task_id="join_results",
        python_callable=lambda **_: print("Joining branches"),
        trigger_rule="none_failed_min_one_success",
    )

    # =============================
    # FINAL TASK
    # =============================
    finalize = PythonOperator(
        task_id="finalize",
        python_callable=lambda **_: print("Pipeline Completed"),
    )

    # -------------------------------------------------
    # Dependencies
    # -------------------------------------------------
    decide_pipeline >> [extract_full, extract_incremental]

    extract_full >> validate_full >> transform_full >> load_full >> audit_full
    extract_incremental >> validate_incremental >> transform_incremental >> load_incremental >> audit_incremental

    audit_full >> join_results
    audit_incremental >> join_results

    join_results >> finalize

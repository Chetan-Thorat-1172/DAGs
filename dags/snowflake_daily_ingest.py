from dag_parser.dynamic.dag_context import (
    DAG,
    BranchPythonOperator,
    PythonOperator,
)
from dag_parser.dynamic.dag_context import SnowflakeOperator
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
# Final Logging
# -------------------------------------------------
def finalize_pipeline(**context):
    print("Pipeline execution completed successfully.")


with DAG(
    dag_id="snowflake_daily_ingest",
    schedule_interval="0 14 * * *",
    start_date=datetime(2026, 5, 26),
    catchup=False,

    params={
        "processDate": Param(
            type="string",
            default="2026-02-01",
            description="Processing date"
        ),
        "APPCODE": Param(
            type="string",
            required=True,
            description="Application Code"
        ),
        "full_load": Param(
            type="boolean",
            default=False,
            description="If true run full load branch"
        ),
    },
) as dag:

    # =============================
    # BRANCH DECISION
    # =============================
    decide_pipeline = BranchPythonOperator(
        task_id="decide_pipeline",
        python_callable=choose_pipeline,
    )

    # =============================
    # FULL LOAD BRANCH
    # =============================

    extract_full = SnowflakeOperator(
        task_id="extract_full",
        sql="""
        CALL PI_FLOW.APP.SP_EXTRACT_FULL(
            %(processDate)s,
            %(APPCODE)s
        );
        """,
        connection_id="snowflake_default",
    )

    load_full = SnowflakeOperator(
        task_id="load_full",
        sql="""
        CALL PI_FLOW.APP.SP_LOAD_FULL(
            %(processDate)s,
            %(APPCODE)s
        );
        """,
        connection_id="snowflake_default",
    )

    # =============================
    # INCREMENTAL LOAD BRANCH
    # =============================

    extract_incremental = SnowflakeOperator(
        task_id="extract_incremental",
        sql="""
        CALL PI_FLOW.APP.SP_EXTRACT_INCREMENTAL(
            %(processDate)s,
            %(APPCODE)s
        );
        """,
        connection_id="snowflake_default",
    )

    load_incremental = SnowflakeOperator(
        task_id="load_incremental",
        sql="""
        CALL PI_FLOW.APP.SP_LOAD_INCREMENTAL(
            %(processDate)s,
            %(APPCODE)s
        );
        """,
        connection_id="snowflake_default",
    )

    # =============================
    # JOIN
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
        python_callable=finalize_pipeline,
        trigger_rule="always",
    )

    # -------------------------------------------------
    # Dependencies
    # -------------------------------------------------
    decide_pipeline >> [extract_full, extract_incremental]

    extract_full >> load_full >> join_results
    extract_incremental >> load_incremental >> join_results

    join_results >> finalize

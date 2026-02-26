from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator, PythonOperator
from dag_parser.dynamic.params import Param
from datetime import datetime


# -----------------------------
# Simple Logging Function
# -----------------------------
def log_success(**context):
    print("Snowflake procedure executed successfully.")


def log_error(**context):
    print("Snowflake procedure failed.")


with DAG(
    dag_id="snowflake_sp_test_with_params",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,

    #  Runtime parameters (shown in UI)
    params={
        "processDate": Param(
            type="string",
            default="20240101",
            description="Processing date"
        ),
        "APPCODE": Param(
            type="string",
            required=True,
            description="Application Code"
        ),
        "restartind": Param(
            type="string",
            default="N",
            description="Restart Indicator (Y/N)"
        ),
    },
) as dag:

    # -----------------------------
    # Call Stored Procedure
    # -----------------------------
    call_snowflake_proc = SnowflakeOperator(
        task_id="call_snowflake_proc",
        sql="""
        CALL PI_FLOW.APP.SP_PROCESS_DATA(
            %(processDate)s,
            %(APPCODE)s,
            %(restartind)s
        );
        """,
    )

    # -----------------------------
    # Success Log
    # -----------------------------
    log_completion = PythonOperator(
        task_id="log_completion",
        python_callable=log_success,
        trigger_rule="all_success",
    )

    # -----------------------------
    # Error Log
    # -----------------------------
    log_failure = PythonOperator(
        task_id="log_failure",
        python_callable=log_error,
        trigger_rule="one_failed",
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    call_snowflake_proc >> [log_completion, log_failure]

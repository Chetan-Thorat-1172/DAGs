from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator
from dag_parser.dynamic.params import Param
from datetime import datetime


# -----------------------------
# Default connection (DAG level)
# -----------------------------
default_args = {
    "snowflake_conn_id": "harsh_conn"
}


with DAG(
    dag_id="test_snowflake_sp_multi_conn",
    schedule_interval=None,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    default_args=default_args,

    # Runtime params (UI)
    params={
        "run_date": Param(type="string", default="2026-03-01"),
        "batch_id": Param(type="string", default="BATCH_001"),
        "env": Param(type="string", default="DEV"),
    },
) as dag:

    # -----------------------------
    # Task 1 → Uses default connection
    # -----------------------------
    sp_task_1 = SnowflakeOperator(
        task_id="call_sp_insert_log",
        sql="""
        CALL PI_FLOW.APP.SP_INSERT_LOG(
            %(run_date)s,
            %(batch_id)s,
            %(env)s
        );
        """
    )

    # -----------------------------
    # Task 2 → Override connection
    # -----------------------------
    sp_task_2 = SnowflakeOperator(
        task_id="call_sp_process_data",
        sql="""
        CALL PI_FLOW.APP.SP_PROCESS_DATA(
            %(run_date)s,
            %(batch_id)s
        );
        """
    )

    # -----------------------------
    # Task 3 → Back to default connection
    # -----------------------------
    sp_task_3 = SnowflakeOperator(
        task_id="call_sp_finalize",
        sql="""
        CALL PI_FLOW.APP.SP_FINALIZE_RUN(
            %(batch_id)s
        );
        """
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    sp_task_1 >> sp_task_2 >> sp_task_3
	
	
	
	
	
	
	

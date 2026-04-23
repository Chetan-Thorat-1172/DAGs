from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator
from dag_parser.dynamic.params import Param
from datetime import datetime

default_args = {
    "snowflake_conn_id": "chetan_conn"
}

with DAG(
    dag_id="test_snowflake_simple",
    schedule_interval=None,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    default_args=default_args,
    params={
        "run_date": Param(type="string", default="2026-03-01"),
        "batch_id": Param(type="string", default="BATCH_001"),
        "env": Param(type="string", default="DEV"),
    },
) as dag:

    # Task 1: Create table
    task_create = SnowflakeOperator(
        task_id="create_run_log_table",
        sql="""
        CREATE TABLE IF NOT EXISTS PI_FLOW.APP.RUN_LOG (
            log_id     NUMBER AUTOINCREMENT PRIMARY KEY,
            run_date   DATE,
            batch_id   VARCHAR,
            env        VARCHAR,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
    )

    # Task 2: Insert a row
    task_insert = SnowflakeOperator(
        task_id="insert_run_log",
        sql="""
        INSERT INTO PI_FLOW.APP.RUN_LOG (run_date, batch_id, env)
        VALUES (%(run_date)s, %(batch_id)s, %(env)s);
        """
    )

    # Task 3: Select to verify
    task_select = SnowflakeOperator(
        task_id="select_run_log",
        sql="""
        SELECT * FROM PI_FLOW.APP.RUN_LOG
        WHERE batch_id = %(batch_id)s;
        """
    )

    task_create >> task_insert >> task_select

from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import SnowflakeOperator
from datetime import datetime


# -----------------------------
# Default Args (DAG-level connection)
# -----------------------------
default_args = {
    "snowflake_conn_id": "Dipak_Snowflake_Conn"   
}


with DAG(
    dag_id="test_multi_connection_dag",
    schedule_interval=None,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    default_args=default_args,
) as dag:

    # -----------------------------
    # Task 1 → Uses DAG-level connection
    # -----------------------------
    task_1 = SnowflakeOperator(
        task_id="task_1_default_conn",
        sql="""
        SELECT CURRENT_TIMESTAMP() AS ts;
        """
    )

    # -----------------------------
    # Task 2 → Uses DAG-level connection
    # -----------------------------
    task_2 = SnowflakeOperator(
        task_id="task_2_default_conn",
        sql="""
        SELECT CURRENT_USER() AS user;
        """
    )

    # -----------------------------
    # Task 3 → Override connection
    # -----------------------------
    task_3 = SnowflakeOperator(
        task_id="task_3_default_conn",
        sql="""
        SELECT CURRENT_ROLE() AS role;
        """
    )

    # -----------------------------
    # Task 4 → Back to default connection
    # -----------------------------
    task_4 = SnowflakeOperator(
        task_id="task_4_default_again",
        sql="""
        SELECT CURRENT_DATABASE() AS db;
        """
    )

    # -----------------------------
    # Dependencies
    # -----------------------------
    task_1 >> task_2 >> task_3 >> task_4

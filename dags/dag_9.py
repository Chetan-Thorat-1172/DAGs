from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def choose_conn_path(**context):
    run_id = context.get("run_id", "")
    return "conn_a_step_1" if (sum(ord(c) for c in run_id) % 2 == 0) else "conn_b_step_1"


def marker(name, **context):
    print(f"dag_9 {name}")


with DAG(
    dag_id="dag_9",
    schedule_interval="45 12 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 9 with 16 tasks and multi-connection branching",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="shared_prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_shared_prepare');")
    t03 = BranchPythonOperator(task_id="branch_connection", python_callable=choose_conn_path)

    t04 = SnowflakeOperator(task_id="conn_a_step_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_a_1');", snowflake_conn_id="chetan_conn")
    t05 = SnowflakeOperator(task_id="conn_a_step_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_a_2');", snowflake_conn_id="chetan_conn")
    t06 = SnowflakeOperator(task_id="conn_a_step_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_9_conn_a_3');", snowflake_conn_id="chetan_conn")
    t07 = SnowflakeOperator(task_id="conn_a_step_4", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_a_4');", snowflake_conn_id="chetan_conn")

    t08 = SnowflakeOperator(task_id="conn_b_step_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_b_1');", snowflake_conn_id="harsh_conn")
    t09 = SnowflakeOperator(task_id="conn_b_step_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_b_2');", snowflake_conn_id="harsh_conn")
    t10 = SnowflakeOperator(task_id="conn_b_step_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_9_conn_b_3');", snowflake_conn_id="harsh_conn")
    t11 = SnowflakeOperator(task_id="conn_b_step_4", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_conn_b_4');", snowflake_conn_id="harsh_conn")

    t12 = SnowflakeOperator(task_id="join_connections", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_join');", trigger_rule="none_failed_min_one_success")
    t13 = SnowflakeOperator(task_id="parallel_audit_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_parallel_audit_a');")
    t14 = SnowflakeOperator(task_id="parallel_audit_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_parallel_audit_b');")
    t15 = SnowflakeOperator(task_id="final_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_9_final_audit');", trigger_rule="all_done")
    t16 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t08]
    t04 >> t05 >> t06 >> t07 >> t12
    t08 >> t09 >> t10 >> t11 >> t12
    t12 >> [t13, t14] >> t15 >> t16


from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator, TriggerDagRunOperator


def choose_trigger_path(**context):
    run_id = context.get("run_id", "")
    return "post_trigger_path_a_1" if (sum(ord(c) for c in run_id) % 2 == 0) else "post_trigger_path_b_1"


def marker(name, **context):
    print(f"dag_8 {name}")


with DAG(
    dag_id="dag_8",
    schedule_interval="45 16 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 8 with 15 tasks using TriggerDagRunOperator and branch",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_prepare');")
    t03 = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        conf={"APPCODE": "LT_APP", "curation_database": "LT_DB", "restartind": "N"},
        wait_for_completion=False,
    )
    t04 = SnowflakeOperator(task_id="sp_after_trigger", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_after_trigger');")
    t05 = BranchPythonOperator(task_id="branch_post_trigger", python_callable=choose_trigger_path)

    t06 = SnowflakeOperator(task_id="post_trigger_path_a_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_path_a_1');")
    t07 = SnowflakeOperator(task_id="post_trigger_path_a_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_8_path_a_2');")
    t08 = SnowflakeOperator(task_id="post_trigger_path_a_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_path_a_3');")

    t09 = SnowflakeOperator(task_id="post_trigger_path_b_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_path_b_1');")
    t10 = SnowflakeOperator(task_id="post_trigger_path_b_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_8_path_b_2');")
    t11 = SnowflakeOperator(task_id="post_trigger_path_b_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_path_b_3');")

    t12 = SnowflakeOperator(task_id="join_path", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_join_path');", trigger_rule="none_failed_min_one_success")
    t13 = SnowflakeOperator(task_id="sp_publish", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_publish');")
    t14 = SnowflakeOperator(task_id="sp_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_8_audit');", trigger_rule="all_done")
    t15 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04 >> t05
    t05 >> [t06, t09]
    t06 >> t07 >> t08 >> t12
    t09 >> t10 >> t11 >> t12
    t12 >> t13 >> t14 >> t15


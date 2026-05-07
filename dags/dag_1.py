from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def choose_path(**context):
    run_id = context.get("run_id", "")
    return "fast_extract" if (sum(ord(c) for c in run_id) % 2 == 0) else "slow_extract"


def marker(name, **context):
    print(f"dag_1 marker: {name}")


with DAG(
    dag_id="dag_1",
    schedule_interval="45 17 * * *",
    start_date=datetime(2026, 5, 7),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 1 with 17 tasks and branch-heavy Snowflake flow",
) as dag:    
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="bootstrap", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_bootstrap');")
    t03 = BranchPythonOperator(task_id="branch_mode", python_callable=choose_path)

    t04 = SnowflakeOperator(task_id="fast_extract", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_fast_extract');")
    t05 = SnowflakeOperator(task_id="fast_validate", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_fast_validate');")
    t06 = SnowflakeOperator(task_id="fast_transform", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_1_fast_transform');")
    t07 = SnowflakeOperator(task_id="fast_load", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_fast_load');")
    t08 = SnowflakeOperator(task_id="fast_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_fast_audit');")

    t09 = SnowflakeOperator(task_id="slow_extract", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_slow_extract');")
    t10 = SnowflakeOperator(task_id="slow_profile", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_1_slow_profile');")
    t11 = SnowflakeOperator(task_id="slow_validate", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_slow_validate');")
    t12 = SnowflakeOperator(task_id="slow_transform", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_1_slow_transform');")
    t13 = SnowflakeOperator(task_id="slow_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_slow_audit');")

    t14 = SnowflakeOperator(task_id="join_branch", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_join');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="post_publish_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_post_publish_a');")
    t16 = SnowflakeOperator(task_id="post_publish_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_1_post_publish_b');")
    t17 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t09]
    t04 >> t05 >> t06 >> t07 >> t08 >> t14
    t09 >> t10 >> t11 >> t12 >> t13 >> t14
    t14 >> [t15, t16] >> t17

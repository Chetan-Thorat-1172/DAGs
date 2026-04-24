from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator, TriggerDagRunOperator


def choose_quality_path(**context):
    run_id = context.get("run_id", "")
    return "quality_extended_1" if (sum(ord(c) for c in run_id) % 2 == 0) else "quality_quick_1"


def marker(name, **context):
    print(f"dag_10 {name}")


with DAG(
    dag_id="dag_10",
    schedule_interval="45 12 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 10 with 20 tasks and combined branch/trigger fanout",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_prepare');")
    t03 = SnowflakeOperator(task_id="sp_ingest", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_ingest');")
    t04 = BranchPythonOperator(task_id="branch_quality", python_callable=choose_quality_path)

    t05 = SnowflakeOperator(task_id="quality_extended_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_extended_1');")
    t06 = SnowflakeOperator(task_id="quality_extended_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_10_quality_extended_2');")
    t07 = SnowflakeOperator(task_id="quality_extended_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_extended_3');")
    t08 = SnowflakeOperator(task_id="quality_extended_4", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_extended_4');")

    t09 = SnowflakeOperator(task_id="quality_quick_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_quick_1');")
    t10 = SnowflakeOperator(task_id="quality_quick_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_quick_2');")
    t11 = SnowflakeOperator(task_id="quality_quick_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_10_quality_quick_3');")
    t12 = SnowflakeOperator(task_id="quality_quick_4", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_quality_quick_4');")

    t13 = SnowflakeOperator(task_id="join_quality", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_join_quality');", trigger_rule="none_failed_min_one_success")
    t14 = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        conf={"APPCODE": "LT_APP_10", "curation_database": "LT_DB", "restartind": "N"},
        wait_for_completion=False,
    )
    t15 = SnowflakeOperator(task_id="post_trigger_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_post_trigger_1');")
    t16 = SnowflakeOperator(task_id="post_trigger_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_post_trigger_2');")
    t17 = SnowflakeOperator(task_id="parallel_publish_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_parallel_publish_a');")
    t18 = SnowflakeOperator(task_id="parallel_publish_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_parallel_publish_b');")
    t19 = SnowflakeOperator(task_id="final_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_10_final_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04
    t04 >> [t05, t09]
    t05 >> t06 >> t07 >> t08 >> t13
    t09 >> t10 >> t11 >> t12 >> t13
    t13 >> t14 >> t15 >> t16
    t16 >> [t17, t18] >> t19 >> t20


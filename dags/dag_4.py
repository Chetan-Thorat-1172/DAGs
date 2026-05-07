from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def capture_conf(**context):
    run_id = context.get("run_id", "")
    mode = "full" if (sum(ord(c) for c in run_id) % 2 == 0) else "delta"
    run_meta = {
        "run_date": "2026-04-01",
        "batch_id": "BATCH_DAG4_001",
        "mode": mode,
    }
    context["ti"].xcom_push(key="run_meta", value=run_meta)
    return run_meta


def choose_quality(**context):
    run_id = context.get("run_id", "")
    return "quality_full_1" if (sum(ord(c) for c in run_id) % 2 == 0) else "quality_delta_1"


def summarize(**context):
    ti = context["ti"]
    print(ti.xcom_pull(task_ids="capture_config", key="run_meta"))


with DAG(
    dag_id="dag_4",
    schedule_interval="30 10 * * *",  
    start_date=datetime(2026, 5, 7),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 4 with 18 tasks and param-driven quality branching",
) as dag:
    t01 = PythonOperator(task_id="capture_config", python_callable=capture_conf)
    t02 = SnowflakeOperator(task_id="sp_prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_prepare');")
    t03 = SnowflakeOperator(task_id="sp_extract", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_extract');")
    t04 = SnowflakeOperator(task_id="sp_transform", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_4_transform');")
    t05 = BranchPythonOperator(task_id="branch_quality_mode", python_callable=choose_quality)

    t06 = SnowflakeOperator(task_id="quality_full_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_quality_full_1');")
    t07 = SnowflakeOperator(task_id="quality_full_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_quality_full_2');")
    t08 = SnowflakeOperator(task_id="quality_full_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_4_quality_full_3');")

    t09 = SnowflakeOperator(task_id="quality_delta_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_quality_delta_1');")
    t10 = SnowflakeOperator(task_id="quality_delta_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_quality_delta_2');")
    t11 = SnowflakeOperator(task_id="quality_delta_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_4_quality_delta_3');")

    t12 = SnowflakeOperator(task_id="join_quality", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_join_quality');", trigger_rule="none_failed_min_one_success")
    t13 = SnowflakeOperator(task_id="sp_enrich_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_enrich_a');")
    t14 = SnowflakeOperator(task_id="sp_enrich_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_enrich_b');")
    t15 = SnowflakeOperator(task_id="sp_publish_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_publish_a');")
    t16 = SnowflakeOperator(task_id="sp_publish_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_publish_b');")
    t17 = SnowflakeOperator(task_id="sp_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_4_audit');", trigger_rule="all_done")
    t18 = PythonOperator(task_id="summarize", python_callable=summarize, trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04 >> t05
    t05 >> [t06, t09]
    t06 >> t07 >> t08 >> t12
    t09 >> t10 >> t11 >> t12
    t12 >> [t13, t14]
    t13 >> t15
    t14 >> t16
    [t15, t16] >> t17 >> t18


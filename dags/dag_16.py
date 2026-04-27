from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_16 {name}")


with DAG(
    dag_id="dag_16",
    schedule_interval="40 17 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 16 with 20 tasks and multi-lane parallelism",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="init_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_init_1');")
    t03 = SnowflakeOperator(task_id="init_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_init_2');")

    t04 = SnowflakeOperator(task_id="lane_1_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_1_a');")
    t05 = SnowflakeOperator(task_id="lane_1_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_1_b');")

    t06 = SnowflakeOperator(task_id="lane_2_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_16_lane_2_a');")
    t07 = SnowflakeOperator(task_id="lane_2_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_2_b');")

    t08 = SnowflakeOperator(task_id="lane_3_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="lane_3_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_3_post');")

    t10 = SnowflakeOperator(task_id="lane_4_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="lane_4_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_4_post');")

    t12 = SnowflakeOperator(task_id="lane_5_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_lane_5_a');")
    t13 = SnowflakeOperator(task_id="lane_5_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_16_lane_5_b');")

    t14 = SnowflakeOperator(task_id="join_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_join_1');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="post_fanout_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_post_fanout_1');")
    t16 = SnowflakeOperator(task_id="post_fanout_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_post_fanout_2');")
    t17 = SnowflakeOperator(task_id="post_fanout_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_post_fanout_3');")
    t18 = SnowflakeOperator(task_id="join_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_join_2');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_16_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t06, t08, t10, t12]
    t04 >> t05 >> t14
    t06 >> t07 >> t14
    t08 >> t09 >> t14
    t10 >> t11 >> t14
    t12 >> t13 >> t14
    t14 >> [t15, t16, t17]
    [t15, t16, t17] >> t18 >> t19 >> t20

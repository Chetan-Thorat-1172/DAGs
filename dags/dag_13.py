from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_13 {name}")


with DAG(
    dag_id="dag_13",
    schedule_interval="40 17 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 13 with 20 tasks and dense parallel sections",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="setup_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_setup_1');")
    t03 = SnowflakeOperator(task_id="setup_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_setup_2');")
    t04 = SnowflakeOperator(task_id="setup_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_setup_3');")

    t05 = SnowflakeOperator(task_id="lane_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_lane_a');")
    t06 = SnowflakeOperator(task_id="lane_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_13_lane_b');")
    t07 = SnowflakeOperator(task_id="lane_c", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_lane_c');")
    t08 = SnowflakeOperator(task_id="lane_d", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_13_lane_d');")
    t09 = SnowflakeOperator(task_id="lane_e", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_lane_e');")

    t10 = SnowflakeOperator(task_id="lane_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t11 = SnowflakeOperator(task_id="lane_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")

    t12 = SnowflakeOperator(task_id="post_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_post_a');")
    t13 = SnowflakeOperator(task_id="post_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_post_b');")
    t14 = SnowflakeOperator(task_id="post_c", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_post_c');")
    t15 = SnowflakeOperator(task_id="post_d", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_13_post_d');")
    t16 = SnowflakeOperator(task_id="post_e", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_post_e');")

    t17 = SnowflakeOperator(task_id="join_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_join_1');", trigger_rule="none_failed_min_one_success")
    t18 = SnowflakeOperator(task_id="join_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_join_2');")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_13_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="end", python_callable=lambda **c: marker("end", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04
    t04 >> [t05, t06, t07, t08, t09, t10, t11]
    t05 >> t12 >> t17
    t06 >> t13 >> t17
    t07 >> t14 >> t17
    t08 >> t15 >> t17
    t09 >> t16 >> t17
    t10 >> t17
    t11 >> t17
    t17 >> t18 >> t19 >> t20

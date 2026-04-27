from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_15 {name}")


with DAG(
    dag_id="dag_15",
    schedule_interval="40 17 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 15 with 20 tasks, parallel-first design",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="seed_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_seed_1');")
    t03 = SnowflakeOperator(task_id="seed_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_seed_2');")
    t04 = SnowflakeOperator(task_id="seed_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_seed_3');")

    t05 = SnowflakeOperator(task_id="lane_a_sp", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_a_sp');")
    t06 = SnowflakeOperator(task_id="lane_b_sp", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_15_lane_b_sp');")
    t07 = SnowflakeOperator(task_id="lane_c_sp", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_c_sp');")
    t08 = SnowflakeOperator(task_id="lane_d_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="lane_e_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")

    t10 = SnowflakeOperator(task_id="lane_a_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_a_post');")
    t11 = SnowflakeOperator(task_id="lane_b_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_b_post');")
    t12 = SnowflakeOperator(task_id="lane_c_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_c_post');")
    t13 = SnowflakeOperator(task_id="lane_d_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_d_post');")
    t14 = SnowflakeOperator(task_id="lane_e_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_lane_e_post');")

    t15 = SnowflakeOperator(task_id="join_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_join_1');", trigger_rule="none_failed_min_one_success")
    t16 = SnowflakeOperator(task_id="post_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_post_1');")
    t17 = SnowflakeOperator(task_id="post_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_15_post_2');")
    t18 = SnowflakeOperator(task_id="join_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_join_2');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_15_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="end", python_callable=lambda **c: marker("end", **c), trigger_rule="all_done")

    t01 >> t02 >> t03 >> t04
    t04 >> [t05, t06, t07, t08, t09]
    t05 >> t10 >> t15
    t06 >> t11 >> t15
    t07 >> t12 >> t15
    t08 >> t13 >> t15
    t09 >> t14 >> t15
    t15 >> [t16, t17]
    [t16, t17] >> t18 >> t19 >> t20

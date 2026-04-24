from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_11 {name}") 

with DAG(
    dag_id="dag_11",
    schedule_interval="30 17 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 11 with 20 tasks, mostly parallel with mixed assets",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_bootstrap", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_bootstrap');")
    t03 = SnowflakeOperator(task_id="sp_prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_prepare');")

    t04 = SnowflakeOperator(task_id="lane_a_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_a_1');")
    t05 = SnowflakeOperator(task_id="lane_a_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_11_lane_a_2');")

    t06 = SnowflakeOperator(task_id="lane_b_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_b_1');")
    t07 = SnowflakeOperator(task_id="lane_b_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_11_lane_b_2');")

    t08 = SnowflakeOperator(task_id="lane_c_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="lane_c_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_c_post');")

    t10 = SnowflakeOperator(task_id="lane_d_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="lane_d_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_d_post');")

    t12 = SnowflakeOperator(task_id="lane_e_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_e_1');")
    t13 = SnowflakeOperator(task_id="lane_e_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_lane_e_2');")

    t14 = SnowflakeOperator(task_id="join_main", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_join_main');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="post_join_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_post_join_a');")
    t16 = SnowflakeOperator(task_id="post_join_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_11_post_join_b');")
    t17 = SnowflakeOperator(task_id="post_join_c", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_post_join_c');")
    t18 = SnowflakeOperator(task_id="final_merge", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_final_merge');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_11_audit');", trigger_rule="all_done")
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

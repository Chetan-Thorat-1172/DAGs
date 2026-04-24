from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_18 {name}")


with DAG(
    dag_id="dag_18",
    schedule_interval="15 16 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 18 with 20 tasks and parallel dominance",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="init_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_init_a');")
    t03 = SnowflakeOperator(task_id="init_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_init_b');")

    t04 = SnowflakeOperator(task_id="lane1_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane1_a');")
    t05 = SnowflakeOperator(task_id="lane1_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_18_lane1_b');")

    t06 = SnowflakeOperator(task_id="lane2_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane2_a');")
    t07 = SnowflakeOperator(task_id="lane2_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane2_b');")

    t08 = SnowflakeOperator(task_id="lane3_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="lane3_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane3_post');")

    t10 = SnowflakeOperator(task_id="lane4_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="lane4_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane4_post');")

    t12 = SnowflakeOperator(task_id="lane5_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_lane5_a');")
    t13 = SnowflakeOperator(task_id="lane5_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_18_lane5_b');")

    t14 = SnowflakeOperator(task_id="join_main", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_join_main');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="post_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_post_1');")
    t16 = SnowflakeOperator(task_id="post_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_post_2');")
    t17 = SnowflakeOperator(task_id="post_3", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_18_post_3');")
    t18 = SnowflakeOperator(task_id="join_final", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_join_final');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_18_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="end", python_callable=lambda **c: marker("end", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t06, t08, t10, t12]
    t04 >> t05 >> t14
    t06 >> t07 >> t14
    t08 >> t09 >> t14
    t10 >> t11 >> t14
    t12 >> t13 >> t14
    t14 >> [t15, t16, t17]
    [t15, t16, t17] >> t18 >> t19 >> t20

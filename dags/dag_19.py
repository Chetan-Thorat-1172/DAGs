from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_19 {name}")


with DAG(
    dag_id="dag_19",
    schedule_interval="30 17 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 19 with 20 tasks and wide fanout",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="prepare_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_prepare_a');")
    t03 = SnowflakeOperator(task_id="prepare_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_prepare_b');")

    t04 = SnowflakeOperator(task_id="a1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_a1');")
    t05 = SnowflakeOperator(task_id="a2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_a2');")

    t06 = SnowflakeOperator(task_id="b1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_b1');")
    t07 = SnowflakeOperator(task_id="b2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_19_b2');")

    t08 = SnowflakeOperator(task_id="c1_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t09 = SnowflakeOperator(task_id="c2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_c2');")

    t10 = SnowflakeOperator(task_id="d1_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="d2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_d2');")

    t12 = SnowflakeOperator(task_id="e1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_e1');")
    t13 = SnowflakeOperator(task_id="e2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_19_e2');")

    t14 = SnowflakeOperator(task_id="join_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_join_1');", trigger_rule="none_failed_min_one_success")
    t15 = SnowflakeOperator(task_id="post_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_post_a');")
    t16 = SnowflakeOperator(task_id="post_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_post_b');")
    t17 = SnowflakeOperator(task_id="post_c", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_post_c');")
    t18 = SnowflakeOperator(task_id="join_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_join_2');", trigger_rule="none_failed_min_one_success")
    t19 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_19_audit');", trigger_rule="all_done")
    t20 = PythonOperator(task_id="finish", python_callable=lambda **c: marker("finish", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t06, t08, t10, t12]
    t04 >> t05 >> t14
    t06 >> t07 >> t14
    t08 >> t09 >> t14
    t10 >> t11 >> t14
    t12 >> t13 >> t14
    t14 >> [t15, t16, t17]
    [t15, t16, t17] >> t18 >> t19 >> t20

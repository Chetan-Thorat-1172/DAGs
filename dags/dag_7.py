from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, SnowflakeOperator


def marker(name, **context):
    print(f"dag_7 {name}")


with DAG(
    dag_id="dag_7",
    schedule_interval="35 16 * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 7 with 17 tasks and triple parallel branches",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="sp_bootstrap", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_bootstrap');")
    t03 = SnowflakeOperator(task_id="sp_ingest", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_ingest');")

    t04 = SnowflakeOperator(task_id="run_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t05 = SnowflakeOperator(task_id="run_dbt_models", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t06 = SnowflakeOperator(task_id="run_sp_chain_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_sp_chain_1');")

    t07 = SnowflakeOperator(task_id="nb_post_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_nb_post_1');")
    t08 = SnowflakeOperator(task_id="nb_post_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_7_nb_post_2');")

    t09 = SnowflakeOperator(task_id="dbt_post_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_dbt_post_1');")
    t10 = SnowflakeOperator(task_id="dbt_post_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_7_dbt_post_2');")

    t11 = SnowflakeOperator(task_id="sp_post_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_sp_post_1');")
    t12 = SnowflakeOperator(task_id="sp_post_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_sp_post_2');")

    t13 = SnowflakeOperator(task_id="join_all", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_join_all');", trigger_rule="none_failed_min_one_success")
    t14 = SnowflakeOperator(task_id="publish_1", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_publish_1');")
    t15 = SnowflakeOperator(task_id="publish_2", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_publish_2');")
    t16 = SnowflakeOperator(task_id="audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_7_audit');", trigger_rule="all_done")
    t17 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t05, t06]
    t04 >> t07 >> t08 >> t13
    t05 >> t09 >> t10 >> t13
    t06 >> t11 >> t12 >> t13
    t13 >> [t14, t15] >> t16 >> t17



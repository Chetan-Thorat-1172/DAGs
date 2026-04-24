from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def choose_path(**context):
    run_id = context.get("run_id", "")
    return "full_extract" if (sum(ord(c) for c in run_id) % 2 == 0) else "inc_extract"


def marker(name, **context):
    print(f"dag_2 {name}")


with DAG(
    dag_id="dag_2",
    schedule_interval="30 17 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 2 with 16 tasks, branch and notebook/dbt split",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="prepare", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_prepare');")
    t03 = BranchPythonOperator(task_id="branch_load_type", python_callable=choose_path)

    t04 = SnowflakeOperator(task_id="full_extract", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_full_extract');")
    t05 = SnowflakeOperator(task_id="full_validate", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_full_validate');")
    t06 = SnowflakeOperator(task_id="full_notebook", sql="""EXECUTE NOTEBOOK PROJECT TESTING.PI_FLOW_LOAD_TEST.NB_LT_PROJECT
MAIN_FILE = 'NB_LT_PROJECT.ipynb'
COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU'
QUERY_WAREHOUSE = 'COMPUTE_WH'
RUNTIME = 'V2.2-CPU-PY3.10'""")
    t07 = SnowflakeOperator(task_id="full_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_full_post');")

    t08 = SnowflakeOperator(task_id="inc_extract", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_inc_extract');")
    t09 = SnowflakeOperator(task_id="inc_validate", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_inc_validate');")
    t10 = SnowflakeOperator(task_id="inc_dbt", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="inc_post", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_inc_post');")

    t12 = SnowflakeOperator(task_id="join_paths", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_join_paths');", trigger_rule="none_failed_min_one_success")
    t13 = SnowflakeOperator(task_id="parallel_metric_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_parallel_metric_a');")
    t14 = SnowflakeOperator(task_id="parallel_metric_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_2_parallel_metric_b');")
    t15 = SnowflakeOperator(task_id="publish", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_2_publish');")
    t16 = PythonOperator(task_id="finalize", python_callable=lambda **c: marker("finalize", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t08]
    t04 >> t05 >> t06 >> t07 >> t12
    t08 >> t09 >> t10 >> t11 >> t12
    t12 >> [t13, t14] >> t15 >> t16



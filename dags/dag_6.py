from datetime import datetime
from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator, SnowflakeOperator


def choose_dbt_branch(**context):
    run_id = context.get("run_id", "")
    return "dbt_branch_a_seed" if (sum(ord(c) for c in run_id) % 2 == 0) else "dbt_branch_b_seed"


def marker(name, **context):
    print(f"dag_6 {name}")


with DAG(
    dag_id="dag_6",
    schedule_interval="20 15 * * *",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    default_args={"snowflake_conn_id": "harsh_conn", "retries": 1, "retry_delay_seconds": 5},
    description="Load test DAG 6 with 16 tasks and DBT branching",
) as dag:
    t01 = PythonOperator(task_id="start", python_callable=lambda **c: marker("start", **c))
    t02 = SnowflakeOperator(task_id="prepare_inputs", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_6_prepare_inputs');")
    t03 = BranchPythonOperator(task_id="branch_dbt_mode", python_callable=choose_dbt_branch)

    t04 = SnowflakeOperator(task_id="dbt_branch_a_seed", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t05 = SnowflakeOperator(task_id="dbt_branch_a_staging", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t06 = SnowflakeOperator(task_id="dbt_branch_a_mart", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t07 = SnowflakeOperator(task_id="dbt_branch_a_test", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")

    t08 = SnowflakeOperator(task_id="dbt_branch_b_seed", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t09 = SnowflakeOperator(task_id="dbt_branch_b_staging", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t10 = SnowflakeOperator(task_id="dbt_branch_b_mart", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")
    t11 = SnowflakeOperator(task_id="dbt_branch_b_test", sql="EXECUTE DBT PROJECT TESTING.PI_FLOW_LOAD_TEST.DBT_LT_PROJECT ARGS = 'run';")

    t12 = SnowflakeOperator(task_id="join_dbt", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_6_join_dbt');", trigger_rule="none_failed_min_one_success")
    t13 = SnowflakeOperator(task_id="sp_publish_a", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_6_publish_a');")
    t14 = SnowflakeOperator(task_id="sp_publish_b", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_3S('dag_6_publish_b');")
    t15 = SnowflakeOperator(task_id="sp_audit", sql="CALL TESTING.PI_FLOW_LOAD_TEST.SP_LT_SLEEP_2S('dag_6_audit');", trigger_rule="all_done")
    t16 = PythonOperator(task_id="end", python_callable=lambda **c: marker("end", **c), trigger_rule="all_done")

    t01 >> t02 >> t03
    t03 >> [t04, t08]
    t04 >> t05 >> t06 >> t07 >> t12
    t08 >> t09 >> t10 >> t11 >> t12
    t12 >> [t13, t14] >> t15 >> t16



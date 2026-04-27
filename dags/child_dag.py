"""
Child DAG — Triggered by parent_trigger_dag via TriggerDagRunOperator.
pu
This DAG:
1. Reads the conf passed from the parent DAG
2. Processes data based on the conf parameters  
3. Outputs a summarydod   

It is designed to be triggered programmatically (schedule_interval=None)
and demonstrates cross-DAG communication via dag_run.conf..
"""
#commentadss
#making changes
from datetime import datetime
from dag_parser.dynamic.dag_context import PythonOperator
from dag_parser.dynamic.dag_context import DAG


def read_conf(**context):
    """Read and display the configuration passed from the parent DAG."""
    dag_run = context.get("dag_run", {})
    conf = dag_run.get("conf", {})

    print(f"=== Child DAG Received Configuration ===")
    print(f"  APPCODE:            {conf.get('APPCODE', 'N/A')}")
    print(f"  curation_database:  {conf.get('curation_database', 'N/A')}")
    print(f"  restartind:         {conf.get('restartind', 'N/A')}")
    print(f"  Full conf: {conf}") 

    return conf


def process_data(**context):
    """Simulate data processing using conf from the parent."""
    ti = context["ti"]
    conf = ti.xcom_pull(task_ids="read_conf")

    appcode = conf.get("APPCODE", "UNKNOWN") if conf else "UNKNOWN"
    db = conf.get("curation_database", "DEFAULT_DB") if conf else "DEFAULT_DB"

    print(f"Processing data for APPCODE={appcode} in database={db}")
    result = {"rows_processed": 1500, "appcode": appcode, "database": db}
    print(f"Processing complete: {result}")
    return result


def finalize(**context):
    """Final task — summarize child DAG execution."""
    ti = context["ti"]
    process_result = ti.xcom_pull(task_ids="process_data")
    print(f"=== Child DAG Finalization ===")
    print(f"  Processing result: {process_result}")
    print(f"  Child DAG completed successfully.")
    return "child_dag_complete"


with DAG(
    dag_id="child_dag",
    schedule_interval=None,
    start_date=datetime(2026, 4, 24),
    catchup=False,
    description="Child DAG triggered by parent via TriggerDagRunOperator",
) as dag:

    t_read_conf = PythonOperator(
        task_id="read_conf",
        python_callable=read_conf,
    )

    t_process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    t_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize,
    )

    t_read_conf >> t_process_data >> t_finalize

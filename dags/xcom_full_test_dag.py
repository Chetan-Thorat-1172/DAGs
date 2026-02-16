from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator
from dag_parser.dynamic.params import Param
from datetime import datetime


# ----------------------------------------------------
# Task 1: Extract
# - Returns dictionary (auto XCom push expected)
# ----------------------------------------------------
def extract_data(**context):
    print(" Extracting data...") 

    data = {
        "rows": 120,
        "source": "customer_table",
        "batch_id": 101
    }

    print(f"Extracted: {data}")

    # Auto XCom push expected (key="return_value")
    return data


# ----------------------------------------------------
# Task 2: Transform
# - Pulls from extract via xcom_pull
# - Pushes explicit XCom
# ----------------------------------------------------
def transform_data(**context):
    ti = context["ti"]

    print(" Transforming data...")

    extracted = ti.xcom_pull(task_ids="extract_task")

    print(f"Pulled from XCom (extract_task): {extracted}")

    transformed = {
        "rows": extracted["rows"],
        "status": "transformed",
        "batch_id": extracted["batch_id"]
    }

    print(f"Transformed result: {transformed}")

    # Explicit push with custom key
    ti.xcom_push(key="transform_metadata", value=transformed)

    # Also auto-push via return_value
    return transformed


# ----------------------------------------------------
# Task 3: Validate
# - Pulls specific XCom key
# ----------------------------------------------------
def validate_data(**context):
    ti = context["ti"]

    print(" Validating data...")

    transform_meta = ti.xcom_pull(
        task_ids="transform_task",
        key="transform_metadata"
    )

    print(f"Pulled transform_metadata: {transform_meta}")

    if transform_meta["rows"] > 0:
        print(" Validation successful")
        return {"validated": True}
    else:
        raise ValueError("No rows found")


# ----------------------------------------------------
# Task 4: Final Summary
# - Pulls multiple upstream results
# ----------------------------------------------------
def finalize_pipeline(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]

    print(" Finalizing pipeline...")

    # Pull default return_value
    extract_result = ti.xcom_pull(task_ids="extract_task")
    transform_result = ti.xcom_pull(task_ids="transform_task")
    validate_result = ti.xcom_pull(task_ids="validate_task")

    print("----- SUMMARY -----")
    print(f"Extract  : {extract_result}")
    print(f"Transform: {transform_result}")
    print(f"Validate : {validate_result}")
    print("-------------------")

    print("DAG Run Params:")
    print(dag_run.conf)

    return "Pipeline completed successfully!"


# ----------------------------------------------------
# DAG Definition
# ----------------------------------------------------
with DAG(
    dag_id="xcom_full_test_dag",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    params={
        "test_mode": Param(
            type="boolean",
            default=True,
            description="Just for testing param rendering"
        )
    },
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate_data,
    )

    finalize_task = PythonOperator(
        task_id="finalize_task",
        python_callable=finalize_pipeline,
    )

    # DAG Dependencies
    extract_task >> transform_task >> validate_task >> finalize_task

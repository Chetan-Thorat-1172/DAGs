"""
Simple Comprehensive Pi-Flow Demo DAG

This DAG demonstrates all major Pi-Flow capabilities:
1. DAG Parameters & Runtime Configuration
2. Branching Logic (BranchPythonOperator)
3. Task Retries & Retry Logic
4. Default Args Inheritance
5. XCom Inter-task Communication
6. Task Trigger Rules
7. Error Handling & Flaky Task Recovery
8. Parallel Task Execution (fan-out/fan-in)
9. Multi-worker Distribution
10. DAG Run Context Access

Execution Flow:
    START -> READ_PARAMS -> VALIDATE_INPUT -> BRANCH_DECISION
             -> FULL_LOAD_PATH OR INCREMENTAL_PATH
             -> PARALLEL_BATCH_PROCESSING
             -> RETRY_QUALITY_CHECK
             -> JOIN_BRANCHES
             -> FINALIZE -> SUCCESS
"""

from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator
from dag_parser.dynamic.params import Param
from datetime import datetime
import socket
import os
import time


# SECTION 1: PARAMETER & CONTEXT READING
def read_and_validate_params(**context):
    """
    Read runtime parameters from DAG_RUN.CONF
    Demonstrates: DAG parameter access, XCom push
    """
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    
    print("\n" + "=" * 70)
    print("TASK: read_and_validate_params")
    print("=" * 70)
    print("  run_date    : " + str(params.get('run_date', 'N/A')))
    print("  customer_id : " + str(params.get('customer_id', 'N/A')))
    print("  environment : " + str(params.get('environment', 'N/A')))
    print("  full_load   : " + str(params.get('full_load', False)))
    print("=" * 70 + "\n")
    
    return params


# SECTION 2: VALIDATION
def validate_input(**context):
    """
    Basic input validation task
    Demonstrates: Task execution context, XCom pull
    """
    ti = context["ti"]
    params = ti.xcom_pull(task_ids="read_params")
    
    print("\n" + "=" * 70)
    print("TASK: validate_input")
    print("=" * 70)
    print("  Received params via XCom: " + str(params))
    print("  Validation passed!")
    print("=" * 70 + "\n")
    
    return True


# SECTION 3: BRANCHING LOGIC
def decide_load_strategy(**context):
    """
    Branch decision based on full_load parameter
    Demonstrates: BranchPythonOperator logic, parameter access
    
    Returns: "full_load_branch" or "incremental_branch"
    """
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    full_load = params.get("full_load", False)
    
    print("\n" + "=" * 70)
    print("TASK: decide_load_strategy (BRANCHING LOGIC)")
    print("=" * 70)
    print("  full_load parameter: " + str(full_load))
    
    if full_load:
        print("  Decision: FULL_LOAD_BRANCH")
        print("=" * 70 + "\n")
        return "full_load_branch"
    else:
        print("  Decision: INCREMENTAL_BRANCH")
        print("=" * 70 + "\n")
        return "incremental_branch"


# SECTION 4A: FULL LOAD BRANCH
def extract_full_data(**context):
    """Extract task for FULL LOAD branch"""
    print("\n" + "=" * 70)
    print("TASK: extract_full_data")
    print("=" * 70)
    print("  Extracting full dataset from source...")
    print("  Rows extracted: 50000")
    print("=" * 70 + "\n")
    
    return {"rows": 50000, "load_type": "full", "source": "primary_db"}


def transform_full_data(**context):
    """Transform task for FULL LOAD branch"""
    ti = context["ti"]
    
    print("\n" + "=" * 70)
    print("TASK: transform_full_data")
    print("=" * 70)
    print("  Pulling extracted data via XCom...")
    
    extracted = ti.xcom_pull(task_ids="extract_full_data")
    print("  Received: " + str(extracted))
    print("  Applying transformations...")
    print("  Rows transformed: 48975 (removed duplicates)")
    print("=" * 70 + "\n")
    
    transformed = {
        "rows": 48975,
        "load_type": "full",
        "transformations_applied": ["deduplicate", "validate_schema"]
    }
    ti.xcom_push(key="full_transform_data", value=transformed)
    return transformed


# SECTION 4B: INCREMENTAL LOAD BRANCH
def extract_incremental_data(**context):
    """Extract task for INCREMENTAL branch"""
    print("\n" + "=" * 70)
    print("TASK: extract_incremental_data")
    print("=" * 70)
    print("  Extracting incremental dataset...")
    print("  Rows extracted: 1250")
    print("=" * 70 + "\n")
    
    return {"rows": 1250, "load_type": "incremental", "source": "primary_db"}


def transform_incremental_data(**context):
    """Transform task for INCREMENTAL branch"""
    ti = context["ti"]
    
    print("\n" + "=" * 70)
    print("TASK: transform_incremental_data")
    print("=" * 70)
    print("  Pulling extracted data via XCom...")
    
    extracted = ti.xcom_pull(task_ids="extract_incremental_data")
    print("  Received: " + str(extracted))
    print("  Applying transformations...")
    print("  Rows transformed: 1250 (no duplicates)")
    print("=" * 70 + "\n")
    
    transformed = {
        "rows": 1250,
        "load_type": "incremental",
        "transformations_applied": ["validate_schema"]
    }
    ti.xcom_push(key="incremental_transform_data", value=transformed)
    return transformed


# SECTION 5: PARALLEL BATCH PROCESSING
def create_parallel_task(batch_num):
    """
    Generic task callable for parallel execution
    Demonstrates: Multi-worker distribution, hostname/PID tracking
    """
    def task_logic(**context):
        ti = context["ti"]
        hostname = socket.gethostname()
        pid = os.getpid()
        
        print("\n" + "-" * 70)
        print("TASK: parallel_batch_" + str(batch_num))
        print("-" * 70)
        print("  Executed on host: " + hostname)
        print("  Process PID: " + str(pid))
        print("  Task ID: " + str(ti.task_id))
        print("  Batch " + str(batch_num) + " processing... sleeping 3 seconds")
        print("-" * 70 + "\n")
        
        time.sleep(3)
        
        return {
            "batch": batch_num,
            "host": hostname,
            "pid": pid,
            "status": "completed"
        }
    
    return task_logic


# SECTION 6: RETRY HANDLING (FLAKY TASK)
def flaky_quality_check(**context):
    """
    Task that fails initially and succeeds after retries
    Demonstrates: Retry logic, try_number tracking, exception handling
    """
    ti = context["ti"]
    try_number = ti.try_number
    
    print("\n" + "=" * 70)
    print("TASK: flaky_quality_check (Attempt " + str(try_number) + ")")
    print("=" * 70)
    
    # Fail on first 2 attempts, succeed on 3rd
    if try_number < 3:
        print("  FAILED: Quality check failed (simulated)")
        print("  Retrying in 3 seconds... (Attempt " + str(try_number + 1) + ")")
        print("=" * 70 + "\n")
        raise Exception("Quality check failed at attempt " + str(try_number))
    else:
        print("  SUCCESS: Quality check passed after " + str(try_number - 1) + " retries!")
        print("  Data quality score: 98.5%")
        print("=" * 70 + "\n")
        
        return {
            "quality_score": 98.5,
            "passed_after_attempts": try_number,
            "status": "success"
        }


# SECTION 7: FAN-IN / BRANCH REJOINING
def join_branches(**context):
    """
    Task that runs after both branches complete
    Demonstrates: Fan-in pattern, trigger_rule: none_failed_min_one_success
    """
    ti = context["ti"]
    
    print("\n" + "=" * 70)
    print("TASK: join_branches (FAN-IN)")
    print("=" * 70)
    print("  Both load branches completed!")
    print("  Merging results...")
    
    full_data = None
    incremental_data = None
    
    try:
        full_data = ti.xcom_pull(task_ids="transform_full_data")
    except:
        pass
    
    try:
        incremental_data = ti.xcom_pull(task_ids="transform_incremental_data")
    except:
        pass
    
    if full_data:
        print("  Full Load Data: " + str(full_data['rows']) + " rows")
    if incremental_data:
        print("  Incremental Data: " + str(incremental_data['rows']) + " rows")
    
    print("=" * 70 + "\n")
    
    return {
        "merge_status": "success",
        "total_batches_processed": 5,
        "timestamp": str(datetime.now())
    }


# SECTION 8: FINALIZATION & SUMMARY
def finalize_pipeline(**context):
    """
    Final task: summarize entire DAG run
    Demonstrates: Pulling multiple upstream XCom values, DAG context access
    """
    ti = context["ti"]
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    
    print("\n" + "=" * 70)
    print("TASK: finalize_pipeline")
    print("=" * 70)
    print("\n  FINAL SUMMARY:")
    print("    DAG Run ID: " + str(dag_run.get('run_id', 'N/A')))
    print("    Run Date: " + str(params.get('run_date', 'N/A')))
    print("    Customer ID: " + str(params.get('customer_id', 'N/A')))
    print("    Environment: " + str(params.get('environment', 'N/A')))
    load_type = "FULL" if params.get('full_load') else "INCREMENTAL"
    print("    Load Type: " + load_type)
    print("\n  All tasks completed successfully!")
    print("  Data pipeline execution finished at: " + str(datetime.now()))
    print("=" * 70 + "\n")
    
    return {
        "pipeline_status": "completed",
        "timestamp": str(datetime.now())
    }


# DAG DEFINITION
default_args = {
    "retries": 1,
    "retry_delay_seconds": 60,
}

with DAG(
    dag_id="simple_comprehensive_demo_dag",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    description="Simple comprehensive Pi-Flow demo covering all major features",
    
    # Runtime Parameters
    params={
        "run_date": Param(
            type="string",
            required=True,
            description="Business date (YYYY-MM-DD)"
        ),
        "customer_id": Param(
            type="integer",
            default=12345,
            description="Customer identifier"
        ),
        "environment": Param(
            type="string",
            default="DEV",
            description="Execution environment (DEV/UAT/PROD)"
        ),
        "full_load": Param(
            type="boolean",
            default=False,
            description="Execute full load (true) or incremental (false)"
        ),
    },
    
    # Default Arguments inherited by tasks
    default_args=default_args,
    
) as dag:
    
    # PHASE 1: INITIALIZATION
    read_params = PythonOperator(
        task_id="read_params",
        python_callable=read_and_validate_params,
    )
    
    validate_input_task = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
    )
    
    # PHASE 2: BRANCH DECISION
    decide_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_load_strategy,
    )
    
    # PHASE 3A: FULL LOAD BRANCH
    extract_full_data_task = PythonOperator(
        task_id="extract_full_data",
        python_callable=extract_full_data,
    )
    
    transform_full_data_task = PythonOperator(
        task_id="transform_full_data",
        python_callable=transform_full_data,
    )
    
    # PHASE 3B: INCREMENTAL LOAD BRANCH
    extract_incremental_data_task = PythonOperator(
        task_id="extract_incremental_data",
        python_callable=extract_incremental_data,
    )
    
    transform_incremental_data_task = PythonOperator(
        task_id="transform_incremental_data",
        python_callable=transform_incremental_data,
    )
    
    # PHASE 4: PARALLEL BATCH PROCESSING (Fan-Out)
    parallel_batch_tasks = [
        PythonOperator(
            task_id="parallel_batch_" + str(i),
            python_callable=create_parallel_task(i),
            trigger_rule="none_failed",
        )
        for i in range(1, 6)
    ]
    
    # PHASE 5: RETRY HANDLING (FLAKY TASK)
    flaky_quality_check_task = PythonOperator(
        task_id="flaky_quality_check",
        python_callable=flaky_quality_check,
        retries=2,
        retry_delay_seconds=3,
    )
    
    # PHASE 6: FAN-IN (Branch Rejoining)
    join_branches_task = PythonOperator(
        task_id="join_branches",
        python_callable=join_branches,
        trigger_rule="none_failed_min_one_success",
    )
    
    # PHASE 7: FINALIZATION
    finalize_task = PythonOperator(
        task_id="finalize_pipeline",
        python_callable=finalize_pipeline,
        trigger_rule="all_success",
    )
    
    # DEPENDENCIES / WORKFLOW ORCHESTRATION
    read_params >> validate_input_task >> decide_branch
    
    decide_branch >> [extract_full_data_task, extract_incremental_data_task]
    
    extract_full_data_task >> transform_full_data_task
    extract_incremental_data_task >> transform_incremental_data_task
    
    # Fan-out: Both transform tasks to all parallel batch tasks
    for batch_task in parallel_batch_tasks:
        transform_full_data_task >> batch_task
        transform_incremental_data_task >> batch_task
    
    # All parallel tasks to quality check
    for batch_task in parallel_batch_tasks:
        batch_task >> flaky_quality_check_task
    
    flaky_quality_check_task >> join_branches_task
    
    join_branches_task >> finalize_task

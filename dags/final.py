from dag_parser.dynamic.dag_context import DAG, PythonOperator, BranchPythonOperator
from dag_parser.dynamic.params import Param
from datetime import datetime
import socket
import os
import time

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: PARAMETER & CONTEXT READINGs
# ═══════════════════════════════════════════════════════════════════════════════

def read_and_validate_params(**context):
    """
    Task 1: Read runtime parameters from DAG_RUN.CONF
    Demonstrates:
      - DAG parameter access
      - XCom push for downstream consumption
    """
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    
    print("\n" + "=" * 80)
    print("TASK: read_and_validate_params")
    print("=" * 80)
    print(f"  ✓ run_date           : {params.get('run_date', 'N/A')}")
    print(f"  ✓ customer_id        : {params.get('customer_id', 'N/A')}")
    print(f"  ✓ environment        : {params.get('environment', 'N/A')}")
    print(f"  ✓ full_load          : {params.get('full_load', False)}")
    print(f"  ✓ max_retries        : {params.get('max_retries', 2)}")
    print("=" * 80 + "\n")
    
    # Return params for XCom push (auto)
    return params


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: VALIDATION & BRANCHING LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def validate_input(**context):
    """
    Task 2: Basic input validation
    Demonstrates:
      - Task execution context
      - Print-based logging
    """
    ti = context["ti"]
    params = ti.xcom_pull(task_ids="read_params")
    
    print("\n" + "=" * 80)
    print("TASK: validate_input")
    print("=" * 80)
    print(f"  ✓ Received params via XCom: {params}")
    print(f"  ✓ Validation passed!")
    print("=" * 80 + "\n")
    
    return True


def decide_load_strategy(**context):
    """
    Task 3: Branch decision based on full_load parameter
    Demonstrates:
      - BranchPythonOperator logic
      - DAG run configuration access
      - Conditional branching
    
    Returns:
      "full_load_branch" or "incremental_branch"
    """
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    full_load = params.get("full_load", False)
    
    print("\n" + "=" * 80)
    print("TASK: decide_load_strategy (BRANCHING LOGIC)")
    print("=" * 80)
    print(f"  ↳ full_load parameter: {full_load}")
    
    if full_load:
        print(f"  ↳ Decision: → FULL_LOAD_BRANCH")
        print("=" * 80 + "\n")
        return "full_load_branch"
    else:
        print(f"  ↳ Decision: → INCREMENTAL_BRANCH")
        print("=" * 80 + "\n")
        return "incremental_branch"


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: FULL LOAD BRANCH
# ═══════════════════════════════════════════════════════════════════════════════

def extract_full_data(**context):
    """Extract task for FULL LOAD branch"""
    print("\n" + "=" * 80)
    print("TASK: extract_full_data")
    print("=" * 80)
    print("  ✓ Extracting full dataset from source...")
    print("  ✓ Rows extracted: 50000")
    print("=" * 80 + "\n")
    
    return {"rows": 50000, "load_type": "full", "source": "primary_db"}


def transform_full_data(**context):
    """Transform task for FULL LOAD branch"""
    ti = context["ti"]
    
    print("\n" + "=" * 80)
    print("TASK: transform_full_data")
    print("=" * 80)
    print("  ✓ Pulling extracted data via XCom...")
    
    extracted = ti.xcom_pull(task_ids="extract_full_data")
    print(f"  ✓ Received: {extracted}")
    print("  ✓ Applying transformations...")
    print("  ✓ Rows transformed: 48975 (removed duplicates)")
    print("=" * 80 + "\n")
    
    transformed = {
        "rows": 48975,
        "load_type": "full",
        "transformations_applied": ["deduplicate", "validate_schema"]
    }
    ti.xcom_push(key="full_transform_data", value=transformed)
    return transformed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: INCREMENTAL LOAD BRANCH
# ═══════════════════════════════════════════════════════════════════════════════

def extract_incremental_data(**context):
    """Extract task for INCREMENTAL branch"""
    print("\n" + "=" * 80)
    print("TASK: extract_incremental_data")
    print("=" * 80)
    print("  ✓ Extracting incremental dataset...")
    print("  ✓ Rows extracted: 1250")
    print("=" * 80 + "\n")
    
    return {"rows": 1250, "load_type": "incremental", "source": "primary_db"}


def transform_incremental_data(**context):
    """Transform task for INCREMENTAL branch"""
    ti = context["ti"]
    
    print("\n" + "=" * 80)
    print("TASK: transform_incremental_data")
    print("=" * 80)
    print("  ✓ Pulling extracted data via XCom...")
    
    extracted = ti.xcom_pull(task_ids="extract_incremental_data")
    print(f"  ✓ Received: {extracted}")
    print("  ✓ Applying transformations...")
    print("  ✓ Rows transformed: 1250 (no duplicates)")
    print("=" * 80 + "\n")
    
    transformed = {
        "rows": 1250,
        "load_type": "incremental",
        "transformations_applied": ["validate_schema"]
    }
    ti.xcom_push(key="incremental_transform_data", value=transformed)
    return transformed


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: PARALLEL BATCH PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

def parallel_batch_task(batch_num: int):
    """
    Generic task callable for parallel execution
    Demonstrates:
      - Multi-worker distribution (hostname, PID)
      - Parallel fan-out pattern
      - Task context variables
    
    Args:
        batch_num: Batch number for identification
    """
    def task_logic(**context):
        ti = context["ti"]
        hostname = socket.gethostname()
        pid = os.getpid()
        
        print("\n" + "─" * 80)
        print(f"TASK: parallel_batch_{batch_num}")
        print("─" * 80)
        print(f"  ✓ Executed on host: {hostname}")
        print(f"  ✓ Process PID: {pid}")
        print(f"  ✓ Task ID: {ti.task_id}")
        print(f"  ✓ Batch {batch_num} processing... sleeping 5 seconds")
        print("─" * 80 + "\n")
        
        time.sleep(5)  # Simulate heavy processing
        
        return {
            "batch": batch_num,
            "host": hostname,
            "pid": pid,
            "status": "completed"
        }
    
    return task_logic


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: RETRY HANDLING (FLAKY TASK)
# ═══════════════════════════════════════════════════════════════════════════════

def flaky_quality_check(**context):
    """
    Task that fails initially and succeeds after retries
    Demonstrates:
      - Retry logic (try_number tracking)
      - Exception handling
      - Eventual success after N retries
    """
    ti = context["ti"]
    try_number = ti.try_number
    
    print("\n" + "=" * 80)
    print(f"TASK: flaky_quality_check (Attempt #{try_number})")
    print("=" * 80)
    
    # Fail on first 2 attempts, succeed on 3rd
    if try_number < 3:
        print(f"  ✗ FAILED: Quality check failed (simulated)")
        print(f"  ↳ Retrying in {80} seconds... (Attempt #{try_number + 1})")
        print("=" * 80 + "\n")
        raise Exception(f"Quality check failed at attempt {try_number}")
    else:
        print(f"  ✓ SUCCESS: Quality check passed after {try_number - 1} retries!")
        print(f"  ✓ Data quality score: 98.5%")
        print("=" * 80 + "\n")
        
        return {
            "quality_score": 98.5,
            "passed_after_attempts": try_number,
            "status": "success"
        }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: FAN-IN / BRANCH REJOINING
# ═══════════════════════════════════════════════════════════════════════════════

def join_branches(**context):
    """
    Task that runs after both branches complete
    Demonstrates:
      - Fan-in pattern
      - Trigger rule: none_failed_min_one_success
      - Multiple upstream task dependencies
    """
    ti = context["ti"]
    
    print("\n" + "=" * 80)
    print("TASK: join_branches (FAN-IN)")
    print("=" * 80)
    print("  ✓ Both load branches completed!")
    print("  ✓ Merging results...")
    
    # Try pulling from both branches (one will exist)
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
        print(f"  ✓ Full Load Data: {full_data['rows']} rows")
    if incremental_data:
        print(f"  ✓ Incremental Data: {incremental_data['rows']} rows")
    
    print("=" * 80 + "\n")
    
    return {
        "merge_status": "success",
        "total_batches_processed": 5,
        "timestamp": datetime.now().isoformat()
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: FINALIZATION & SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

def finalize_pipeline(**context):
    """
    Final task: summarize entire DAG run
    Demonstrates:
      - Pulling multiple upstream XCom values
      - DAG run parameters access
      - Pipeline completion summary
    """
    ti = context["ti"]
    dag_run = context.get("dag_run", {})
    params = dag_run.get("conf", {})
    
    print("\n" + "=" * 80)
    print("TASK: finalize_pipeline")
    print("=" * 80)
    print("\n  📊 FINAL SUMMARY:")
    print(f"     • DAG Run ID: {dag_run.get('run_id', 'N/A')}")
    print(f"     • Run Date: {params.get('run_date', 'N/A')}")
    print(f"     • Customer ID: {params.get('customer_id', 'N/A')}")
    print(f"     • Environment: {params.get('environment', 'N/A')}")
    print(f"     • Load Type: {'FULL' if params.get('full_load') else 'INCREMENTAL'}")
    print(f"\n  ✓ All tasks completed successfully!")
    print(f"  ✓ Data pipeline execution finished at: {datetime.now().isoformat()}")
    print("=" * 80 + "\n")
    
    return {
        "pipeline_status": "completed",
        "timestamp": datetime.now().isoformat()
    }


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 1,
    "retry_delay_seconds": 60,
}

with DAG(
    dag_id="comprehensive_pi_flow_demo",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    description="Comprehensive Pi-Flow Demo covering all major features",
    
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
        "max_retries": Param(
            type="integer",
            default=2,
            description="Maximum retry attempts for flaky tasks"
        ),
    },
    
    # Default Arguments (inherited by all tasks unless overridden)
    default_args=default_args,
    
) as dag:
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 1: INITIALIZATION
    # ─────────────────────────────────────────────────────────────────────────────
    
    read_params = PythonOperator(
        task_id="read_params",
        python_callable=read_and_validate_params,
        doc="Read and validate runtime parameters from DAG_RUN.CONF",
    )
    
    validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
        doc="Validate input parameters before processing",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 2: BRANCH DECISION
    # ─────────────────────────────────────────────────────────────────────────────
    
    decide_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_load_strategy,
        doc="Decide between FULL_LOAD and INCREMENTAL branches based on params",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 3A: FULL LOAD BRANCH
    # ─────────────────────────────────────────────────────────────────────────────
    
    extract_full_data_task = PythonOperator(
        task_id="extract_full_data",
        python_callable=extract_full_data,
        doc="Extract full dataset from source database",
    )
    
    transform_full_data_task = PythonOperator(
        task_id="transform_full_data",
        python_callable=transform_full_data,
        doc="Transform full dataset with deduplication",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 3B: INCREMENTAL LOAD BRANCH
    # ─────────────────────────────────────────────────────────────────────────────
    
    extract_incremental_data_task = PythonOperator(
        task_id="extract_incremental_data",
        python_callable=extract_incremental_data,
        doc="Extract incremental dataset (delta only)",
    )
    
    transform_incremental_data_task = PythonOperator(
        task_id="transform_incremental_data",
        python_callable=transform_incremental_data,
        doc="Transform incremental dataset",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 4: PARALLEL BATCH PROCESSING (Fan-Out)
    # ─────────────────────────────────────────────────────────────────────────────
    # These 5 tasks run in parallel, demonstrating multi-worker distribution
    
    parallel_batch_tasks = [
        PythonOperator(
            task_id=f"parallel_batch_{i}",
            python_callable=parallel_batch_task(i),
            trigger_rule="none_failed",
            doc=f"Parallel batch processing task {i}",
        )
        for i in range(1, 6)
    ]
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 5: RETRY HANDLING (FLAKY TASK)
    # ─────────────────────────────────────────────────────────────────────────────
    
    flaky_quality_check_task = PythonOperator(
        task_id="flaky_quality_check",
        python_callable=flaky_quality_check,
        retries=2,  # Will fail on attempt 1-2, succeed on attempt 3
        retry_delay_seconds=5,  # Wait 5 seconds between retries (shorter for demo)
        doc="Quality check task that retries on failure (flaky task demo)",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 6: FAN-IN (Branch Rejoining)
    # ─────────────────────────────────────────────────────────────────────────────
    
    join_branches_task = PythonOperator(
        task_id="join_branches",
        python_callable=join_branches,
        trigger_rule="none_failed_min_one_success",  # At least one branch must succeed
        doc="Join both load branches after completion",
    )
    
    # ─────────────────────────────────────────────────────────────────────────────
    # PHASE 7: FINALIZATION
    # ─────────────────────────────────────────────────────────────────────────────
    
    finalize_task = PythonOperator(
        task_id="finalize_pipeline",
        python_callable=finalize_pipeline,
        trigger_rule="all_success",
        doc="Final task: summarize DAG execution and generate report",
    )
    
    # ═════════════════════════════════════════════════════════════════════════════
    # DEPENDENCIES / WORKFLOW ORCHESTRATION
    # ═════════════════════════════════════════════════════════════════════════════
    
    # Phase 1: Init → Validate
    read_params >> validate_input
    
    # Phase 2: Branch Decision
    validate_input >> decide_branch
    
    # Phase 3: Branch Execution
    decide_branch >> [extract_full_data_task, extract_incremental_data_task]
    
    # Phase 3: Branch Continuation
    extract_full_data_task >> transform_full_data_task
    extract_incremental_data_task >> transform_incremental_data_task
    
    # Phase 4: Both branches → Parallel processing
    [transform_full_data_task, transform_incremental_data_task] >> parallel_batch_tasks
    
    # Phase 5: Parallel tasks → Quality check (with retries)
    parallel_batch_tasks >> flaky_quality_check_task
    
    # Phase 6: Quality check → Join branches
    flaky_quality_check_task >> join_branches_task
    
    # Phase 7: Join → Finalize
    join_branches_task >> finalize_task

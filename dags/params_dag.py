from dag_parser.dynamic.dag_context import DAG
from dag_parser.dynamic.operators import PythonOperator
from dag_parser.dynamic.params import Param
from datetime import datetime


def show_params(**context):
    dag_run = context["dag_run"]
    params = dag_run.conf or {}

    print("==== DAG_RUN.PARAMS ====")
    print(f"run_date    : {params.get('run_date')}")
    print(f"customer_id : {params.get('customer_id')}")
    print(f"full_load   : {params.get('full_load')}")
    print("========================")


def decide_load(**context):
    params = context["dag_run"].conf or {}
    if params.get("full_load"):
        print("Running FULL LOAD logic")
    else:
        print("Running INCREMENTAL LOAD logic")


with DAG(
    dag_id="pi_flow_param_demo",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    params={
        "run_date": Param(
            type="string",
            required=True,
            description="Business date for the run"
        ),
        "customer_id": Param(
            type="integer",
            default=0,
            description="Customer identifier" 
        ),
        "full_load": Param(
            type="boolean",
            default=False,
            description="Whether to run full load"
        ),
    },
) as dag:

    print_parameters = PythonOperator(
        task_id="print_parameters",
        python_callable=show_params,
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=decide_load,
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=lambda **_: print("Pipeline completed"),
    )

    # Dependencies
    print_parameters >> process_data >> finalize

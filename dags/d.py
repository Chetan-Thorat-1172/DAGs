from datetime import datetime, timedelta
from airflow import models
from airflow.operators import bash, empty, python, email, trigger_dagrun
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from BIC_I6.project import config
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

with models.DAG(
    'dwi6_TenantCheck',
    default_args=config.default_args,
    tags=["BIC_I6"],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    #schedule=timedelta(minutes=5),
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    params={
    #"processDate": Param(datetime.today().strftime('%Y-%m-%d'), type="string",
    #  description="ProcessDate"),
    "restartind": Param("N", type="string",
      description="restartind")  
      }  
    ) as dag:
    
  def get_Prcs_Dte(**kwargs):
    ti = kwargs['ti']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=config.CONNECTION_ID,database=config.APP_DB, schema=config.APP_SCHEMA)
    sql_query = "SELECT PRCS_DTE FROM V_GET_PRCS_DTE WHERE APPL_NAME=%s;"
    
    # Define parameter values
    parameter_value =  Variable.get("I6_BIC_VARS", deserialize_json=True)['APPL_CODE']
    result = snowflake_hook.get_first(sql_query,[parameter_value])
    # Assuming result is a single value, otherwise adjust accordingly
    
    ti.xcom_push(key="PrcsDte", value= result[0] if result is not None else '1900-01-01')
 
  GetPrcsDte = PythonOperator(
    task_id='GetPrcsDte',
    python_callable=get_Prcs_Dte,
    provide_context=True,
   )

  CheckTenant = SQLExecuteQueryOperator(
    task_id='CheckTenant',
    sql="call p_tenant_check(%s ,'AIRFLOW',%s, %s)",
    #parameters=[ Variable.get("B3_BIC_VARS", deserialize_json=True)['APPL_CODE'],'{{dag_run.conf.get("processDate") if dag_run.conf.get("processDate") else params.get("processDate")}}','{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'],
    parameters=[ Variable.get("I6_BIC_VARS", deserialize_json=True)['APPL_CODE'],'{{task_instance.xcom_pull(key="PrcsDte",task_ids="GetPrcsDte")}}','{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'],
    split_statements=True,
    conn_id= config.CONNECTION_ID,
    hook_params={'database': config.APP_DB, 'schema': config.APP_SCHEMA},    
    return_last=False,
  )  

  def retrieve_return_value(ti):
    return_value =  ti.xcom_pull(task_ids="CheckTenant",key="return_value")
    print("Return value:", return_value)
    ti.xcom_push(key="return",value=return_value[0][0][0])

  retrieve_return_value_task = PythonOperator(
    task_id='retrieve_return_value',
    python_callable=retrieve_return_value,
    provide_context=True,
    dag=dag,
  )  
  
  def condition(ti):
    # Evaluate your condition here
    # For example, return the task_id of the next task based on the condition
    if ti.xcom_pull(key="return",task_ids="retrieve_return_value") == "S":
        return 'dwi6_main'
    else:
        return 'SkipProcessing'
        
  branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=condition,
    provide_context=True,
    dag=dag,
  )
  
  dwi6_main = trigger_dagrun.TriggerDagRunOperator(
    task_id='dwi6_main',
    trigger_dag_id='dwi6_main',
    wait_for_completion=True,
    poke_interval=10,
    retries=0,
      )
  
  SkipProcessing=DummyOperator(task_id='SkipProcessing', dag=dag)  
  
  GetPrcsDte>>CheckTenant >> retrieve_return_value_task
  retrieve_return_value_task >> branch_task
  branch_task >> [dwi6_main, SkipProcessing]
 

from datetime import datetime
from airflow import models
from airflow.operators import bash, empty, python, email, trigger_dagrun
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from BIC_L5.project import config
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.models.param import Param

with models.DAG(
    'dwl5_main',
    default_args=config.default_args,
    tags=["BIC_L5"],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule=None,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    params={
    "restartind": Param("N", type="string",
      description="restartind")  
      }    

    ) as dag:
    

  def getParams(**kwargs):
    ti = kwargs['ti']
    snowflake_hook = SnowflakeHook(snowflake_conn_id=config.CONNECTION_ID, database=config.APP_DB, schema=config.APP_SCHEMA)
    sql_query = "select to_char(processDate,'YYYY-MM-DD') AS processDate, DateForNames, PrcsDte,BankFileCreateDate from v_application_parameters WHERE application_code = %s;"
    
    # Define parameter values
    parameter_value =  Variable.get("L5_BIC_VARS", deserialize_json=True)['APPL_CODE']
    result = snowflake_hook.get_first(sql_query,[parameter_value])
    # Assuming result is a single value, otherwise adjust accordingly
    landing_view_database =  Variable.get("L5_BIC_VARS", deserialize_json=True)['landing_view_database']
    landing_view_schema =  Variable.get("L5_BIC_VARS", deserialize_json=True)['landing_view_schema']
    curation_database =  Variable.get("L5_BIC_VARS", deserialize_json=True)['curation_database']
    curation_schema =  Variable.get("L5_BIC_VARS", deserialize_json=True)['curation_schema']
    
    ti.xcom_push(key="processDate", value=result[0])
    ti.xcom_push(key="DateForNames",value=result[1])
    ti.xcom_push(key="PrcsDte", value=result[2])
    ti.xcom_push(key="BankFileCreateDate", value=result[3])
    ti.xcom_push(key="APPCODE", value=parameter_value)
    ti.xcom_push(key="landing_view_database", value=landing_view_database) #read from Airflow Variables
    ti.xcom_push(key="landing_view_schema", value=landing_view_schema)  #read from Airflow Variables
    ti.xcom_push(key="curation_database", value=curation_database) #read from Airflow Variables
    ti.xcom_push(key="curation_schema", value=curation_schema)  #read from Airflow Variables    
    #return result

  Getresults = PythonOperator(
    task_id='getParams',
    python_callable=getParams,
    provide_context=True,
   )
   
  dwl5main_Log_Start = SQLExecuteQueryOperator(
    task_id='dwl5main_Log_Start',
    sql="call P_UPDATE_AUDIT_TRAIL(%s,%s,'l5 Processing dwl5main','STARTED','INSERT','AIRFLOW', %s)",
    parameters=['{{task_instance.xcom_pull(key="processDate",task_ids="getParams")}}'  ,'{{task_instance.xcom_pull(key="APPCODE",task_ids="getParams")}}','{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'],
    split_statements=True,
    conn_id= config.CONNECTION_ID,
    hook_params={'database': config.APP_DB, 'schema': config.APP_SCHEMA},    
    return_last=False,
  )

  dwl5i1nnn= trigger_dagrun.TriggerDagRunOperator(
    task_id='dwl5i1nnn',     
    trigger_dag_id='dwl5i1nnn',               
    wait_for_completion=True,
    poke_interval=10,
    retries=0,
    conf={
      "processDate": '{{task_instance.xcom_pull(key="processDate",task_ids="getParams")}}' ,
      "DateForNames": '{{task_instance.xcom_pull(key="DateForNames",task_ids="getParams")}}' ,
      "PrcsDte": '{{task_instance.xcom_pull(key="PrcsDte",task_ids="getParams")}}' ,
      "BankFileCreateDate": '{{task_instance.xcom_pull(key="BankFileCreateDate",task_ids="getParams")}}',
      "APPCODE": '{{task_instance.xcom_pull(key="APPCODE",task_ids="getParams")}}',
      "landing_view_database": '{{task_instance.xcom_pull(key="landing_view_database",task_ids="getParams")}}',
      "landing_view_schema": '{{task_instance.xcom_pull(key="landing_view_schema",task_ids="getParams")}}',
      "curation_database": '{{task_instance.xcom_pull(key="curation_database",task_ids="getParams")}}',
      "curation_schema": '{{task_instance.xcom_pull(key="curation_schema",task_ids="getParams")}}'      
      
      }
      )
                            

  dwl5_PostProcessing=trigger_dagrun.TriggerDagRunOperator(
    task_id='dwl5_PostProcessing',
    trigger_dag_id='dwl5_PostProcessing',
    wait_for_completion=True,
    poke_interval=10,
    retries=0,
    conf={
      "processDate": '{{task_instance.xcom_pull(key="processDate",task_ids="getParams")}}' ,
      "DateForNames": '{{task_instance.xcom_pull(key="DateForNames",task_ids="getParams")}}' ,
      "PrcsDte": '{{task_instance.xcom_pull(key="PrcsDte",task_ids="getParams")}}' ,
      "BankFileCreateDate": '{{task_instance.xcom_pull(key="BankFileCreateDate",task_ids="getParams")}}',
      "APPCODE": '{{task_instance.xcom_pull(key="APPCODE",task_ids="getParams")}}',
      "landing_view_database": '{{task_instance.xcom_pull(key="landing_view_database",task_ids="getParams")}}',
      "landing_view_schema": '{{task_instance.xcom_pull(key="landing_view_schema",task_ids="getParams")}}',
      "curation_database": '{{task_instance.xcom_pull(key="curation_database",task_ids="getParams")}}',
      "curation_schema": '{{task_instance.xcom_pull(key="curation_schema",task_ids="getParams")}}' ,     
      "restartind": '{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'
      }
    )
   
  dwl5main_Log_Comp = SQLExecuteQueryOperator(
    task_id='dwl5main_Log_Comp',
    sql="call P_UPDATE_AUDIT_TRAIL(%s,%s,'l5 Processing dwl5main','COMPLETED','UPDATE','AIRFLOW',%s)",
    parameters=['{{task_instance.xcom_pull(key="processDate",task_ids="getParams")}}'  ,'{{task_instance.xcom_pull(key="APPCODE",task_ids="getParams")}}','{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'],
    split_statements=True,
    trigger_rule='none_failed',
    conn_id= config.CONNECTION_ID,
    hook_params={'database': config.APP_DB, 'schema': config.APP_SCHEMA},    
    return_last=False,
  )

  dwl5main_Log_Err = SQLExecuteQueryOperator(
    task_id='dwl5main_Log_Err',
    sql="call P_UPDATE_AUDIT_TRAIL(%s,%s,'l5 Processing dwl5main','ERROR','UPDATE','AIRFLOW',%s)",
    parameters=['{{task_instance.xcom_pull(key="processDate",task_ids="getParams")}}'  ,'{{task_instance.xcom_pull(key="APPCODE",task_ids="getParams")}}','{{dag_run.conf.get("restartind") if dag_run.conf.get("restartind") else params.get("restartind")}}'],
    split_statements=True,
    trigger_rule='one_failed',
    conn_id= config.CONNECTION_ID,
    hook_params={'database': config.APP_DB, 'schema': config.APP_SCHEMA},
    return_last=False,
  )

Getresults >>dwl5main_Log_Start >>  dwl5i1nnn >>  dwl5_PostProcessing >> [dwl5main_Log_Comp, dwl5main_Log_Err]

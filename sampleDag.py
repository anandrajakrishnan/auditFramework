'''
Created on Dec. 29, 2024

@author: anand
'''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils import getappid, initbatch, callproc, closebatch
#
dag = DAG (
    dag_id='sample_dag', schedule_interval = None
    )
#
@getappid(application_name='SAMPLE_ETL')
def p_get_applicationid(*args, **kwargs):
    return kwargs['returnJson']
#
@initbatch(task_id=1)
def p_initializebatch(*args, **kwargs):
    return kwargs['returnJson']
#
@callproc(task_id=2)
def p_dataacquisition(*args, **kwargs):
    return kwargs['returnJson']
#
@callproc(task_id=3)
def p_build_dimension1(*args, **kwargs):
    return kwargs['returnJson']
#
@callproc(task_id=4)
def p_build_dimension2(*args, **kwargs):
    return kwargs['returnJson']
#
@callproc(task_id=5)
def p_build_fact(*args, **kwargs):
    return kwargs['returnJson']
#
@closebatch(task_id=6)
def p_closebatch(*args, **kwargs):
    return kwargs['returnJson']
#
with dag:
    get_applicationid = PythonOperator(
        task_id = 'get_applicationid',
        python_callable = p_get_applicationid,
        provide_context=True
        )
    initializebatch = PythonOperator(
        task_id = 'initializebatch',
        python_callable = p_initializebatch,
        provide_context = True
        )
    dataacquisition = PythonOperator(
        task_id = 'dataacquisition',
        python_callable = p_dataacquisition,
        provide_context = True
        )
    build_dimension1 = PythonOperator(
        task_id = 'build_dimension1',
        python_callable = p_build_dimension1,
        provide_context = True
        )
    build_dimension2 = PythonOperator(
        task_id = 'build_dimension2',
        python_callable = p_build_dimension2,
        provide_context = True
        )
    build_fact = PythonOperator(
        task_id = 'build_fact',
        python_callable = p_build_fact,
        provide_context = True
        )
    closebatch = PythonOperator(
        task_id = 'closebatch',
        python_callable = p_closebatch,
        provide_context = True
        )
#
get_applicationid >> initializebatch
initializebatch >> dataacquisition
dataacquisition >> [build_dimension1, build_dimension2] >> build_fact
build_fact >> closebatch

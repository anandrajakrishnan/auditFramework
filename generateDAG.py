import yaml
#
TEMPLATE_IMPORT = """from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from utils import getappid, initbatch, callproc, closebatch
#"""
TEMPLATE_DAG="""dag = DAG(
    dag_id=':dag_id:', schedule_interval=':schedule_interval:', start_date=days_ago(1)
)
#"""
#
TEMPLATE_FUNCTION="""@:decorator:(:param:=:param_value:)
def :function_name:(*args, **kwargs):
    return kwargs['returnJson']
#"""
#
TEMPLATE_PYTHON_OPERATOR="""    :task_id: = PythonOperator(
        task_id=':task_id:',
        python_callable=:function_name:,
        provide_context=True
    )"""
with open('<location of feeded yaml file>') as inFile:
    ymlData = yaml.safe_load(inFile)
#
if 'dag' not in ymlData:
    print('dag element is missing from the yml file.')
    exit(1)
if 'task' not in ymlData:
    print('task element is missing from the yml file.')
    exit(1)
dag_id = ymlData['dag']['dag_id']
schedule_interval = ymlData['dag']['schedule_interval']
print(TEMPLATE_IMPORT)
print(TEMPLATE_DAG.replace(':dag_id:', dag_id).replace(':schedule_interval:', schedule_interval))
for eachKey in ymlData['task']:
    eachTask = ymlData['task'][eachKey]
    taskName = eachKey
    function_name = 'p_' + taskName
    decorator = eachTask['decorator']
    if decorator == 'getappid':
        param = 'application_name'
        param_value = "'" + dag_id + "'"
    else:
        param = 'task_id'
        param_value = eachTask['task_id']
    print(TEMPLATE_FUNCTION.replace(':decorator:', decorator).replace(':param:', param).replace(':param_value:', str(param_value)).replace(':function_name:', function_name))
#
print('with dag:')
for eachKey in ymlData['task']:
    eachTask = ymlData['task'][eachKey]
    taskName = eachKey
    function_name = 'p_' + taskName
    if ymlData['task'][eachKey]['operator'] == 'PythonOperator':
        print(TEMPLATE_PYTHON_OPERATOR.replace(':task_id:', taskName).replace(':function_name:', function_name))
#
for eachKey in ymlData['task']:
    if ymlData['task'][eachKey]['upstreamTask'] is None:
        continue
    else:
        if len(ymlData['task'][eachKey]['upstreamTask']) == 1:
            upstreamTask = ymlData['task'][eachKey]['upstreamTask'][0]
        else:
            upstreamTask = '[' + ','.join(ymlData['task'][eachKey]['upstreamTask']) + ']'
    print(upstreamTask + ' >> ' + eachKey)

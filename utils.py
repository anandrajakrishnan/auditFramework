'''
Created on Dec. 29, 2024

@author: anand
'''
from functools import wraps
from airflow.operators.python import get_current_context
#
def getappid(application_name):
    def decorator_getappid(func):
        @wraps(func)
        def wrapper_getappid(*args, **kwargs):
            # get application_id using application_name by
            # using lookup on audit_application table
            # create a json output to be returned to caller function
            application_id=101
            returnJson={}
            returnJson['application_id']=application_id
            returnJson['application_name']=application_name
            kwargs['returnJson']=returnJson
            return func(*args, **kwargs)
        return decorator_getappid
#
def initbatch(task_id):
    def decorator_initbatch(func):
        @wraps(func)
        def wrapper_initbatch(*args, **kwargs):
            ti=kwargs['ti']
            currentContext=get_current_context()
            upstreamTaskIDs=list(currentContext['task'].upstream_task_ids)
            prevTaskID=upstreamTaskIDs[0]
            prevTaskIDReturn=ti.xcom_pull(task_ids=prevTaskID)
            application_id=str(prevTaskIDReturn['application_id'])
            # check the status of audit_batch_log that has application_id from above
            # and max(load_start_date)
            # if the status is not 'C' (i.e. complete):
            #   fail the step
            # else:
            # move to next step
            # insert into audit_batch_log:
            #   application_id
            #   batch_start_date with current timestamp
            #   status as 'I' (i.e. initiated)
            # fetch the new batch_id from audit_batch_log
            batch_id = 101
            # insert into audit_batch_control:
            #   batch_id from above
            #   application_id from above
            #   for each task_id in the application_id
            #   status as 'I' (i.e. initiated)
            #   task_start_date as current timestamp
            # update audit_batch_execution_control (batch_id, application_id, task_id):
            #   status as 'C' (i.e. completed)
            #   task_end_date as current timestamp
            returnJson=prevTaskIDReturn
            returnJson['batch_id']=batch_id
            kwargs['returnJson']=returnJson
            return func(*args, **kwargs)
        return wrapper_initbatch
    return decorator_initbatch
#
def callproc(task_id):
    def decorator_callproc(func):
        def wrapper_callproc(*args, **kwargs):
            ti=kwargs['ti']
            currentContext=get_current_context()
            upstreamTaskIDs=list(currentContext['task'].upstream_task_ids)
            prevTaskID=upstreamTaskIDs[0]
            prevTaskIDReturn=ti.xcom_pull(task_ids=prevTaskID)
            batch_id=prevTaskIDReturn['batch_id']
            application_id=prevTaskIDReturn['application_id']
            # update audit_batch_control(batch_id, application_id, task_id):
            #   status as 'I'
            #   task_start_date as current timestamp
            # lookup audit_app_task using application_id and task_id
            #   fetch the task_name, which gives the name of the stored procedure
            # call the stored procedure "task_name"
            #   make sure that the stored procedure returns the source_record_count
            #   and target_record_count
            # update audit_batch_execution_control (batch_id, application_id, task_id):
            #   status as 'C' (i.e. completed)
            #   task_end_date as current timestamp
            #   source_record_count
            #   target_record_count
            returnJson=prevTaskIDReturn
            kwargs['returnJson']=returnJson
            return func(*args, **kwargs)
        return wrapper_callproc
    return decorator_callproc
#
def closebatch(task_id):
    def decorator_closebatch(func):
        @wraps(func)
        def wrapper_closebatch(*args, **kwargs):
            ti=kwargs['ti']
            currentContext=get_current_context()
            upstreamTaskIDs=list(currentContext['task'].upstream_task_ids)
            prevTaskID=upstreamTaskIDs[0]
            prevTaskIDReturn=ti.xcom_pull(task_ids=prevTaskID)
            application_id=prevTaskIDReturn['application_id']
            batch_id=prevTaskIDReturn['batch_id']
            # update audit_batch_log(batch_id, application_id):
            #   status as 'C'
            #   batch_end_time as current timestamp
            returnJson=prevTaskIDReturn
            kwargs['returnJson']=returnJson
            return func(*args, **kwargs)
        return wrapper_closebatch
    return decorator_closebatch
#

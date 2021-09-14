# note that <executor> config item in airflow.cfg should be set to parallel
# executor e.g. LocalExecutor instead of default SequentialExectuor
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.models.baseoperator import cross_downstream

with  DAG( dag_id='test_tasks', start_date=datetime(2021,9,12),schedule_interval=None ) as dag:
    t = [ DummyOperator(task_id=f'task{i}') for i in range(1,7)]

cross_downstream(t[0:1],t[1:3])
cross_downstream(t[1:3],t[3:6])

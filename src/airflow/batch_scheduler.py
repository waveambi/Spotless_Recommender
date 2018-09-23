import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


schedule_interval = timedelta(days=1)

default_args = {
    'owner': 'Tao',
    'depends_on_past': False,
    'start_date': datetime.now() - schedule_interval,
    'email': ['wave.songtao@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': callback_function,
    # 'on_success_callback': callback_function,
    # 'on_retry_callback': callback_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'batch_scheduler',
    default_args=default_args,
    description='DAG for the Spark Batch Job',
    schedule_interval=schedule_interval)


task1 = BashOperator(
    task_id='run_batch_processing_job',
    bash_command='cd /home/ubuntu/Insight_Restaurant_Recommendation/src ; bash spark-batch-run.sh',
    dag=dag)

task2 = BashOperator(
    task_id='run_batch_machine_learning_job',
    bash_command='cd /home/ubuntu/Insight_Restaurant_Recommendation/src ; bash spark-batch-machine-learning-run.sh',
    dag=dag)


task1.doc_md = """\
Spark Batch Processing Job is scheduled to start every other day
"""

task2.doc_md = """\
#### Task Documentation
Spark Machine Learning Job is scheduled to start every other day
"""

dag.doc_md = __doc__
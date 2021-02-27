from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
import json  # https://bigdata-etl.com/apache-airflow-create-dynamic-dag/
from airflow.operators.python import PythonOperator
from karen_dag.operators.operator import finnhub_test, karens_custom_dag


def create_dag(dag_id,
               schedule,
               default_args,
               conf):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        init = DummyOperator(
            task_id='start_tasks',
            dag=dag
        )
        karen = PythonOperator(
            task_id='karens_job_interview',
            python_callable=karens_custom_dag,
            dag=dag

        )

        karen_taks2 = PythonOperator(
            task_id='karens_job_interview2',
            python_callable=karens_custom_dag,
            dag=dag
        )


        init >> karen >> karen_taks2

        return dag


def generate_dag_from_config():
    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, 'config.json')) as json_data:
        conf = json.load(json_data)
        schedule = conf['schedule']
        dag_id = conf['name']

        args = {
            'owner': 'deeptendies',
            'depends_on_past': False,
            'start_date': datetime.now(),
            'email': ['deeptendies@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'concurrency': 1,
            'max_active_runs': 1
        }
        globals()[dag_id] = create_dag(dag_id, schedule, args, conf)


generate_dag_from_config()

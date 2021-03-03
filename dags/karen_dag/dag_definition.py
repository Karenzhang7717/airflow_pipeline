from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
import json 
import os
from airflow.operators.python import PythonOperator
from karen_dag.operators.operator import read_from_psql,read_from_txt


def create_dag(dag_id,schedule,default_args,conf):
    '''Task code that creates dags'''
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        init = DummyOperator(
            task_id='start_tasks',
            dag=dag
        )
        Procurement_and_x_processing = PythonOperator(
            task_id='data_from_psql',
            python_callable=read_from_psql,
            dag=dag
        )

        x_material = PythonOperator(
            task_id='data_from_txt',
            python_callable=read_from_txt,
            dag=dag
        )


        init >> Procurement_and_x_processing >> x_material

        return dag


def generate_dag_from_config():
    '''Task code that generates dags from configuration'''
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, 'config.json')) as json_data:
        conf = json.load(json_data)
        schedule = conf['schedule']
        dag_id = conf['name']
        #sets arguments of the configuration
        args = {
            'owner': 'karen',
            'depends_on_past': False,
            'start_date': datetime.now(),
            'email': ['karenzhang7717@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'concurrency': 1,
            'max_active_runs': 1
        }
        globals()[dag_id] = create_dag(dag_id, schedule, args, conf)


generate_dag_from_config()

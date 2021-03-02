import logging
import pandas as pd
from airflow.example_dags.example_python_operator import print_context
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice
import psycopg2
from psycopg2 import Error

def read_from_psql(ds, *args, **kwargs): # dag function
    print(ds)
    print(kwargs)
    #todo: write your functions in here
    #output_file()
    print('success')
    sources = 0


    connection = psycopg2.connect(user="karen",
                                  password="karen",
                                  host="airflow-karen_postgres_karen_interview_1",
                                  port="5432",
                                  database="karen")
    cursor = connection.cursor()

    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")

    hook = PostgresHook(postgres_conn_id='karen',
                        postgres_default='karen',
                        autocommit=True,
                        database="postgres_db")
    print(hook)
    hook.run('select * from ball_milling;')
    # request='SELECT * FROM ball_milling'
    # connection=pg_hook.get_conn()
    # cursor=connection.cursor()
    # cursor.execute(request)
    # sources=cursor.fetchall()
    # for source in sources:
    #     print('sources: {0} - activated: {1}'.format(source[0],source[1]))
    return sources


def karens_custom_dag(ds, *args, **kwargs): # dag function
    print(ds)
    print(kwargs)
    #todo: write your functions in here





    #todo:get data to be saved to csv.
    # data=kwargs['dag_run'].conf['csv_file_path']
    return ("success")


def helper_test_print(res): #generic function
    print("testing the feature:")
    print(res)


if __name__ == '__main__':
    karens_custom_dag()
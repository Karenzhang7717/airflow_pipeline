import logging
import pandas as pd
from airflow.example_dags.example_python_operator import print_context
from airflow.operators.python import PythonOperator


def read_from_psql(ds, *args, **kwargs): # dag function
    print(ds)
    print(kwargs)
    #todo: write your functions in here
    #output_file()
    print('success')
    request='SELECT * FROM ball_milling'
    pg_hook=PostgresHook(postgre_conn_id='sql_connection',schema='public')
    connection=pg_hook.get_conn()
    cursor=connection.cursor()
    cursor.execute(request)
    sources=cursor.fetchall()
    for source in sources:
        print('sources: {0} - activated: {1}'.format(source[0],source[1]))
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
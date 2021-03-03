

from airflow.example_dags.example_python_operator import print_context
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import psycopg2, psycopg2.extras
import logging

from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice

def read_from_psql(ds, *args, **kwargs): # dag function
    print(ds)
    print(kwargs)
    print('success')
    connection = psycopg2.connect(user='karen',
                                password='karen',
                                host='host.docker.internal',
                                port='5432',
                                database='postgres')
    request='SELECT * FROM ball_milling'
    pg_hook=PostgresHook(postgre_conn_id='karen',schema='public')
    cursor=connection.cursor()
    cursor.execute(request)
    sources=cursor.fetchall()
    for source in sources:
        print('sources: {0} - activated: {1}'.format(source[0],source[1]))
    # return sources

    # hook = PostgresHook(postgres_conn_id='karen',
    #                     postgres_default='karen',
    #                     autocommit=True,
    #                     database="karen",
    #                     schema='public')
    
    # request='SELECT * FROM ball_milling'
    # connection=hook.get_conn()
    print(connection)
    cursor=connection.cursor()
    cursor.execute(request)
    data_ball_milling=cursor.fetchall()
   # file_path="D:\\Karen\\test\\"
    file_path="/opt/airflow/logs/"
    print(file_path)
    # temp_path = file_path + '_dump_.csv'
    #temp_path = 'D:\\Karen\\test\\testbook.csv'
    tmp_path = file_path + 'dump.csv'
    with open(tmp_path, 'w', newline='') as fp:
        print(tmp_path)
        print('open success')
        a = csv.writer(fp, quoting = csv.QUOTE_ALL)
        a.writerow('testing')
        logging.info('testing+++++++++++++')
        # print('writer success')
        # print(cursor.description)
        # print(i[1] for i in cursor.description)
        a.writerow(i[0] for i in cursor.description)
        a.writerow(data_ball_milling)
        logging.info('finished writing rows')
    # full_path = temp_path + '.gz'
    # with open(temp_path, 'rb') as f:
    #     data = f.read()
    # f.close()
    #hook.bulk_dump(request,tmp_path)


    for source in data_ball_milling:
        print('sources: {0} - activated: {1}'.format(source[0],source[1]))
        print(os.getcwd()) 
    #return sources

    #todo:get data to be saved to csv.
    # data=kwargs['dag_run'].conf['csv_file_path']
 #   return ("success")

# def output_file():
#     dirpath = 'D:/Karen/airflow_pipeline/dae-challenge/x-lab-data'
#     output = 'D:/Karen/airflow_pipeline/dae-challenge/Book3.csv'
    
#     outfile = open(output, 'w',newline='')
#     csvout = csv.writer(outfile)
#     csvout.writerow(['data source','material_uid','Measurement','Probe Resistance (ohm)','Gas Flow Rate (L/min)','Gas Type','Probe Material','Current (mA)','Field Strength (T)','Sample Position','Magnet Reversal'])
#     files = os.listdir(dirpath)
#     files_hall=files[:35]
#     regex = re.compile(r'[\n\r\t]')

#     for filename in files_hall:
#         with open(dirpath + '/' + filename) as afile:
#             row=[] # list of values we will be constructing
           
#             for line in islice(afile, 2, None):#extract from the third row
#                 line_input = line.strip('" \n') # I will be explaining this later
#                 words=regex.sub(" ",line_input).split(' ')[-1]
#                 #row[0]='x-labs data'
#                 row.append(words) # adds the retrieved value to our row
#             row.insert(0,'X-LABS DATA')
#         csvout.writerow(row)
    
#     csvout.writerow(['data source','material_uid','Measurement','Pb concentration','Sn concentration','O Concentration','Gas Flow Rate (L/min)','Gas Type','Plasma Temperature (celsius)','Detector Temperature (celsius)','Field Strength (T)','Plasma Observation','Radio Frequency (MHz)'])
#     files_icp=files[35:]
#     for filename in files_icp:
#         with open(dirpath + '/' + filename) as afile:
#             row=[] # list of values we will be constructing
#             for line in islice(afile, 2, None):#extract from the third row
#                 line_input = line.strip('" \n') # I will be explaining this later
#                 words=regex.sub(" ",line_input).split(' ')[-1]
#                 row.append(words) # adds the retrieved value to our row
#             row.insert(0,'X-LABS DATA')
#         csvout.writerow(row)
    
#     outfile.close()



def helper_test_print(res): #generic function
    print("testing the feature:")
    print(res)


if __name__ == '__main__':
    read_from_psql(1)
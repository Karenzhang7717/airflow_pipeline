

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
    #pg_hook=PostgresHook(postgre_conn_id='karen',schema='public')
    cursor=connection.cursor()
    cursor.execute(request)
    # sources=cursor.fetchall()
    # for source in sources:
    #     print('sources: {0} - activated: {1}'.format(source[0],source[1]))
    # print(connection)
    # # cursor=connection.cursor()
    # # cursor.execute(request)
    data_ball_milling=cursor.fetchall()
    file_path="/opt/airflow/logs/"
    print(file_path)
  
    tmp_path = file_path + 'dump.csv'
    with open(tmp_path, 'w', newline='') as fp:
        print(tmp_path)
        print('open success')
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        #headers=[]
        #headers.append(i[0] for i in cursor.description)
        a.writerow(i[0] for i in cursor.description)
        #headers.insert(0,'data source')
        #a.writerow(headers)
        for data in data_ball_milling:
            a.writerow(data)

        logging.info('finished writing rows')
    
    request2='SELECT * FROM hot_press'
    cursor.execute(request2)
    data_hot_press=cursor.fetchall()
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_hot_press:
            a.writerow(data)

    request3='SELECT * FROM material_procurement'
    cursor.execute(request3)
    data_material_proc=cursor.fetchall()
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_material_proc:
            a.writerow(data)

def output_file(ds, *args, **kwargs):
    dirpath = '/opt/airflow/logs/x-lab-data'
    output="/opt/airflow/logs/dump.csv"
    outfile = open(output, 'a',newline='')
    csvout = csv.writer(outfile)
    csvout.writerow(['data source','material_uid','Measurement','Probe Resistance (ohm)','Gas Flow Rate (L/min)','Gas Type','Probe Material','Current (mA)','Field Strength (T)','Sample Position','Magnet Reversal'])
    files = os.listdir(dirpath)
  #  files_hall=files[:35]
    regex = re.compile(r'[\n\r\t]')

    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing       
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') # I will be explaining this later
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
            row.insert(0,'X-LABS DATA')
           # print(row[2])
        if row[2]=='Hall':
            csvout.writerow(row)
    
    csvout.writerow(['data source','material_uid','Measurement','Pb concentration','Sn concentration','O Concentration','Gas Flow Rate (L/min)','Gas Type','Plasma Temperature (celsius)','Detector Temperature (celsius)','Field Strength (T)','Plasma Observation','Radio Frequency (MHz)'])
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') # I will be explaining this later
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
            row.insert(0,'X-LABS DATA')
        if row[2]=='ICP':
            csvout.writerow(row)
    
    outfile.close()

def helper_test_print(res): #generic function
    print("testing the feature:")
    print(res)


if __name__ == '__main__':
    read_from_psql(1)
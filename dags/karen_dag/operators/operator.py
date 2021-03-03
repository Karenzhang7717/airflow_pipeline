
#from airflow.example_dags.example_python_operator import print_context
from airflow.operators.python import PythonOperator
#from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import psycopg2, psycopg2.extras
#import logging
from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice

def read_from_psql(ds, *args, **kwargs): # dag function
    connection = psycopg2.connect(user='karen',
                                password='karen',
                                host='host.docker.internal',
                                port='5432',
                                database='postgres')
    request='SELECT * FROM ball_milling'
    cursor=connection.cursor()
    cursor.execute(request)
    data_ball_milling=cursor.fetchall()
    file_path="/opt/airflow/logs/"
 
    tmp_path = file_path + 'master_db.csv'
    with open(tmp_path, 'w', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_ball_milling:
            a.writerow(data)
    
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

def read_from_txt(ds, *args, **kwargs):
    dirpath = '/opt/airflow/logs/x-lab-data'
    output="/opt/airflow/logs/master_db.csv"
    outfile = open(output, 'a',newline='')
    csvout = csv.writer(outfile)
    csvout.writerow(['data source','material_uid','Measurement','Probe Resistance (ohm)','Gas Flow Rate (L/min)','Gas Type','Probe Material','Current (mA)','Field Strength (T)','Sample Position','Magnet Reversal'])
    files = os.listdir(dirpath)
    regex = re.compile(r'[\n\r\t]')

    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing       
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') # I will be explaining this later
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
            row.insert(0,'X-LABS DATA')
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


# if __name__ == '__main__':
#     read_from_psql(1)
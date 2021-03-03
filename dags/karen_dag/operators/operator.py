from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice

def read_from_psql(ds, *args, **kwargs):
    '''A dag function that reads data from Procurement and X-processing's psql database and saves the queried data to the master csv file.
    '''
    #connects to psql database
    connection = psycopg2.connect(user='karen',
                                password='karen',
                                host='host.docker.internal',
                                port='5432',
                                database='postgres')
    #query data from ball_milling table
    request='SELECT * FROM ball_milling'
    cursor=connection.cursor()
    cursor.execute(request)
    data_ball_milling=cursor.fetchall()
    #file path to save the master csv db
    file_path="/opt/airflow/logs/"
    tmp_path = file_path + 'master_db.csv'
    #save the queried data from ball_milling to master_db.csv
    with open(tmp_path, 'w', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_ball_milling:
            a.writerow(data)

    #query data from hot_press table 
    request2='SELECT * FROM hot_press'
    cursor.execute(request2)
    data_hot_press=cursor.fetchall()
    #save the queried data from hot_press to master_db.csv
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_hot_press:
            a.writerow(data)
    #query data from material_procurement table    
    request3='SELECT * FROM material_procurement'
    cursor.execute(request3)
    data_material_proc=cursor.fetchall()
    #save the queried data from material_procurement to master_db.csv
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_material_proc:
            a.writerow(data)

def read_from_txt(ds, *args, **kwargs):
    '''A dag function that reads data from X-lab's txt files and saves the queried data to the master csv file.
    '''
    #the file path that saved x-lab's data
    dirpath = '/opt/airflow/logs/x-lab-data'
    #output file path
    output="/opt/airflow/logs/master_db.csv"
    outfile = open(output, 'a',newline='')
    csvout = csv.writer(outfile)
    #write headers
    csvout.writerow(['data source','material_uid','Measurement','Probe Resistance (ohm)','Gas Flow Rate (L/min)','Gas Type','Probe Material','Current (mA)','Field Strength (T)','Sample Position','Magnet Reversal'])
    files = os.listdir(dirpath)
    regex = re.compile(r'[\n\r\t]')
    #gathers all data uses Hall as the measurement
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing       
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
            row.insert(0,'X-LABS DATA')
        if row[2]=='Hall':
            csvout.writerow(row)

    #write headers
    csvout.writerow(['data source','material_uid','Measurement','Pb concentration','Sn concentration','O Concentration','Gas Flow Rate (L/min)','Gas Type','Plasma Temperature (celsius)','Detector Temperature (celsius)','Field Strength (T)','Plasma Observation','Radio Frequency (MHz)'])
    #gathers all data uses ICP as the measurement
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
            row.insert(0,'X-LABS DATA')
        if row[2]=='ICP':
            csvout.writerow(row)
    
    outfile.close()

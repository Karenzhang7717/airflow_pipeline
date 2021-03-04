from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice
import pandas as pd
from sqlalchemy import create_engine

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
    df_ball_milling = pd.DataFrame(data_ball_milling)
    print(df_ball_milling[:10])


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
    df_hot_press = pd.DataFrame(data_hot_press)
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
   # df_material_proc = pd.DataFrame()
    #df_material_proc.loc[0]=i[0] for i in cursor.description
    df_material_proc = pd.DataFrame(data_material_proc)
    #TODO: ADD HEADERS
    #headers_material_proc=i[0] for i in cursor.description
    #df_material_proc.loc[-1]=['a','b']
    #save the queried data from material_procurement to master_db.csv
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_material_proc:
            a.writerow(data)
    #df1=pd.merge(df_material_proc, df_ball_milling, how='left', left_on='ball_milling_uid', right_on='uid')
    #df1=df_ball_milling.join (df_material_proc.set_index( [ 'ball_milling_uid' ], verify_integrity=True ),on=[ 'uid' ], how='left' )
    print(df_material_proc[:10])

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
 
    connection = psycopg2.connect(user='karen',
                            password='karen',
                            host='host.docker.internal',
                            port='5432',
                            database='postgres')#sets connection to sql database
    cursor=connection.cursor()
    engine = create_engine('postgresql://karen:passwordkaren@localhost:5432/postgres')
    df=[]
    #TODO: sql set primary key
    df.to_sql('table_name', engine)   #write Hall results to psql database
    cursor.execute("")

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

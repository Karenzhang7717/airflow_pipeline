from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta
import os
import csv
import re
from itertools import islice
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine

connection = psycopg2.connect(user='karen',
                            password='karen',
                            host='host.docker.internal',
                            port='5432',
                            database='postgres')#sets connection to sql database
dirpath = '/opt/airflow/logs/x-lab-data'
files = os.listdir(dirpath)
regex = re.compile(r'[\n\r\t]')


def read_from_psql(ds, *args, **kwargs):
    '''A dag function that reads data from Procurement and X-processing's psql database and saves the queried data to the master csv file.
    '''
    #query data from ball_milling table
    request='SELECT * FROM ball_milling'
    cursor=connection.cursor()
    cursor.execute(request)
    data_ball_milling=cursor.fetchall()
    df_ball_milling = pd.DataFrame(data_ball_milling,columns=["uid","process_name","milling_time", "milling_time_units", "milling_speed", "milling_speed_units", "output_material_name", "output_material_uid", "hot_press_uid"])
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
    df_hot_press = pd.DataFrame(data_hot_press,columns=["uid", "process_name", "hot_press_temperature", "hot_press_temperature_units", "hot_press_pressure", "hot_press_pressure_units", "hot_press_time", "hot_press_time_units", "output_material_name", "output_material_uid"])
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
    df_material_proc = pd.DataFrame(data_material_proc,columns=['uid','material_name','mass_fraction','ball_milling_uid'])
  
    #df_material_proc1 = pd.DataFrame(columns=['uid','material_name','mass_fraction','ball_milling_uid'])
   # df_material_proc1.append(df_material_proc, ignore_index = True)
    with open(tmp_path, 'a', newline='') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ',')
        a.writerow(i[0] for i in cursor.description)
        for data in data_material_proc:
            a.writerow(data)
    df1=pd.merge(df_material_proc, df_ball_milling, how='left', left_on='ball_milling_uid', right_on='uid')
    df2=pd.merge(df1, df_hot_press, how='left', left_on='hot_press_uid', right_on='uid')
    print("df2 is............................................")
    print(df2[:10])
    return df2.to_json()

def read_from_txt_hall(ds, *args, **kwargs):
    '''A dag function that reads data from X-lab's txt files and saves the queried data to the master csv file.
    '''
    #the file path that saved x-lab's data
   # dirpath = '/opt/airflow/logs/x-lab-data'
    #output file path
    output="/opt/airflow/logs/master_db.csv"
    outfile = open(output, 'a',newline='')
    csvout = csv.writer(outfile)
    #write headers
    #csvout.writerow(['data_source','material_uid','Measurement','Probe_Resistance (ohm)','Gas_Flow_Rate_(L/min)','Gas_Type','Probe_Material','Current_(mA)','Field_Strength_(T)','Sample_Position','Magnet_Reversal'])
    #files = os.listdir(dirpath)
   # regex = re.compile(r'[\n\r\t]')
    #gathers all data uses Hall as the measurement
    
    df_hall=pd.DataFrame(columns=["material_uid","Measurement","Probe_Resistance_ohm","Gas_Flow_Rate_L_per_min",\
        "Gas_Type", "Probe_Material", "Current_mA", "Field_Strength_T", "Sample_Position","Magnet_Reversal"])
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing       
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
 
        if row[1]=='Hall':
            df_hall.loc[len(df_hall)]=row
   # print(df_hall)
 
  
    cursor=connection.cursor()
    # cursor.execute("CREATE TABLE IF NOT EXISTS public.lab_hall (\
    # data_source character varying(30),\
    # material_uid character varying(40),\
    # Measurement character varying(20),\
    # Probe_Resistance_ohm real,\
    # Gas_Flow_Rate_L_per_min real,\
    # Gas_Type character varying(10),\
    # Probe_Material character varying(10),\
    # Current_mA real,\
    # Field_Strength_T real,\
    # Sample_Position real,\
    # Magnet_Reversal character varying(10))")
 
    engine = create_engine('postgresql://karen:passwordkaren@host.docker.internal:5432/postgres')
    #cursor.execute("DROP TABLE IF EXISTS lab_hall")
    #TODO: sql set primary key
    df_hall.to_sql('lab_hall', engine,if_exists='replace',index=False)   #write Hall results to psql database
    cursor.execute("SELECT * FROM lab_hall")
    # data_hall=cursor.fetchall()
    # df_hall_psql = pd.DataFrame(data_hall)
    # print(df_hall_psql)
    #gathers all data uses ICP as the measurement
    return df_hall.to_json()



def read_from_txt_icp(ds, *args, **kwargs):
   # files = os.listdir(dirpath)
    #regex = re.compile(r'[\n\r\t]')
    df_icp=pd.DataFrame(columns=['material_uid','Measurement','Pb_concentration','Sn_concentration','O_Concentration','Gas_Flow_Rate_L_per_min','Gas_Type','Plasma_Temperature_celsius','Detector_Temperature_celsius','Field_Strength_T','Plasma_Observation','Radio_Frequency_MHz'])
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] # list of values we will be constructing
            for line in islice(afile, 2, None):#extract from the third row
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
        if row[1]=='ICP':
            df_icp.loc[len(df_icp)]=row
    engine = create_engine('postgresql://karen:passwordkaren@host.docker.internal:5432/postgres')
    df_icp.to_sql('lab_icp', engine,if_exists='replace',index=False)   #write icp results to psql database
    cursor=connection.cursor()
    cursor.execute("SELECT * FROM lab_icp")
    data_icp=cursor.fetchall()
    df_icp_psql = pd.DataFrame(data_icp)
    print(df_icp_psql)
    return df_icp.to_json()
 
 
def generate_master_csv(ds, *args, **kwargs):
    df_x_process=pd.read_json(read_from_psql(1))
    print("df4 is............................................")
    print(df_x_process)
    df_hall=pd.read_json( read_from_txt_hall(2))
    df_hall['material_uid']=json_normalize(df_hall['material_uid'])
    df_icp=pd.read_json( read_from_txt_icp(2))
    df_icp=df_icp.apply(json_normalize)
    #print(df_hall)
   # df_x_process['uid'] = df_x_process['uid'].astype(str)
    #df_hall['material_uid'] = df_hall['material_uid'].astype(str)
    df1=pd.merge(df_x_process, df_hall, how='left', left_on='uid', right_on='material_uid')
    df_x_process['uid'].convert_dtypes()
    print(df_x_process.columns.tolist())
    print(df_hall.columns.tolist())
    print(df1)
    print(df_x_process['uid'].dtype)
    print(df_hall['material_uid'].dtype)
   
    df2=pd.merge(df1, df_icp, how='left', left_on='uid', right_on='material_uid')
    df2.to_csv(r'/opt/airflow/logs/master_db1.csv', header='true')

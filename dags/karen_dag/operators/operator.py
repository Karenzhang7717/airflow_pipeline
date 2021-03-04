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
    
    #query data from hot_press table 
    request2='SELECT * FROM hot_press'
    cursor.execute(request2)
    data_hot_press=cursor.fetchall()
    df_hot_press = pd.DataFrame(data_hot_press,columns=["uid", "process_name", "hot_press_temperature", "hot_press_temperature_units", "hot_press_pressure", "hot_press_pressure_units", "hot_press_time", "hot_press_time_units", "output_material_name", "output_material_uid"])
  
    #query data from material_procurement table    
    request3='SELECT * FROM material_procurement'
    cursor.execute(request3)
    data_material_proc=cursor.fetchall()
    df_material_proc = pd.DataFrame(data_material_proc,columns=['uid','material_name','mass_fraction','ball_milling_uid'])
  
    df1=pd.merge(df_material_proc, df_ball_milling, how='left', left_on='ball_milling_uid', right_on='uid')
    df2=pd.merge(df1, df_hot_press, how='left', left_on='hot_press_uid', right_on='uid') #join dataframes from X-processing together
    return df2.to_json()

def read_from_txt_hall(ds, *args, **kwargs):
    '''
    A dag function that reads data from X-lab's measurements using Hall into a dataframe, and saves the dataframe to psql database
    Returns:
      The json object of the X lab's results measured by Hall.
    '''
    df_hall=pd.DataFrame(columns=["material_uid","Measurement","Probe_Resistance_ohm","Gas_Flow_Rate_L_per_min",\
        "Gas_Type", "Probe_Material", "Current_mA", "Field_Strength_T", "Sample_Position","Magnet_Reversal"])
    for filename in files:  #read from csv, and gathers all data uses Hall as the measurement
        with open(dirpath + '/' + filename) as afile:
            row=[]      
            for line in islice(afile, 2, None): #extract from the third row of the file for useful information
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
        if row[1]=='Hall':
            df_hall.loc[len(df_hall)]=row #only append to database if uses Hall measurement
    cursor=connection.cursor()
    engine = create_engine('postgresql://karen:passwordkaren@host.docker.internal:5432/postgres') #connects to postgres database
    df_hall.to_sql('lab_hall', engine,if_exists='replace',index=False)   #write the dataframe to psql database
    # cursor=connection.cursor()
    # cursor.execute("ALTER TABLE ONLY lab_hall ADD CONSTRAINT lab_hall_pkey PRIMARY KEY (material_uid);") #add primary key
    # cursor.execute("ALTER TABLE ONLY lab_hall \
    # ADD CONSTRAINT lab_hall_material_uid_fkey FOREIGN KEY (material_uid) REFERENCES hot_press(output_material_uid);") #add foreign key
    return df_hall.to_json() #returns json object of the dataframe



def read_from_txt_icp(ds, *args, **kwargs):
    '''
    A dag function that reads data from X-lab's measurements using ICP into a dataframe, and saves the dataframe to psql database
    Returns:
      The json object of the X lab's results measured by ICP.
    '''
    df_icp=pd.DataFrame(columns=['material_uid','Measurement','Pb_concentration','Sn_concentration','O_Concentration',\
        'Gas_Flow_Rate_L_per_min','Gas_Type','Plasma_Temperature_celsius','Detector_Temperature_celsius','Field_Strength_T',\
            'Plasma_Observation','Radio_Frequency_MHz'])
    for filename in files:
        with open(dirpath + '/' + filename) as afile:
            row=[] 
            for line in islice(afile, 2, None):#extract from the third row for useful information
                line_input = line.strip('" \n') 
                words=regex.sub(" ",line_input).split(' ')[-1]
                row.append(words) # adds the retrieved value to our row
        if row[1]=='ICP':
            df_icp.loc[len(df_icp)]=row   #only append to database if uses ICP measurement

    engine = create_engine('postgresql://karen:passwordkaren@host.docker.internal:5432/postgres')#connects to postgres database
    df_icp.to_sql('lab_icp', engine,if_exists='replace',index=False)   #write icp results to psql database
    # cursor=connection.cursor()
    # cursor.execute("ALTER TABLE ONLY lab_icp ADD CONSTRAINT lab_icp_pkey PRIMARY KEY (material_uid);") #add primary key
    # cursor.execute("ALTER TABLE ONLY lab_icp \
    # ADD CONSTRAINT lab_icp_material_uid_fkey FOREIGN KEY (material_uid) REFERENCES hot_press(output_material_uid);") #add foreign key
    # data_icp=cursor.fetchall()
    # df_icp_psql = pd.DataFrame(data_icp)
    # print(df_icp_psql)
    return df_icp.to_json()
 
 
def generate_master_csv(ds, *args, **kwargs):
    '''
    Combines dataframes from X-Processing, X-labs together and generates a master csv file with all data
    '''
    df_x_process=pd.read_json(read_from_psql(1))
    df_hall=pd.read_json( read_from_txt_hall(2))
    df_icp=pd.read_json( read_from_txt_icp(2))
 
    df1=pd.merge(df_x_process, df_hall, how='left', left_on='output_material_uid_y', right_on='material_uid')#join data from X-Lab and X-Processing together
    df2=pd.merge(df1, df_icp, how='left', left_on='output_material_uid_y', right_on='material_uid')
    df2.to_csv(r'/opt/airflow/logs/master_db1.csv', header='true', index=False) #export to master csv file

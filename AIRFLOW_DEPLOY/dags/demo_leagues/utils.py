import pandas as pd
import time
import random
import os
from datetime import datetime

def get_data(url,liga):

    tiempo = [5,3,8]
    time.sleep(random.choice(tiempo))
    df = pd.read_html(url)
    df=pd.concat([df[0],df[1]],ignore_index=True,axis=1)
    df=df.rename(columns={0:'EQUIPO',1:'J', 2:'G', 3:'E', 4:'P', 5:'GF', 6:'GC', 7:'DIF', 8:'PTS'})
    df['EQUIPO']=df['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric()==True else x[4:])
    df['LIGA'] = liga

    run_date = datetime.now()
    run_date = run_date.strftime("%Y-%m-%d")
    df['CREATED_AT'] = run_date

    return df

def data_processing(df):

    df_spain=get_data(df['URL'][0],df['LIGA'][0])
    df_premier=get_data(df['URL'][1],df['LIGA'][1])
    df_italy=get_data(df['URL'][2],df['LIGA'][2])
    df_germany=get_data(df['URL'][3],df['LIGA'][3])
    df_francia=get_data(df['URL'][4],df['LIGA'][4])
    df_portugal=get_data(df['URL'][5],df['LIGA'][5])
    df_holanda=get_data(df['URL'][6],df['LIGA'][6])

    df_final=pd.concat([df_spain,df_premier,df_italy,df_francia,df_portugal,df_holanda],ignore_index=False)

    return df_final

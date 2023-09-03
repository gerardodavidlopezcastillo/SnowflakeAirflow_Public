import pandas as pd 
import time
import random
import os
from utils import data_processing
from datetime import datetime
import uuid
import ssl

# Obtener la fecha actual en formato YYYY-MM-DD
current_date = time.strftime('%Y-%m-%d')

# Generar el nombre del archivo con la fecha actual
file_name = f'premier_positions_{current_date}.csv'

# Desactivamos el certificado SSL
ssl._create_default_https_context = ssl._create_unverified_context
print('Se desactiva el ceretificado SSL de la pagina para el SCRAPING')

# URLS a consultar
url = ['https://www.espn.com.co/futbol/posiciones/_/liga/esp.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/eng.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/ita.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/ger.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/fra.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/por.1',
      'https://www.espn.com.co/futbol/posiciones/_/liga/ned.1']
ligas = ['ESPAÑA','INGLATERRA','ITALIA','GERMANY','FRANCIA','PORTUGAL','HOLANDA']

# Asignamos los campos del DF
df_ligas = {
    'LIGA':ligas,
    'URL':url
} 

# Cargamos la tabla LIGA y URL en un DF
df_ligas = pd.DataFrame(df_ligas)
df = df_ligas
print('Se cargan los nombres de las ligas y las URLs')

# 
#df = pd.read_csv('/aws/DE-PRJCTS/SnowflakeAirflow/SnowflakeAirflow/AIRFLOW_DEPLOY/df_ligas.csv')

# Asignamos un valor unico para cada equipo, usando un codigo python (uuid) y lo guardamos en team_table
df_team = pd.read_csv('/aws/DE-PRJCTS/SnowflakeAirflow/SnowflakeAirflow/AIRFLOW_DEPLOY/team_table.csv')
print('Se carga tabla con IDS unicos para los equipos')

# Procesamos la informacion que tenemos
df_data = data_processing(df)
df_final = pd.merge(df_data,df_team,how='inner',on='EQUIPO')
df_final = df_final[['ID_TEAM','EQUIPO', 'J', 'G', 'E', 'P', 'GF', 'GC', 'DIF', 'PTS', 'LIGA', 'CREATED_AT']]
df_final.to_csv(f'./{file_name}',index=False)
print(f'Informacion SCRAPING procesada en archivo {file_name}')
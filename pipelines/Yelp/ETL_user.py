#############################################################
## Programa     :   ETL_user.py
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importar archivos json desde drive, realizar ETL y guardar resultado en Drive nuevamente
#############################################################

#Librerias a utilizar
import pandas as pd
import numpy as np
import fastparquet as fp
import re
import json

from pandas import json_normalize

# Leer archivo
data = fp.ParquetFile('user-002.parquet')

# Convertir a df
df = data.to_pandas()

# Eliminar las columnas que no se van a utilizar
df = df.drop(['compliment_hot','compliment_more'], axis=1)
df = df.drop(
    ['compliment_profile',
    'compliment_cute',
    'compliment_list',
    'compliment_note',
    'compliment_plain',
    'compliment_cool',
    'compliment_funny',
    'compliment_writer',
    'compliment_photos'], 
    axis=1)
df = df.drop(['elite','friends','fans'], axis=1)

# Eliminar la hora del campo "yelping_since"
df['yelping_since'] = df['yelping_since'].dt.date

# Pasar el campo "yelping_since" a fromato datetime
df['yelping_since'] = pd.to_datetime(df['yelping_since'])

# Guardar DF en un archivo JSON
df.to_json('user_yelp.json',orient='records', lines=True)

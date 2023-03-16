#############################################################
## Programa     :   ETL_tips.py
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importar archivos json desde drive, realizar ETL y guardar resultado en Drive nuevamente
#############################################################

# Librerias a utilizar
import pandas as pd
import numpy as np
import json
from pandas import json_normalize
import re

# Leer archivo
tips = pd.read_json('tip.json', lines=True)

# Eliminar la hora del campo "date"
tips['date'] = tips['date'].dt.date

# Eliminar el campo compliment_count, ya que no va a ser utilizado en el analisis
tips = tips.drop('compliment_count', axis=1)

# Eliminar caracteres especiales en la columna "text"
tips['text'] = tips['text'].apply(lambda x: re.sub('[^a-zA-Z0-9 \.\-\n]', '', x))

# Pasar el campo date a fromato datetime
tips['date'] = pd.to_datetime(tips['date'])

# Pasar todos los caracteres de text a minusculas
tips['text'] = tips['text'].str.lower()

# Guardar DF en un archivo JSON
tips.to_json('tips.json',orient='records', lines=True)
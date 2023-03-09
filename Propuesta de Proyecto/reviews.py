# Adaptar los códigos a un archivo .py

#Librerias a utilizar
import pandas as pd
import json

#Leer los archivo JSON en un DataFrame de Pandas
reviews_01 = pd.read_json('archivos reviews/1.json', lines=True)

reviews_02 = pd.read_json('archivos reviws/2.json', lines=True)

reviews_03 = pd.read_json('archivos reviws/3.json', lines=True)

reviews_04 = pd.read_json('archivos reviws/4.json', lines=True)

reviews_05 = pd.read_json('archivos reviws/5.json', lines=True)

reviews_06 = pd.read_json('archivos reviws/6.json', lines=True)

reviews_07 = pd.read_json('archivos reviws/7.json', lines=True)

reviews_08 = pd.read_json('archivos reviws/8.json', lines=True)

reviews_09 = pd.read_json('archivos reviws/9.json', lines=True)

reviews_10 = pd.read_json('archivos reviws/10.json', lines=True)

reviews_11 = pd.read_json('archivos reviws/11.json', lines=True)

reviews_12 = pd.read_json('archivos reviws/12.json', lines=True)

reviews_13 = pd.read_json('archivos reviws/13.json', lines=True)

reviews_14 = pd.read_json('archivos reviws/14.json', lines=True)

reviews_15 = pd.read_json('archivos reviws/15.json', lines=True)

reviews_16 = pd.read_json('archivos reviws/16.json', lines=True)

reviews_17 = pd.read_json('archivos reviws/17.json', lines=True)

reviews_18 = pd.read_json('archivos reviws/18.json', lines=True)

reviews_19 = pd.read_json('archivos reviws/19.json', lines=True)

# Concatenar los dataframes usando la función concat
reviews = pd.concat(
    [reviews_01, reviews_02, reviews_03, reviews_04, reviews_05, reviews_06, reviews_07, reviews_08, reviews_09, reviews_10, 
     reviews_11, reviews_12, reviews_13, reviews_14, reviews_15, reviews_16, reviews_17, reviews_18, reviews_19]
    )

# Vista
print(reviews.head())

# Columnas
print(reviews. columns)

# Total de datos foltrados por florida
print(reviews.shape)

# Obtener información sobre los tipos de datos, valores faltantes 
print(reviews.info())

# Estadísticas básicas de los datos
print(reviews.describe())

# Datos nulos de cada columna 
print(reviews.isnull().sum())

# Eliminar {} en la columna "resp"
reviews['resp'] = reviews['resp'].apply(lambda x: json.dumps(x))
reviews['resp'] = reviews['resp'].apply(lambda x: x.replace('{', '').replace('}', ''))

# Reemplazar las listas por cadena de caracteres
reviews['user_id'] = reviews['user_id'].apply(lambda x: str(x).strip('[]'))
reviews['name'] = reviews['name'].apply(lambda x: str(x).strip('[]'))
reviews['time'] = reviews['time'].apply(lambda x: str(x).strip('[]'))
reviews['rating'] = reviews['rating'].apply(lambda x: str(x).strip('[]'))
reviews['text'] = reviews['text'].apply(lambda x: str(x).strip('[]'))
reviews['pics'] = reviews['pics'].apply(lambda x: str(x).strip('[]'))
reviews['resp'] = reviews['resp'].apply(lambda x: str(x).strip('[]'))
reviews['gmap_id'] = reviews['gmap_id'].apply(lambda x: str(x).strip('[]'))

# Encunetro duplicados
print(reviews.duplicated().sum())
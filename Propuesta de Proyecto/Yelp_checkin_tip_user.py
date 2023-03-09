## Informe de Calidad de los datos proveniente de Yelp:
#"checkin" 

#Librerias a utilizar
import pandas as pd

# Abrir archivo JSON
checkin = pd.read_json('Yelp/checkin.json', lines=True)

# Tamaño del archivo
print(checkin.shape)

# Vista
print(checkin.head())

# Columnas
print(checkin.columns)

# Obtener información sobre los tipos de datos, valores faltantes 
print(checkin.info())

# Estadísticas básicas de los datos
print(checkin.describe())

# Datos nulos de cada columna 
print(checkin.isnull().sum())

# Encunetro duplicados
print(checkin.duplicated().sum())

# Abrir archivo JSON
tip = pd.read_json('Yelp/tip.json', lines=True)

# Tamaño del archivo
print(tip.shape)

# Vista
print(tip.head())

# Columnas
print(tip.columns)

# Obtener información sobre los tipos de datos, valores faltantes 
print(tip.info())

# Estadísticas básicas de los datos
print(tip.describe())

# Datos nulos de cada columna 
print(tip.isnull().sum())

# Encunetro duplicados
print(tip.duplicated().sum())

# Abrir archivo parquet
user = pd.read_parquet('Yelp/user.parquet')

# Tamaño del archivo
print(user.shape)

# Vista
print(user.head())

# Columnas
print(user.columns)

# Obtener información sobre los tipos de datos, valores faltantes 
print(user.info())

# Estadísticas básicas de los datos
print(user.describe())

# Datos nulos de cada columna 
print(user.isnull().sum())

# Encunetro duplicados
print(user.duplicated().sum())

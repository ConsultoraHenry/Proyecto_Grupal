# Adaptar los códigos a un archivo .py

# Libreria a utilizar
import pandas as pd

# Leer los archivos 
metadata_sitios_1 = pd.read_json('archivos metadata\Archivos metadata filtrados\1_6.json')
metadata_sitios_2 = pd.read_json('archivos metadata\Archivos metadata filtrados\7_11.json')

# Concatenacion de los archivos
metadata_sitios = pd.concat([metadata_sitios_1, metadata_sitios_2])

# Guardar el DataFrame en un archivo JSON
metadata_sitios.to_json('archivos metadata\Archivos metadata filtrados\metadata_sitios.json', orient='records')

# Imprimir los primeros 5 registros del DataFrame
print(metadata_sitios.head())

# Imprimir las columnas del DataFrame
print(metadata_sitios.columns)

# Imprimir el número de filas y columnas del DataFrame
print(metadata_sitios.shape)

# Obtener información sobre los tipos de datos, valores faltantes
print(metadata_sitios.info())

# Obtener estadísticas básicas de los datos
print(metadata_sitios.describe())

# Obtener el número de valores nulos por columna
print(metadata_sitios.isnull().sum())

# Reemplazar las listas por cadenas de caracteres
metadata_sitios['category'] = metadata_sitios['category'].apply(lambda x: str(x).strip('[]'))
metadata_sitios['hours'] = metadata_sitios['hours'].apply(lambda x: str(x).strip('[]'))
metadata_sitios['MISC'] = metadata_sitios['MISC'].apply(lambda x: str(x).strip('[]'))
metadata_sitios['relative_results'] = metadata_sitios['relative_results'].apply(lambda x: str(x).strip('[]'))

# Encontrar registros duplicados
print(metadata_sitios.duplicated().sum())
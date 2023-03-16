#############################################################
## Programa     :   reviewsYelp_extraccion-ETL-carga
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importa archivos json desde drive, realiza el ETL y sube al repositorio de google cloud
#############################################################

# Librerias a utilizar
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage

import pandas as pd
import re
import pyarrow as pa
import pyarrow.parquet as pq
import io

# Configurar las credenciales para acceder a las API de Google Drive y Google Cloud Storage
creds = service_account.Credentials.from_service_account_file('delorean-data-consulting-be19f9b979f1.json') 
drive_service = build('drive', 'v3', credentials=creds)

# Credenciales para acceder a google storage, no me dejó con las otras credenciales
storage_client = storage.Client.from_service_account_json('delorean-data-consulting-805bd95798d3.json')

# Crear una instancia del API de Drive
service = build('drive', 'v3', credentials=creds)

# El bucket destino en google cloud
bucket_name = 'yelp_bucket_henry'
bucket = storage_client.bucket(bucket_name)

# Consulta para listar los archivos de la carpeta especificada
folder_id = '1xYWHG_WpUZEyIdF3lXq3WzldpLXOFZII'
query = f"'{folder_id}' in parents and trashed = false and name = 'reviewsYelp.json'"
results = service.files().list(q=query, includeItemsFromAllDrives=True, spaces='drive', supportsAllDrives=True, fields="nextPageToken, files(id, name)").execute()

# Imprimir los nombres de los archivos 
print('opción 1 :',results)

# Obtener el archivo de Google Drive
file_bytes = io.BytesIO(drive_service.files().get_media(fileId=results['files'][0]['id']).execute())
print('file_bytes')

# Leer archivo
reviews_pri = pd.read_json(file_bytes)

# Cambiar tipos de datos, al correcto en cada columna
reviews_pri = reviews_pri.astype({'review_id': str, 'user_id': str, 'business_id': str, 'stars': int, 'useful': int, 'funny': int, 'cool': int, 'text': str})

# Cambia tipo de dato de la columna "date"
reviews_pri['date'] = pd.to_datetime(reviews_pri['date'])
reviews_pri['date'] = reviews_pri['date'].dt.date

# Eliminar dulicados
reviews_pri.drop_duplicates(inplace=True)

# Eliminar caracteres especiales
reviews_pri['text'] = reviews_pri['text'].apply(lambda x: re.sub('[^a-zA-Z0-9 \.\-\n]', '', x))
# Fin ETL

# Guardar el dataframe concatenado con el nombre del archivo, como control
reviews_pri.to_json('./datasets/reviewsYelp/'+final_fileName)

# Guardar el dataframe en el bucket de google storage
file_bytes = io.BytesIO(reviews_pri)
blob = bucket.blob(final_fileName)
blob.upload_from_file(file_bytes)

print('Trabajo realizado')
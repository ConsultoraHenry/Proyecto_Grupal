#############################################################
## Programa     :   metadataSitios_extraccion-carga
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importar archivos json desde drive y se carga al repositorio de google cloud
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
import json

# Configurar las credenciales para acceder a las API de Google Drive y Google Cloud Storage
creds = service_account.Credentials.from_service_account_file('delorean-data-consulting-be19f9b979f1.json') 
drive_service = build('drive', 'v3', credentials=creds)

# Credenciales para acceder a google storage
storage_client = storage.Client.from_service_account_json('delorean-data-consulting-805bd95798d3.json')

# Crear una instancia del API de Drive
service = build('drive', 'v3', credentials=creds)

# El bucket destino en google cloud
bucket_name = '	metadata_bucket1'
bucket = storage_client.bucket(bucket_name)

# Consulta para listar los archivos de la carpeta especificada
folder_id = '1JqUS2z6FU1384uGdOroBJAaJvoY-JOic'
query = f"'{folder_id}' in parents and trashed = false and name = 'metadata_sitios.json'"
results = service.files().list(q=query, includeItemsFromAllDrives=True, spaces='drive', supportsAllDrives=True, fields="nextPageToken, files(id, name)").execute()

# Imprimir los nombres de los archivos (No es necesario)
print('opción 1 :',results)

# Obtén el archivo de Google Drive
file_bytes = io.BytesIO(drive_service.files().get_media(fileId=results['files'][0]['id']).execute())

# Sube el archivo en formato JSON al bucket en Google Cloud Storage
final_file_name = results['files'][0]['name'].replace('.csv', '.json')
parquet_blob = bucket.blob(final_file_name)
parquet_blob.upload_from_file(file_bytes, content_type='application/octet-stream')

print('Trabajo realizado')
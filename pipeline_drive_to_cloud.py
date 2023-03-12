#############################################################
## Programa     :   pipeline_drive_to_cloud.py
## Fecha        :   Marzo 11; 2023
## Autor        :   grupo4
## Sinopsis     :   Importa archivos json desde drive al repositorio de google cloud
#############################################################
import io
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage


# Set up the credentials for accessing the Google Drive and Google Cloud Storage APIs
creds = service_account.Credentials.from_service_account_file('./assets/delorean-data-consulting-be19f9b979f1.json') 
drive_service = build('drive', 'v3', credentials=creds)
#Cree una nuevas credenciales para acceder a google storage, no me dejó con las otras credenciales
storage_client = storage.Client.from_service_account_json('./assets/delorean-data-consulting-805bd95798d3.json')

#configura el bucket destino en google cloud
bucket_name = 'metadata_bucket1'
bucket = storage_client.bucket(bucket_name)

# Crea una lista con los id, de las carpetas a importar
#aquí iria todo pero lo probé con esos dos
lstEstados = [  {'estado' : 'Florida'
               ,'googleDriveId':'1kxYcx3BjWNR2IVJ9odksaPRVOWzwQJts'
                }
                ,{'estado' : 'California'
               ,'googleDriveId':'1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll'
                }
            ]
#este ciclo recorre la lista de estados
for itemEstado in lstEstados:
    print(itemEstado.get('estado'))
    print(itemEstado.get('googleDriveId'))
    #consulta la carpeta en drive
    query = f"'{itemEstado.get('googleDriveId')}' in parents and trashed = false"
    results = drive_service.files().list(q=query, includeItemsFromAllDrives=True, spaces='drive', supportsAllDrives=True, fields="nextPageToken, files(id, name)").execute()
    
    #Este ciclo, recorre los archivos de la carpeta y los sube al bucket de la instancia de google cloud
    print('subiendo los archivos de la carpeta ',itemEstado.get('estado'),':')
    for item in results.get('files', []):
        print(itemEstado.get('estado')+item['name'])
        
        #asigna un nombre "compuesto" para no repetir los archivos
        final_fileName=itemEstado.get('estado')+item['name']
        #descarga el archivo y lo "entuba" para subirlo a storage
        file_bytes = io.BytesIO(drive_service.files().get_media(fileId=item['id']).execute())
        #sube el archivo a google could storage
        blob = bucket.blob(final_fileName)
        blob.upload_from_file(file_bytes)


print('Trabajo realizado')
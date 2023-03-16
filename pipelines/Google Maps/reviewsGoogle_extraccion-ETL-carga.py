#############################################################
## Programa     :   reviewsGoogle_extraccion-ETL-carga.py
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importa archivos json desde drive, realiza el ETL y sube al repositorio de google cloud
#############################################################

# Librerias a utilizar
import io
import os
import pandas as pd
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage

# Configurar las credenciales para acceder a las API de Google Drive y Google Cloud Storage
creds = service_account.Credentials.from_service_account_file('./assets/delorean-data-consulting-be19f9b979f1.json') 
drive_service = build('drive', 'v3', credentials=creds)

# Credenciales para acceder a google storage
storage_client = storage.Client.from_service_account_json('./assets/delorean-data-consulting-805bd95798d3.json')

# El bucket destino en google cloud
bucket_name = 'metadata_bucket1'
bucket = storage_client.bucket(bucket_name)

# Lista con los id, de las carpetas a importar
lstEstados = [  {'estado' : 'Florida'
               ,'googleDriveId':'1kxYcx3BjWNR2IVJ9odksaPRVOWzwQJts'
                }
                ,{'estado' : 'California'
               ,'googleDriveId':'1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll'
                }
                ,{'estado' : 'Nueva York'
               ,'googleDriveId':'18HYLDXcKg-cC1CT9vkRUCgea04cNpV33'
                }
                ,{'estado' : 'Texas'
               ,'googleDriveId':'1zq12pojMW2zeGgts0lHFSf_pF1L_4UWr'
                }
                ,{'estado' : 'Hawaii'
               ,'googleDriveId':'1IIAsDSlBiqQEdoN_n0E8a6TTAnrJMyWf'
                }
                ,{'estado' : 'Nevada'
               ,'googleDriveId':'1W8x6jX1u0fCvpSf0hPHK5rg5jftKEjXK'
                }
                ,{'estado' : 'Illinois'
               ,'googleDriveId':'1F8x0ymIgQInUCaqAxdkG89h2IQesNSEM'
                }
                ,{'estado' : 'Massachusetts'
               ,'googleDriveId':'1ORrUfBkvwJ4PiyxovgvWT2f1G9_y5r6c'
                }
                ,{'estado' : 'Nueva Jersey'
               ,'googleDriveId':'1jktC8qBJqIOBFf8ZNF7hmB66LHcqlvlG'
                }
                ,{'estado' : 'Georgia'
               ,'googleDriveId':'1MuPznes6CebS6gyWPVU-kR4EVKKLY4l3'
                }
            ]
lstEstados = [{'estado' : 'Texas'
               ,'googleDriveId':'1zq12pojMW2zeGgts0lHFSf_pF1L_4UWr'
                }]

# Recorre la lista de estados
for itemEstado in lstEstados:
    print(itemEstado.get('estado'))
    print(itemEstado.get('googleDriveId'))
    # Consulta la carpeta en drive
    query = f"'{itemEstado.get('googleDriveId')}' in parents and trashed = false"
    results = drive_service.files().list(q=query, includeItemsFromAllDrives=True, spaces='drive', supportsAllDrives=True, fields="nextPageToken, files(id, name)").execute()
    
    # Asigna un nombre "compuesto" para no repetir los archivos
    final_fileName=itemEstado.get('estado')+'_reviews.json'
    
    # Crea un dataframe vacío para ir concatenando estados
    dfReviewsEstado = pd.DataFrame()

    # Recorrer los archivos de la carpeta y los sube al bucket de la instancia de google cloud
    print('subiendo los archivos de la carpeta ',itemEstado.get('estado'),':')
    for item in results.get('files', []):
        print(itemEstado.get('estado')+item['name'])
        
        # Descarga el archivo y lo sube a storage
        file_bytes = io.BytesIO(drive_service.files().get_media(fileId=item['id']).execute())

        # Leer el archivo en un dataframe
        dfReviews = pd.read_json(file_bytes, lines=True)

        # Rellenar los nulos con "no data"
        dfReviews['text'].fillna('no data',inplace=True)

        # Eliminar caracteres especiales
        dfReviews['text'] = dfReviews['text'].apply(lambda x: re.sub('[^a-zA-Z0-9 \.\-\n]', '', x))

        # Eliminar columna "pics"
        dfReviews.drop('pics',inplace=True,axis=1)

        # Concatenar el dataframe
        dfReviewsEstado = pd.concat([dfReviewsEstado, dfReviews], axis=0,ignore_index=True)
    
    print(dfReviews.shape)

    # Guardar el dataframe concatenado con el nombre del archivo, como control
    dfReviewsEstado.to_json('reviews'+final_fileName, orient='records',lines=True)

    # Guardar el dataframe en el bucket de google storage
    file_bytes = io.BytesIO(dfReviewsEstado)
    blob = bucket.blob(final_fileName)
    blob.upload_from_file(file_bytes)
        
print('Trabajo realizado')
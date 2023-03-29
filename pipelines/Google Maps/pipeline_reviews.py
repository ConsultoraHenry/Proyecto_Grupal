#############################################################
## Programa     :   pipeline_reviews.py
## Fecha        :   Marzo 13; 2023
## Autor        :   grupo4
## Sinopsis     :   Importa archivos json desde drive a un dataset en google BigQuery
#############################################################
import apache_beam as beam
import logging
import json
import google.auth
import pandas as pd
import re
import argparse
from datetime import datetime

from apache_beam.io import ReadAllFromText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Parsea los paŕametros leídos
parser = argparse.ArgumentParser()
parser.add_argument(
    '--estado',
    dest='estado',
    required=True,
    help='Estado a descargar archivos JSON')
args = parser.parse_args()

#Lista de folders a leer
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

#busca en el diccionario el parámetros y lo deja en una variable global
estado= [x for x in lstEstados if x.get('estado') == args.estado]

#recupera la ultima fecha de carga
with open('./fechas/fechas.json') as file:
    dFechas = json.load(file)

ultimaFecha='2000-01-01'
for cadaEstado in dFechas['estados']:
    if cadaEstado['estado'] == estado[0].get('estado'):
        ultimaFecha = cadaEstado['fecha']

#si no existe el estado lo agrega
if ultimaFecha == '2000-01-01':
    #agrega el estado
    dFechas['estados'].append({'estado':estado[0].get('estado'),'fecha':ultimaFecha})

fechaMayor=ultimaFecha

print('configura el proyecto y el pipeline')
# Establece los nombres de proyecto y de dataset
PROJECT_ID = 'delorean-data-consulting'
BUCKET_NAME = 'gs://metadata_bucket1/'
FILE_NAME = BUCKET_NAME+estado[0].get('estado')+'_update'
DATASET_NAME = 'delorean_dataset'
TABLE_NAME = 'google_reviews_test'

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT_ID
#google_cloud_options.region = 'your-region'
google_cloud_options.job_name = 'importa-json-a-cloudStorage'
google_cloud_options.staging_location = BUCKET_NAME+'staging'
google_cloud_options.temp_location = BUCKET_NAME+'temp'
#options.view_as(GoogleCloudOptions).runner = 'DataflowRunner'

# Define the pipeline
pipeline = beam.Pipeline(options=options)

# establece las credenciales de la API de Google Drive esto es como público
creds = service_account.Credentials.from_service_account_file('./creds/delorean-data-consulting-be19f9b979f1.json')
drive_service = build('drive', 'v3', credentials=creds)

# Define la info de la tabla BigQuery
#table_spec = bigquery.TableReference(
#    projectId=PROJECT_ID,
#    datasetId=DATASET_NAME,
#    tableId=TABLE_NAME)

# Define the JSON file schema
#schema = bigquery.TableSchema()
#schema.fields.append(bigquery.TableFieldSchema(name='gmap_id', type='STRING'))
#schema.fields.append(bigquery.TableFieldSchema(name='rating', type='INTEGER'))
#schema.fields.append(bigquery.TableFieldSchema(name='text', type='STRING'))
#schema.fields.append(bigquery.TableFieldSchema(name='name', type='STRING'))
#schema.fields.append(bigquery.TableFieldSchema(name='user_id', type='FLOAT'))
#schema.fields.append(bigquery.TableFieldSchema(name='resp_time', type='TIMESTAMP'))
#schema.fields.append(bigquery.TableFieldSchema(name='resp_text', type='STRING'))

# Define función para recuperar los archivos de la carpeta compartida
def list_files(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, includeItemsFromAllDrives=True, spaces='drive', supportsAllDrives=True, fields="nextPageToken, files(id, name)").execute()
    for item in results.get('files', []):
        yield item

#esta función se necesitará para convertir el campo resp a campos individiales
def convierteJSON(texto):
    if texto == None:
        texto={'time':'','text':''}
    registro = dict(texto)
    return pd.Series([registro.get('time'),registro.get('text')])


# Esta función procesa los archivos con Pandas
def process_file(file):
    file_content = drive_service.files().get_media(fileId=file['id']).execute().decode('utf-8')
    dfReviews = pd.read_json(file_content, lines=True)
    #elimina registros sin fecha
    dfReviews = dfReviews[~dfReviews['time'].isna()]
    # Rellenar los nulos con "no data"
    dfReviews['text'].fillna('no data',inplace=True)
    # Eliminar caracteres especiales
    dfReviews['text'] = dfReviews['text'].apply(lambda x: re.sub('[^a-zA-Z0-9 \.\-\n]', '', x))
    #separa el campo resp
    dfReviews[['time_resp','text_resp']]=dfReviews['resp'].apply(convierteJSON)
    #convierte las columnas timestamp a fecha
    dfReviews['time'] = pd.to_datetime(dfReviews['time'], unit='ms')
    dfReviews['time_resp'] = pd.to_datetime(dfReviews['time_resp'], unit='ms')
    # Eliminar columnas "pics" y "resp"
    dfReviews.drop(['pics','resp'],inplace=True,axis=1)
    #actualiza el dataframe con los registros mayores a ultimafecha
    dfReviews=dfReviews[dfReviews['time']>=ultimaFecha]
    #actualiza ultimafecha
    global fechaMayor
    try:
        fecha = dfReviews['time'].max().strftime('%Y-%m-%d')
    except:
        fecha = fechaMayor
    
    if fecha > fechaMayor:
        fechaMayor = fecha
    #termina proceso
    return dfReviews

#Esta función actualiza la fecha mas reciente leída de los dataframe
def actualizaFecha(estado,fecha):
    for cadaEstado in dFechas['estados']:
        if cadaEstado['estado'] == estado:
            cadaEstado['fecha'] = fecha
    #guarda el archivo json
    with open('./fechas/fechas.json', 'w') as file:
        json.dump(dFechas, file, indent=4)
    return

# Define the pipeline steps

print(estado[0].get('estado'))
print(estado[0].get('googleDriveId'))
(pipeline
    | 'Lista archivos Drive' >> beam.Create(list_files(drive_service,estado[0].get('googleDriveId')))
    | 'Procesa Archivos' >> beam.Map(process_file)
    | 'Combina DataFrames' >> beam.CombineGlobally(lambda dfReviews: pd.concat(dfReviews))
    | 'Transforma Data' >> beam.Map(lambda dfReviews: dfReviews.to_dict('records'))
    | 'Escribe a GCS' >> beam.io.WriteToText(
            file_path_prefix=FILE_NAME,
            file_name_suffix='.json',
            shard_name_template='',
            num_shards=0)
    )

print('ejecuta el pipeline')
# Run the pipeline
result = pipeline.run()

#actualiza archivo de fechas
actualizaFecha(estado[0].get('estado'),fechaMayor)

#termina proceso
result.wait_until_finish()
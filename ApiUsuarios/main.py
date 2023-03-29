#importa
import pandas as pd
import numpy as np
import pickle
import os

from google.cloud import storage
from google.cloud import bigquery
from pydantic import BaseModel
from surprise import SVD
from surprise import Dataset
from surprise import Reader
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.neighbors import NearestNeighbors
from fastapi import FastAPI

app = FastAPI(title="API Recomendaciones", description="API del sistema de recomendación. DeLorean Data Consulting", version="1.0")
#creamos el modelo y lo dejamos global
modeloUsuarios = None


#proyecto
PROJECT =  'delorean-data-consulting'

#establece acciones a realizar al arrancar la api
@app.on_event("startup")
def load_model():
    global modeloUsuarios
    #Cree una nuevas credenciales para acceder a google storage, no me dejó con las otras credenciales
    storage_client = storage.Client.from_service_account_json('./creds/delorean-data-consulting-805bd95798d3.json')
    
    bucket = storage_client.bucket('metadata_bucket1')
    
    #usa esas mismas credencialespara hacer una conexión a bigquery
    bQClient = bigquery.Client.from_service_account_json('./creds/delorean-data-consulting-805bd95798d3.json')
        
    #ahora si lee el archivo para usuarios
    blob = bucket.blob('modelos/modelo_usuarios.pickle')
    pickle_in = blob.download_as_string()
    modeloUsuarios =  pickle.loads(pickle_in)

    
#realiza una consulta bigquery y se trae un data set rest u hoteles
def getUsuariosDataFrame(user_id : str):
    global PROJECT  
    qryUsrs = f"""
        select user_id
        ,gmap_id
        ,rating
        from delorean_dataset.reviews_definitivos_Google
        where user_id != {user_id}
        and gmap_id in (select distinct gmap_id from delorean_dataset.metadata_sitios
                      where State in (select distinct State from delorean_dataset.metadata_sitios
                                    where gmap_id in (select distinct gmap_id from delorean_dataset.reviews_definitivos_Google
                                                      where user_id = {user_id})
                                        and hotel1_restaurant0=0)
                    )
        and rating >=3
        order by rating desc; 
    """
    dfUsers = pd.read_gbq(qryUsrs, dialect='standard', project_id=PROJECT)
    print(dfUsers)
    return dfUsers

def getBusinessDataFrame(hotel1_restaurant0 : int, state : str, category : str):
    global PROJECT
    qryBusiness = f"""
    select gmap_id
        ,hotel1_restaurant0
        ,avg_rating
        ,num_of_reviews
        ,State
        ,city
        ,category
        ,avg_compound
        ,avg_conteoletras
    from delorean_dataset.metadata_sitios
    where hotel1_restaurant0 = {hotel1_restaurant0} and state = '{state}' and category='{category}'
    """
    print(qryBusiness)
    dfUsers = pd.read_gbq(qryBusiness, dialect='standard', project_id=PROJECT)
    return dfUsers

@app.get("/test")
async def root():
    return {"message": "La API está en línea"}

@app.get("/usr_predice_uno")
def modelo_usr_predice_uno(user_id : str, gmap_id:str):
    lstPrediccion = modeloUsuarios.predict(user_id,gmap_id)
    return lstPrediccion

@app.get("/usr_predice_varios")
def modelo_usr_predice_varios(user_id : str):
    # Get the top-N recommended movies for the user
    dfUsuarios = getUsuariosDataFrame(user_id)
    dfUsuarios['RatingEstimado'] = dfUsuarios['user_id'].apply(lambda x: modeloUsuarios.predict(user_id, x).est)
    dfUsuarios.sort_values('RatingEstimado', ascending = False, inplace=True)
    print(dfUsuarios)
    return dfUsuarios.head(10).to_json(orient='split')

@app.get("/cont_predice_uno")
def modelo_cont_predice_uno():
    x=0
    return {'mensaje':'En desarrollo'}

@app.get("/cont_predice_varios")
def modelo_cont_predice_(hotel1_restaurant0 : int, state : str, category : str):
    #Genera el dataframe
    dfContenido = getBusinessDataFrame(hotel1_restaurant0, state, category)
    # Create pipeline para variables numericas
    numeric_pipe = Pipeline([
    ('scaler', StandardScaler())
    ])
    # Crea pipeline para procesar  variables categoricas
    categorical_pipe = Pipeline([
    ('encoder', OneHotEncoder(drop = 'first'))
    ])
    # Transforma las columnas
    col_transf = ColumnTransformer([
    ('numeric', numeric_pipe, dfContenido._get_numeric_data().columns.tolist()),
    ('categoric', categorical_pipe, dfContenido.select_dtypes('object').columns.tolist()) 
    ])
    # Transforma las columnas
    col_transf = ColumnTransformer([
    ('numeric', numeric_pipe, dfContenido._get_numeric_data().columns.tolist()),
    ('categoric', categorical_pipe, dfContenido.select_dtypes('object').columns.tolist()) 
    ])
    # Algoritmo NearestNeighbors de Scikit-learn
    #Este parámetro especifica el número de vecinos más cercanos que se utilizarán para encontrar los puntos más cercanos a un punto de consulta
    nneighbors = NearestNeighbors(n_neighbors = 5, metric = 'cosine').fit(dfContenido)
    dif, ind = nneighbors.kneighbors(dfContenido[1])
    print(dfContenido.loc[ind[0][1:], :])
    return dfContenido.loc[ind[0][1:], :]

@app.get('/getBusiness')
async def getBusiness(gmap_id : str):
    global PROJECT 
    qryBusiness = f"""select name
    ,address
    ,category
    ,avg_rating
    ,State
    ,city
    ,url
    from delorean_dataset.metadata_sitios
    where gmap_id = '{gmap_id}'
    """
    dfBusiness = pd.read_gbq(qryBusiness, dialect='standard', project_id=PROJECT)
    print(dfBusiness)
    return dfBusiness   


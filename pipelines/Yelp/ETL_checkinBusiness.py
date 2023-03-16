#############################################################
## Programa     :   ETL_checkinBusiness
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importar archivos json desde drive, realizar ETL y guardar resultado en Drive nuevamente
#############################################################

# Librerias a utilizar
import pandas as pd
import json

# Leer archivo json línea por línea, armacenando cada objeto json en una lista
json_data = []
with open('checkin.json') as f:
    for line in f:
        json_data.append(json.loads(line))

# Conviertir la lista de objetos json en un dataframe de pandas
checkin = pd.concat([pd.json_normalize(data) for data in json_data], ignore_index=True)

business = pd.read_pickle('business.pkl')

# Eliminar columnas duplicadas
business = business.loc[:,~business.columns.duplicated()]

# Completar columna 'state'
business.at[0, 'state'] = 'CA'
business.at[1, 'state'] = 'MO'
business.at[2, 'state'] = 'AZ'

# Eliminar llaves en las columnas que las poseen
business['attributes'] = business['attributes'].astype(str)
business['attributes'] = business['attributes'].str.strip('{}')
business['hours'] = business['hours'].astype(str)
business['hours'] = business['hours'].str.strip('{}')

# Eliminar comillas
business['attributes'] = business['attributes'].str.replace(r"'", '', regex=True)
business['hours'] = business['hours'].str.replace(r"'", '', regex=True)

# Filtrar por los 10 estados que se van a utilizar
estados = ['FL', 'CA', 'NY', 'NV', 'HI', 'TX', 'MA', 'IL', 'NJ', 'GA']
business_filtrado = business[business['state'].isin(estados)]

# Concatenar los dos dataframes mediante 'business_id'
df_concat = pd.merge(business_filtrado, checkin, on='business_id', how='inner')

# Se pasa a formato json
df_concat.to_json('business_y_checkin.json',orient='records', lines=True)

print(df_concat)
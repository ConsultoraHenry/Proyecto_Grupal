#############################################################
## Programa     :   ETL_metadataSitios
## Fecha        :   Marzo 14; 2023
## Autor        :   grupo4
## Sinopsis     :   Importar archivos json desde drive, realizar ETL y guardar resultado en Drive nuevamente
#############################################################

# Librerias a utilizar
import pandas as pd
import numpy as np

# Diccionario de Estados
Dict_Filter = {
    "CA" : "California",
    "FL" : "Florida",
    "NV" : "Nevada",
    "TX" : "Texas",
    "NY" : "New York",
    "MA" : "Massachusetts",
    "IL" : "Illinois",
    "NJ" : "New Jersey",
    "GA" : "Georgia",
    "HI" : "Hawaii"
}

# Se crea un DF vacio
Df_T = pd.DataFrame()
for i in range(1,2): #12
    #Df_i = pd.read_json(f"{i}.json", lines=True)
    Df_i = pd.read_json(f"{i}.json", lines=True).drop(["price", "description", "state", "MISC"], axis=1)
    Df_i["State"] = "Unknow"
    
    #Rellena la columna "hours" con no especifica donde hay nulos        
    Df_i["address"].fillna("NA, Null ", inplace = True)
    Df_i.fillna("NA", inplace = True)
    

    Slice = [False] * len (Df_i.category)
    i1 = 0
    for i in Df_i.index:
        #if isinstance(Df_i.loc[i]["address"].split(','), list): #[-1].split(" ")
        try:
            #'''
            if (Df_i.loc[i]["address"].split(',')[-1].split(" ")[1] in Dict_Filter.keys()):
                #print(Df_i["category"][i])
                #Df_i.loc[i]["category"] = ",".join(Df_i["category"][i]).lower()            
                Df_i["category"][i] = ",".join(Df_i.loc[i]["category"]).lower()
                #Df_i["category"][i] = ",".join(Df_i["category"][i]).lower()                
                if ("restaurant" in Df_i.loc[i]["category"].split(' ')) or ("hotel" in Df_i.loc[i]["category"].split(' ')): # or ('Resort hotel' in Df_i["category"][i])
                    Df_i["State"][i] = Df_i.loc[i]["address"].split(',')[-1].split(" ")[1]
                    Df_i["relative_results"][i] = ",".join(Df_i.loc[i]["relative_results"])
                    Slice[i1] = True
            #'''
        except Exception:
            #e = sys.exc_info()[1]
            #print(e.args[0])
            Slice[i1] = False
            
            
        i1+=1
    Df_i = Df_i[Slice].reset_index(drop=True)    
            
    # Acá se guarda el df sub i en el df total
    Df_T = pd.concat([Df_T, Df_i])


# Rellenar los nulos de las columnas "hours" y "category" con "no espeficica"
Df_T["hours"].fillna("no especifica", inplace = True)    
Df_T["category"].fillna("no especifica", inplace = True)
    
# Eliminar caracteres especiales en todas las columnas
Df_T['name'] = Df_T['name'].apply(lambda x: str.replace('[^a-zA-Z0-9 \.\-\n]', '', x))
Df_T['address'] = Df_T['address'].apply(lambda x: str.replace('[^a-zA-Z0-9 \.\-\n]', '', x))
Df_T['gmap_id'] = Df_T['gmap_id'].apply(lambda x: str.replace('[^a-zA-Z0-9 \.\-\n]', '', x))
Df_T['url'] = Df_T['url'].apply(lambda x: str.replace('[^a-zA-Z0-9 \.\-\n]', '', x))

# Eliminar duplicados
Df_T = Df_T.drop_duplicates(["name"])
Df_T = Df_T.drop_duplicates(["address"])
Df_T = Df_T.drop_duplicates(["gmap_id"])
Df_T = Df_T.drop_duplicates(["url"])

# Cambiar de tipo de dato, al correcto
Df_T["name"] = Df_T["name"].astype(str)
Df_T["address"] = Df_T["address"].astype(str)
Df_T["gmap_id"] = Df_T["gmap_id"].astype(str)
Df_T["category"] = Df_T["category"].astype(str)
Df_T["hours"] = Df_T["hours"].astype(str)
Df_T["relative_results"] = Df_T["relative_results"].astype(str)

# Se pasa a formato json
Df_T.to_json('metadata_sitios.json',orient='records', lines=True)

print(Df_T)
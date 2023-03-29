import streamlit as st
import json
import pandas as pd
import numpy as np
import sklearn
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import LabelEncoder

#cargar los archivos json
reviews = pd.read_json('reviews_usuarios.json')
metadata = pd.read_json('metadata.json')
metadata = pd.read_json('metadata_contenido.json')

le = LabelEncoder()

metadata['type_encoded'] = le.fit_transform(metadata['type'])
metadata['address_encoded'] = le.fit_transform(metadata['address'])
metadata['hotel_restaurant_encoded'] = le.fit_transform(metadata['hotel_restaurant'])

#definir la interfaz basica de la pagina web
st.markdown('<h2 style="text-align: center;color: blue;font-size: 50px;">- Modelo de recomendación basado en usuarios -</h2>', unsafe_allow_html=True)

#solicitar al usuario su nombre y un nombre que le guste
name = st.text_input("Nombre de usuario", "Escribe aqui tu nombre")


if name:
    #buscar todas las reviews del usuario solicitado
    reviews_user = reviews.loc[reviews['name'] == name]


#buscar los lugares frecuentados por el usuario 
lugares_frecuentados = []
for review in reviews_user.itertuples():
    gmap_id = getattr(review, 'gmap_id')
    if not metadata.loc[metadata['gmap_id'] == gmap_id].empty:
        lugar = metadata.loc[metadata['gmap_id'] == gmap_id].iloc[0]
        #lugar = metadata.loc[metadata['gmap_id'] == gmap_id].iloc[0]
        lugares_frecuentados.append(lugar)


#mostrar los lugares frecuentados por el usuario (hasta 5)
st.title("Lugares frecuentados por {}:".format(name))
for lugar in lugares_frecuentados[:5]:
    st.write("🔷  `NOMBRE`: {}".format(lugar['name']))
    st.write("🔷  `DIRECCIÓN`: {}".format(lugar['address']))
    st.write("🔷  `CARACTERÍSTICA`: {}".format(lugar['characteristics']))
    st.write("🔷  `HOSPEDAJE O RESTAURANTE`: {}".format(lugar['hotel_restaurant']))
    st.write("🔷  `TIPO`: {}".format(lugar['type']))

# Creamos un contador de lugares recomendados
contador_lugares = 0

# Mostramos los primeros 5 lugares frecuentados por el usuario
st.header("Recomendación de lugares para {}".format(name))
for lugar in lugares_frecuentados[:5]:
    # Obtenemos la categoría del lugar actual
    lugar_categoria = lugar['characteristics']

# Creamos un contador de lugares recomendados
contador_lugares = 0

# Obtener todos los lugares excepto los lugares frecuentados por el usuario
lugares = metadata[~metadata['gmap_id'].isin(reviews_user['gmap_id'])]

# construir el modelo KNN con todos los lugares
features = ['address_encoded', 'hotel_restaurant_encoded', 'type_encoded', 'avg_rating']
modelo_knn = NearestNeighbors(metric='euclidean', algorithm='brute')
modelo_knn.fit(lugares[features])

# Mostramos los primeros 5 lugares recomendados para el usuario
#st.header("Recomendación de lugares para {}".format(name))
for lugar in lugares_frecuentados[:5]:
    # Obtenemos las características del lugar actual
    lugar_caracteristicas = lugar['characteristics']
    
    # Buscamos los lugares similares al lugar actual
    distancia, indices = modelo_knn.kneighbors([lugar[features]])
    
    # Mostramos los lugares similares al lugar actual, excepto los lugares frecuentados por el usuario
    for i in indices.flatten():
        recomendado = lugares.iloc[i]
        if contador_lugares >= 5:
            break
        st.write("🔷  `NOMBRE`: {}".format(recomendado['name']))
        st.write("🔷  `DIRECCIÓN`: {}".format(recomendado['address']))
        st.write("🔷  `CARACTERÍSTICA`: {}".format(recomendado['characteristics']))
        st.write("🔷  `HOSPEDAJE O RESTAURANTE`: {}".format(recomendado['hotel_restaurant']))
        st.write("🔷  `TIPO`: {}".format(recomendado['type']))
        contador_lugares += 1

st.markdown('<h2 style="text-align: center;color: blue">- Modelo de recomendación basado en contenido -</h2>', unsafe_allow_html=True)

#creamos los filtros para el usuario
#city = st.selectbox('City', metadata['city'].unique())
hotel_restaurant = st.selectbox('Hotel o Restaurante', metadata['hotel_restaurant'].unique())
State = st.selectbox('Estado', metadata['State'].unique())
avg_rating = st.slider('Rating', min_value = 1, max_value = 5, value = 4)


#creamos un botón para filtrar los datos
if st.button('Filtrar'):
#filtramos el dataframe segun los filtros del usuario
    df_filtered = metadata[(metadata['State'] == State) & (metadata['avg_rating'] >= avg_rating) & (metadata['hotel_restaurant'] == hotel_restaurant)]

#ordenamos el dataframe segun el avg_rating
    df_sorted = df_filtered.sort_values('avg_rating', ascending=False)

#seleccionamos los 5 primeros
    df_5 = df_sorted.head(5)

#mostramos al usuario los lugares
    st.subheader('Los 5 mejores lugares son:')
    st.write(df_5[['name', 'address', 'city', 'avg_rating', 'characteristics', 'url']])

#mostramos al usuario algunos reviews
    st.subheader('Reviews de los 5 mejores lugares son:')

    for gmap_id in df_5['gmap_id']:
        st.write(reviews.loc[reviews['gmap_id'] == gmap_id, ['date', 'text', 'rating']])

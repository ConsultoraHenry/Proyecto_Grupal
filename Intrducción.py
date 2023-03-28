import streamlit as st
import pandas as pd
import webbrowser
from PIL import Image

st.set_page_config(page_title="DeLeaoran Data Consulting", layout="wide")

col13,col14= st.columns([1,5])
with col13:
    st.write("")
with col14:    
    st.image('imagenes/imagen3.png',width=950)


row1_spacer1, row1_1, row1_spacer2, row1_2, row1_spacer3 = st.columns((.1, 0.8, .1, 2.3, .1))

with row1_1:
    st.markdown('<h2 style="text-align: justify; color: blue"></h2>', unsafe_allow_html=True)
    st.markdown('<h2 style="text-align: justify;color: blue"></h2>', unsafe_allow_html=True)
    st.markdown('<h2 style="text-align: center;color: blue">Sobre nosotros</h2>', unsafe_allow_html=True)
with row1_2:
    st.markdown('<h4 style="text-align: justify;color: black">¡Llevamos su negocio directo al futuro!. Somos una consultora dedicada al análisis de datos, la toma de decisiones a nivel empresarial, la automatización y transformación digital.</h4>', unsafe_allow_html=True)
    st.markdown('<h4 style="text-align: justify;color: black">Buscamos brindar las mejores soluciones basadas en datos para que tomes la elección mas óptima ajustada a tus preferencias.</h4>', unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

st.write("")
st.markdown('<h2 style="text-align: center;color: black">PROYECTO: Análisis de mercado en base a Google Maps y Yelp</h2>', unsafe_allow_html=True)
st.write('Realizamos un análisis exhaustivo utilizando datos de Google Maps y Yelp en Estados Unidos, efocado en los 10 estados con mayor turismo y en el mercado de hotelería y restaurantes. El uso de los datos de estas plataformas es de gran importancia para las empresas que desean obtener información sobre la ubicación geográfica de sus clientes y la percepción que tienen de sus productos o servicios. Estos datos proporcionan información valiosa sobre la cantidad de negocios cercanos, la cantidad de clientes potenciales y la calidad de los servicios que se ofrecen en la zona. En este informe, presentaremos un análisis detallado de los datos recopilados de estas plataformas. Discutiremos las principales conclusiones y recomendaciones basadas en nuestros hallazgos, y explicaremos cómo estas conclusiones pueden ayudar a mejorar la toma de decisiones estratégicas en el ámbito empresarial.',font_size=40)

st.markdown('<h2 style="text-align: center;color: black">Dos enfoques para generar valor:</h2>', unsafe_allow_html=True)
col4, col5= st.columns(2)

with col4:
    st.image("https://cdn.5dias.com.py/uploads/inversion-alternativa-1200x900.jpg")
    st.markdown('<h3 style="text-align: center ;color: blue">Análisis para inversión</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: center;color: black">Mediante un exhaustivo análisis de distintas fuentes de datos, te guiamos en tu búsqueda de oportunidades de inversión a nivel empresarial en rubros específicos.</h5>', unsafe_allow_html=True)

with col5:
    st.image("https://ayudaleyprotecciondatos.es/wp-content/uploads/2020/11/Machine-Learning-01.jpg",width=625)
    st.markdown('<h3 style="text-align: center;color: blue">Recomendación para usuario</h3>', unsafe_allow_html=True)
    st.markdown('<h5 style="text-align: center;color: black">Utilizando modelos de Machine Learning, generamos la recomendación más precisa posible para el usuario final que desea concurrir al lugar que mejor se adapte a sus preferencias.</h5>', unsafe_allow_html=True)

st.write("")
st.write("")

st.markdown('<h2 style="text-align: center;color: black">EQUIPO DE TRABAJO</h2>', unsafe_allow_html=True)
st.image('imagenes/imagen4.png',width=1190)
st.write("")

url = "https://github.com/ConsultoraHenry/Proyecto_Grupal"
if st.button("ℹ️ℹ️ 👉🏼👉🏼 Para más información sobre el proyecto y nuestro equipo de trabajo, presione aquí 👈🏼👈🏼 ℹ️ℹ️",  use_container_width=True):
    webbrowser.open_new_tab(url)

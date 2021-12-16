import streamlit as st
import pandas as pd
import psycopg2
import pandas as pds
import numpy as np
import pydeck as pdk
import plotly.express as px

st.set_page_config()

st.title('Airbnb vous expulse!')

def load_data(nrows=None):
    conn = psycopg2.connect(database='airbnb', user="airbnb", password="airbnb", host="127.0.0.1", port="5432")
    data = pd.read_sql_query(
        """
        select 
            search_area.name as search_area_name, room.*
        from 
            room
            inner join survey on survey.survey_id = room.survey_id
            inner join search_area on search_area.search_area_id = survey.search_area_id
        where 
            survey.survey_id = 3
        """
    ,con=conn)
    if nrows:
        data = data.head(nrows)
    data['license'].fillna("", inplace=True)
    data['has_license'] = data.license.str.len() > 6
    data['city'] = data.city.str.title().str.strip()
    return data

# Create a text element and let the reader know the data is loading.
data_load_state = st.text('Loading data...')
# Load 10,000 rows of data into the dataframe.
data = load_data()
# Notify the reader that the data was successfully loaded.
data_load_state.write("")
#if st.checkbox('Show raw data'):
#    st.subheader('Raw data')
#    st.write(data)

# Sidebar
city_choice = st.sidebar.multiselect('Choisissez une ville:', data.city.unique())
if city_choice:
    data = data[data.city.isin(city_choice)]

room_type_choice = st.sidebar.multiselect('Choisissez un type de logement:', data.room_type.unique())
if room_type_choice:
    data = data[data.room_type.isin(room_type_choice)]

license_choice = st.sidebar.multiselect('Avec numéro d\'enregistrement:', data.has_license.unique())
if license_choice:
    data = data[data.has_license.isin(license_choice)]

st.write("#")
st.write("## Tous les logements")
st.write("#")
# Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Logements", len(data))
col2.metric("Logements enregistrés", f"{len(data[data.has_license == True])} ({100 * len(data[data.has_license == True]) / len(data):.0f}%)")
col3.metric("Loueurs", len(data.host_id.unique()))

# Map
fig = px.scatter_mapbox(data, 
                        lat=data.latitude,
                        lon=data.longitude,
                        color=data.has_license,
                        hover_name=data.name,
                        hover_data={
                            'room_type':True, 
                            'reviews':True, 
                            'accommodates':True, 
                            'license':True,
                            'latitude':False,
                            'longitude':False,
                            'has_license': False
                            },
                        zoom=12,
                        size_max=1,
                        labels={
                            'has_license':'Avec N° Enregistrement', 
                            'license':'Num enregistrement',
                            'room_type':'Type de logement',
                            'reviews':'Nb Commentaires',
                            'accomodates':'Capacité'
                            },
                        height=500,
                        opacity=0.7,
                        color_discrete_map={True: 'darkseagreen', False:'lightcoral'},
                        category_orders={'has_license':[True, False]}
                        )

fig.update_layout(mapbox_accesstoken="pk.eyJ1IjoibWF6ZWx4IiwiYSI6ImNqOG9tODMzYzA1MnAydnBjZG5lYTR4bGwifQ.R7lZcLkJejwX4D4--1yMSA")

st.plotly_chart(fig, use_container_width=True)

host_room_nb = data.groupby('host_id').size().reset_index()
host_room_nb.columns = ['host_id', 'nb']
multi_rooms_treshold = st.sidebar.slider('Seuil multi-logements', 1, int(host_room_nb.nb.max()), 10)
multi_room_hosts = host_room_nb[host_room_nb.nb >= multi_rooms_treshold].host_id.unique()
data_multi_rooms = data[data.host_id.isin(multi_room_hosts)].sort_values(by="host_id", ascending=True)

st.write("#")
st.write(f"## Loueurs ayant plusieurs logements (>{multi_rooms_treshold})")
st.write("#")

# Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Logements", len(data_multi_rooms))
col2.metric("Logements enregistrés", f"{len(data_multi_rooms[data_multi_rooms.has_license == True])} ({100 * len(data_multi_rooms[data_multi_rooms.has_license == True]) / len(data_multi_rooms):.0f}%)")
col3.metric("Loueurs", len(data_multi_rooms.host_id.unique()))

# Map
fig_multi_rooms = px.scatter_mapbox(data_multi_rooms, 
                                    lat=data_multi_rooms.latitude,
                                    lon=data_multi_rooms.longitude,
                                    color=data_multi_rooms.has_license,
                                    hover_name=data_multi_rooms.name,
                                    hover_data={
                                        'room_type':True, 
                                        'reviews':True, 
                                        'accommodates':True, 
                                        'license':True,
                                        'latitude':False,
                                        'longitude':False,
                                        'has_license': False
                                        },
                                    zoom=12,
                                    size_max=1,
                                    labels={
                                        'has_license':'Avec N° Enregistrement', 
                                        'license':'Num enregistrement',
                                        'room_type':'Type de logement',
                                        'reviews':'Nb Commentaires',
                                        'accomodates':'Capacité'
                                        },
                                    height=500,
                                    opacity=0.7,
                                    color_discrete_map={True: 'darkseagreen', False:'lightcoral'},
                                    category_orders={'has_license':[True, False]}
                        )

fig_multi_rooms.update_layout(
    mapbox_accesstoken="pk.eyJ1IjoibWF6ZWx4IiwiYSI6ImNqOG9tODMzYzA1MnAydnBjZG5lYTR4bGwifQ.R7lZcLkJejwX4D4--1yMSA")


st.plotly_chart(fig_multi_rooms, use_container_width=True)

if st.checkbox('Voir les données'):
    st.subheader('Tous les loueurs')
    st.write(data)
    st.subheader('Loueurs multi-logements seulements')
    st.write(data_multi_rooms)
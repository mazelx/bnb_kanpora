from folium.map import FeatureGroup
import streamlit as st
from streamlit_folium import folium_static
import folium
from dataclasses import dataclass


import streamlit as st
import pandas as pd
import psycopg2
import pandas as pds
import numpy as np
import pydeck as pdk
import plotly.express as px

NB_COLS = 3
@dataclass
class RoomTypes():
    ENTIRE_APT:str = "Entire home/apt"
    PRIVATE_ROOM:str = "Private room"
    SHARED_ROOM:str = "Shared room"
    HOTEL_ROOM:str = "Hotel room"


st.set_page_config(layout='wide')

st.title('Alda - Observatoire des locations AirBNB - Biarritz Anglet Bayonne')
st.markdown("#")

@st.cache
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
raw_data = load_data()

with st.expander("Filtres", True):
    st.markdown("**Types de logement : **")
    room_type_stats = raw_data.room_type.value_counts()
    selected_room_types = []
    if st.checkbox(f'Appartement entier ({room_type_stats[RoomTypes.ENTIRE_APT]})', value=True):
        selected_room_types.append(RoomTypes.ENTIRE_APT)
    if st.checkbox(f'Chambre privée ({room_type_stats[RoomTypes.PRIVATE_ROOM]})', value=False):
        selected_room_types.append(RoomTypes.PRIVATE_ROOM)
    if st.checkbox(f'Chambre d\'hôtel ({room_type_stats[RoomTypes.HOTEL_ROOM]})', value=False):
        selected_room_types.append(RoomTypes.HOTEL_ROOM)
    if st.checkbox(f'Chambre partagé ({room_type_stats[RoomTypes.SHARED_ROOM]})', value=False):
        selected_room_types.append(RoomTypes.SHARED_ROOM)
    raw_data = raw_data[raw_data.room_type.isin(selected_room_types)]
    st.markdown("#")

    st.markdown("**Seuil des loueurs multi-logement : **")
    host_room_nb = raw_data.groupby('host_id').size().reset_index()
    host_room_nb.columns = ['host_id', 'nb']
    multi_rooms_treshold = st.slider('', 1, int(host_room_nb.nb.max()), 10)
    multi_room_hosts = host_room_nb[host_room_nb.nb >= multi_rooms_treshold].host_id.unique()
    data_multi_rooms = raw_data[raw_data.host_id.isin(multi_room_hosts)].sort_values(by="host_id", ascending=True)

    if st.checkbox('Afficher uniquement les loueurs multi-logement sur la carte'):
        data = data_multi_rooms
    else:
        data = raw_data

dfs = []
dfs.append(data[data.city.isin(['Biarritz'])])
dfs.append(data[data.city.isin(['Anglet'])])
dfs.append(data[data.city.isin(['Bayonne'])])

dfs_multi_rooms = []
dfs_multi_rooms.append(data_multi_rooms[data_multi_rooms.city.isin(['Biarritz'])])
dfs_multi_rooms.append(data_multi_rooms[data_multi_rooms.city.isin(['Anglet'])])
dfs_multi_rooms.append(data_multi_rooms[data_multi_rooms.city.isin(['Bayonne'])])

st.markdown("#")
st.markdown("""En 2020, la CAPB a pris une série de mesures pour tenter de réguler
            la prolifération des meublés touristiques. Sur certaines des 24 communes classées en zone tendue, 
            il est désormais nécessaire d’obtenir une autorisation temporaire de
            changement d’usage pour toutes les résidences secondaires utilisées
            comme meublés touristiques,permanents ou occasionnels. Et le
            nombre d’autorisations accordées est limité à 1 logement par propriétaire
            sur 8 communes et à 2 logements par propriétaire sur 6 autres.")
            """)

maps = list()
for i, col in enumerate(list(st.columns(NB_COLS))):
    with col:
        try: 
            st.markdown(f"# {dfs[i].city.values[0] or 'Aucune donnée'}")
        except:
            st.markdown("Aucune donnée")

# Maps
for i, col in enumerate(list(st.columns(NB_COLS))):
    with col:
        tileset = r'https://api.mapbox.com/styles/v1/mazelx/ckxag12zx9xzv15p51usukqmf/tiles/256/{z}/{x}/{y}@2x?access_token=pk.eyJ1IjoibWF6ZWx4IiwiYSI6ImNqOG9tODMzYzA1MnAydnBjZG5lYTR4bGwifQ.R7lZcLkJejwX4D4--1yMSA'

        map = folium.Map(tiles=tileset, attr='<a href="https://www.mapbox.com/map-feedback/" target="_blank">Improve this map</a>',
                        location=[dfs[i].latitude.mean(),dfs[i].longitude.mean()],
                        zoom_start=13,
        )
        feature_group = folium.FeatureGroup("Locations")
        
        for index, row in dfs[i].iterrows():
            color = '#9ad9a3' if row.has_license else '#e86b6b'
            feature_group.add_child(
                folium.CircleMarker(
                    location=[row.latitude,row.longitude],
                    radius=1,
                    tooltip=row['name'],
                    popup=f'''
                            <b>{row['name']}</b><br>
                            {row['accommodates']} personnes<br>
                            {'Enregistré (' + row['license'] + ')' if row['has_license'] else 'Non enregistré'}<br>
                            <a href="https://www.airbnb.com/rooms/{row.room_id}" target="_blank" rel="noopener noreferrer">www.airbnb.com/rooms/{row.room_id}</a><br>
                            <a href="https://www.airbnb.com/users/show/{row.host_id}" target="_blank" rel="noopener noreferrer">www.airbnb.com/users/show/{row.host_id}</a>
                           ''',
                    fill=True,
                    color=color,
                    fill_color=color,
                )
            )
        map.add_child(feature_group)
        maps.append(map)

st.markdown("#")
data_load_state = st.text('Loading data...')

# Metrics
for i, col in enumerate(list(st.columns(NB_COLS))):
    df = dfs[i]
    df_multi_rooms = dfs_multi_rooms[i]
    with col:
        folium_static(maps[i], 300, 300)
        st.markdown("#")
        st.metric(label="Capacité (Nombre de lits)", value=df.accommodates.sum(), delta="+XX% Y-1")
        st.metric(label="Logements", value=len(df), delta="+XX% Y-1", delta_color="inverse")
        st.metric(label="Loueurs", value=len(df.host_id.unique()), delta="+XX% Y-1")
        st.metric(label="Taux d'enregistrement", value=f"{100 * len(df[df.has_license == True]) / len(df):.0f}%", delta="+XX% Y-1", delta_color="off")
        st.metric(label="Loueurs multi-logement", value=len(df_multi_rooms.host_id.unique()), delta="+XX% Y-1")
        

data_load_state.write("")

st.markdown("#")
st.markdown("#")
st.markdown("#")
if st.checkbox('Voir les données'):
    st.write(data)

 
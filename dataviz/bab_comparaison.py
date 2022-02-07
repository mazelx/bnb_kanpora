from pandas.core.frame import DataFrame

import streamlit as st
from streamlit_folium import folium_static
import folium
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import s3fs

NB_COLS = 3
COLOR_GREEN = '#9ad9a3'
COLOR_RED = '#e86b6b'
CITIES = ['Biarritz', 'Anglet', 'Bayonne']
#PATH = f's3://kanpora-data/bab/rooms_current.csv'
PATH = f's3://kanpora-data/rooms_8.csv'

@dataclass
class RoomTypes():
    ENTIRE_APT:str = "Logement entier"
    PRIVATE_ROOM:str = "Chambre privée"
    SHARED_ROOM:str = "Chambre partagée"
    HOTEL_ROOM:str = "Chambre d'hôtel"

st.set_page_config(layout='wide')

@st.cache(ttl=660)
def load_data(nrows=None):
    data = pd.read_csv(PATH)
    if nrows:
        data = data.head(nrows)
    data['license'].fillna("", inplace=True)
    data['has_license'] = data.license.str.len() > 6
    data['city'] = data.city.str.title().str.strip()
    data = data[data.city.isin(CITIES)]
    return data

# Create a text element and let the reader know the data is loading.
raw_data = DataFrame()
try:
    raw_data = load_data()
except FileNotFoundError:
    st.write(f"Source file not found {PATH}")

st.title('Observatoire des locations AirBNB')
if(len(raw_data) <1):
    st.subheader("Données indisponibles")

else:
    st.subheader(raw_data.search_area_name.max())
    st.markdown("#")

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

    # Should be groupby
    dfs = []
    dfs_multi_rooms = []
    for city in CITIES:
        dfs.append(data[data.city.isin([city])])
        dfs_multi_rooms.append(data_multi_rooms[data_multi_rooms.city.isin([city])])

    maps = list()
    for i, col in enumerate(list(st.columns(NB_COLS))):
        with col:
            try: 
                st.markdown(f"# {dfs[i].city.values[0] or 'Aucune donnée'}")
            except:
                st.markdown("Aucune donnée")

    # Maps
    tileset = r'https://api.mapbox.com/styles/v1/mazelx/ckxag12zx9xzv15p51usukqmf/tiles/256/{z}/{x}/{y}@2x?access_token=pk.eyJ1IjoibWF6ZWx4IiwiYSI6ImNqOG9tODMzYzA1MnAydnBjZG5lYTR4bGwifQ.R7lZcLkJejwX4D4--1yMSA'
    for i, col in enumerate(list(st.columns(NB_COLS))):
        with col:
            if len(dfs[i])>0:
                map = folium.Map(tiles=tileset, attr='<a href="https://www.mapbox.com/map-feedback/" target="_blank">Improve this map</a>',
                                location=[dfs[i].latitude.mean(),dfs[i].longitude.mean()],
                                zoom_start=13,
                )
                feature_group = folium.FeatureGroup("Locations")
                
                for index, row in dfs[i].iterrows():
                    color = COLOR_GREEN if row.has_license else COLOR_RED
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
    st.markdown("""En 2020, la CAPB a pris une série de mesures pour tenter de réguler
                la prolifération des meublés touristiques. Sur certaines des 24 communes classées en zone tendue, 
                il est désormais nécessaire d’obtenir une autorisation temporaire de
                changement d’usage pour toutes les résidences secondaires utilisées
                comme meublés touristiques,permanents ou occasionnels. Et le
                nombre d’autorisations accordées est limité à 1 logement par propriétaire
                sur 8 communes et à 2 logements par propriétaire sur 6 autres.
                """)
    st.markdown(f"""La carte ci-dessous présente les annonces AirBNB disposant d'un numéro d'enregistrement 
                (<span style=\"color:{COLOR_GREEN}\">en vert</span>) et celles ne disposant pas de numéro d'enregistrement
                (<span style=\"color:{COLOR_RED}\">en rouge</span>).""", unsafe_allow_html=True)
    st.markdown("""*nb: La carte affiche sans distinction les résidences principales ou secondaires, loués par des particuliers 
                ou des professionnels*""")
    st.markdown("#")
    st.markdown("#")
    data_load_state = st.text('Chargement des données...')
    # Render columns
    for i, col in enumerate(list(st.columns(NB_COLS))):
        df = dfs[i]
        df_multi_rooms = dfs_multi_rooms[i]
        with col:
            if len(dfs[i])>0:
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
    if st.checkbox('Voir les données'):
        st.write(data)

    
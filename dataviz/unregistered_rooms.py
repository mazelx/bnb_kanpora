from pandas.core.frame import DataFrame

import streamlit as st
from streamlit_folium import folium_static
import folium
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import s3fs

COLOR_GREEN = '#9ad9a3'
COLOR_RED = '#e86b6b'
COLOR_GRAY = '#495057'
CITIES = ['Biarritz', 'Anglet', 'Bayonne', 'Guéthary', 'Bidart', 'Ciboure', 
          'Cambo-Les-Bains', 'Urrugne', 'Saint-Jean-De-Luz', 'Ascain', 'Hendaye',
          'Ustaritz', 'Arcangues']
#PATH = f's3://kanpora-data/bab/rooms_current.csv'
PATH = f's3://kanpora-data/rooms_9.csv'
MOBILITY_LICENSE = ['Disponible uniquement via un bail mobilité', 'Available with a mobility lease only ("bail mobilité")']
HOTEL_LICENSE = ['Exempt - hotel-type listing', 'Dispense : logement de type hôtelier', 'Dispense : logement de type hôtelier']
SURVEY_ID = 9

@dataclass
class RoomTypes():
    ENTIRE_APT:str = "Logement entier"
    PRIVATE_ROOM:str = "Chambre privée"
    SHARED_ROOM:str = "Chambre partagée"
    HOTEL_ROOM:str = "Chambre d'hôtel"

#st.set_page_config(layout='wide')
def make_clickable(link, text):
        # target _blank to open new window
        # extract clickable text to display for your link
        return f'<a target="_blank" href="{link}">{text}</a>'

selected_rooms = []

@st.cache(ttl=660)
def load_data(nrows=None):
    data = pd.read_csv(PATH)
    if nrows:
        data = data.head(nrows)
    data['license'].fillna("", inplace=True)
    data['has_license'] = (data.license.str.contains('64') & (data.license.str.len() > 6) | (data.license.isin(MOBILITY_LICENSE + HOTEL_LICENSE)))
    data['city'] = data.city.str.title().str.strip()
    data['room_url'] = data.apply(lambda row: make_clickable('https://www.airbnb.com/rooms/' + str(row.room_id), str(row.room_id)), axis=1)
    data['host_url'] = data.apply(lambda row: make_clickable('https://www.airbnb.com/users/show/' + str(row.host_id), str(row.host_id)), axis=1)
    data = data[data.survey_id == SURVEY_ID]
    data = pd.merge(
        data,
        data.groupby('host_id').agg(host_nb_rooms=pd.NamedAgg('room_id', 'count')).reset_index(drop=False),
        on=['host_id']
    )
    data = pd.merge(
        data,
        data[data.has_license == False].groupby('host_id').agg(host_nb_rooms_unreg=pd.NamedAgg('room_id', 'count')).reset_index(drop=False),
        how='left',
        on=['host_id']
    )
    data['host_nb_rooms_unreg'] = data.host_nb_rooms_unreg.fillna(0).astype(int)
    data['host_pct_unreg'] = (100 * ((data.host_nb_rooms - data.host_nb_rooms_unreg) / data.host_nb_rooms)).astype(int)
    data['nb_reservations'] = data['reviews'].fillna(0).astype(int) * 2
    data['accommodates'] = data['accommodates'].fillna(0).astype(int)
    return data

# Create a text element and let the reader know the data is loading.
raw_data = DataFrame()
try:
    raw_data = load_data()
    
except FileNotFoundError:
    st.write(f"Source file not found {PATH}")

st.title("Annonces non enregistrées")
if(len(raw_data) <1):
    st.subheader("Données indisponibles")

else:
    st.markdown("#")
    st.markdown("#")
    st.markdown("## Résumé par ville")
    for city in sorted(CITIES):
        _df = raw_data[raw_data.city == city]
        cols = st.columns(5)
        cols[0].markdown(f"### {city}")
        cols[1].metric(label="Capacité", value=_df.accommodates.sum())
        cols[2].metric(label="Logements", value=len(_df))
        cols[3].metric(label="Loueurs", value=len(_df.host_id.unique()))
        cols[4].metric(label="Enregistrement", value=f"{100 * len(_df[_df.has_license == True]) / (len(_df) + 0.1):.0f}%")
            
    st.markdown("#")
    st.markdown("#")
    st.markdown("## Selection des annonces")
    st.markdown("Choissisez une annonce pour la mettre en evidence sur la carte et la faire apparaitre dans la selection (en bas de la page)")

    selected_cities = st.multiselect(
        'Ville:',
        CITIES, 
        CITIES)
    
    df_city_unreg = raw_data[(raw_data.city.isin(selected_cities)) & (raw_data.has_license == False)]

    # COMPONENT : Search
    selected_rooms = st.multiselect(
        'Annonces à mettre en évidence',
        df_city_unreg.room_id.to_list(),
    )

    selected_hosts = st.multiselect(
        'Loueurs à mettre en évidence',
        df_city_unreg.host_id.unique(),
    )

    # COMPONENT : Map
    tileset = r'https://api.mapbox.com/styles/v1/mazelx/ckxag12zx9xzv15p51usukqmf/tiles/256/{z}/{x}/{y}@2x?access_token=pk.eyJ1IjoibWF6ZWx4IiwiYSI6ImNqOG9tODMzYzA1MnAydnBjZG5lYTR4bGwifQ.R7lZcLkJejwX4D4--1yMSA'

    map = folium.Map(tiles=tileset, attr='<a href="https://www.mapbox.com/map-feedback/" target="_blank">Improve this map</a>',
                    location=[df_city_unreg.latitude.mean(), df_city_unreg.longitude.mean()],
                    zoom_start=13,
    )
    feature_group = folium.FeatureGroup("Locations")
    
    for index, row in df_city_unreg.iterrows():
        color = COLOR_RED if (row.room_id in selected_rooms or row.host_id in selected_hosts) else COLOR_GRAY
        feature_group.add_child(
            folium.CircleMarker(
                location=[row.latitude,row.longitude],
                radius=1,
                tooltip=f'''
                        <b>{row['room_id']}: {row['name']}</b><br>
                        {row['accommodates']} personne(s)<br>
                        {row['host_nb_rooms_unreg']} annonce(s) du même loueur<br>
                        {'Num enregistrement incorrect (' + row['license'] + ')' if row['license'] else "Sans num d' enregistrement"}<br>
                    ''',
                popup=f'''
                        <b>{row['room_id']}: {row['name']}</b><br>
                        {row['accommodates']} personne(s)<br>
                        {row['host_nb_rooms_unreg']} annonce(s) du même loueur<br>
                        {'Num enregistrement incorrect (' + row['license'] + ')' if row['license'] else "Sans num d' enregistrement"}<br>
                        <a href="https://www.airbnb.com/rooms/{row.room_id}" target="_blank" rel="noopener noreferrer">www.airbnb.com/rooms/{row.room_id}</a><br>
                        <a href="https://www.airbnb.com/users/show/{row.host_id}" target="_blank" rel="noopener noreferrer">www.airbnb.com/users/show/{row.host_id}</a>
                        <img src={row['picture_url']} width=200>
                    ''',
                fill=True,
                color=color,
                fill_color=color,
            ) 
        )
    map.add_child(feature_group)
    folium_static(map)

    # COMPONENT : Table
    st.markdown("#")
    st.markdown("## Toutes les annonces")
    st.markdown("Cliquer sur le titre de colonne pour changer le tri des annonces")
    df_city_unreg = df_city_unreg[['city', 'room_id','host_id', 'name', 'license', 'nb_reservations', 'rate', 'host_nb_rooms', 'host_nb_rooms_unreg', 'host_pct_unreg']].sort_values('nb_reservations', ascending=False).reset_index(drop=True)
    df_city_unreg = df_city_unreg.rename(columns = {
        'city': 'ville',
        'room_id' : 'annonce',
        'host_id' : 'loueur',
        'name' : 'titre',
        'license' : 'num enregistrement',
        'nb_reservations' : 'nb reservations (estim)',
        'rate' : 'prix',
        'host_nb_rooms' : "nb annonces du loueur",
        'host_nb_rooms_unreg' : 'nb annonces du loueur sans enreg',
        'host_pct_unreg' : "% enregistrement de l'hote"
        })
    st.dataframe(data=df_city_unreg)
    
    st.markdown("#")
    st.markdown("## Annonces selectionnées")
    for key, row in df_city_unreg[(df_city_unreg.annonce.isin(selected_rooms)) | (df_city_unreg.loueur.isin(selected_hosts)) ].iterrows():
        st.write(
            f'<a href="https://www.airbnb.com/rooms/{row.annonce}" target="_blank" rel="noopener noreferrer">www.airbnb.com/rooms/{row.annonce}</a>',
            unsafe_allow_html=True)

    
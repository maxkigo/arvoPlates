from nltk.sem.chat80 import borders
from pymongo import MongoClient
import pandas as pd
import streamlit as st
import plotly as pt
import base64
import plotly.express as px
import plotly.graph_objs as go
from PIL import Image
from io import BytesIO
import requests
import pymysql
import paramiko
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
from os.path import expanduser
import tempfile
import os
import datetime

st.set_page_config(layout="wide", page_title="Kigo Verification Beta", page_icon="decorations/kigo-icon-adaptative.png")

st.markdown(
    """
    <div style="text-align: center;">
        <img src="https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png" 
        alt="Imagen al inicio" style="width: 25%; max-width: 30%; height: auto;">
    </div>
    """,
    unsafe_allow_html=True
)

pem_key = st.secrets['pem']['private_key']

# Configuration and Env of MongoDB
mongo_config = {
    "uri": st.secrets["mongo_credentials"]["uri"],
    "database": st.secrets["mongo_credentials"]["database"],
    "collection": st.secrets["mongo_credentials"]["collection"]
}

# Connection to MongoDB
def connect_to_mongo():
    try:
        client = MongoClient(mongo_config["uri"])
        print("Conexión a MongoDB exitosa.")
        return client[mongo_config["database"]]
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return None

# Mongo data fecht
def get_mongo_data():
    db = connect_to_mongo()
    if db is not None:  # Cambiar la verificación a comparación explícita con None
        collection = db[mongo_config["collection"]]

        # Pipeline de la consulta agregada
        pipeline = [
            {
                "$project": {
                    "_id": True,
                    "timestamp": "$timestamp",
                    "sgReturnVehicleLocationData.timeStamp": "$timeStamp",
                    "sgSpeedMs": "$sgReturnVehicleLocationData.sgSpeedMs",
                    "latitude": "$sgReturnVehicleLocationData.gpsDataVehicle.latitude",
                    "longitude": "$sgReturnVehicleLocationData.gpsDataVehicle.longitude",
                    "license": "$sgReturnVehicleLocationData.license",
                    "activeAnprCameras": "$sgReturnVehicleLocationData.activeAnprCameras",
                    "confidence": "$sgReturnVehicleLocationData.confidence",
                    "vehicleSpeedMs": "$sgReturnVehicleLocationData.vehicleSpeedMs",
                    "country": "$sgReturnVehicleLocationData.country",
                    "image0Url": {"$arrayElemAt": ["$sgReturnVehicleLocationData.images.image.url", 0]},
                    "image1Url": {"$arrayElemAt": ["$sgReturnVehicleLocationData.images.image.url", 1]},
                    "image2Url": {"$arrayElemAt": ["$sgReturnVehicleLocationData.images.image.url", 2]},
                    "image3Url": {"$arrayElemAt": ["$sgReturnVehicleLocationData.images.image.url", 3]},
                    "image4Url": {"$arrayElemAt": ["$sgReturnVehicleLocationData.images.image.url", 4]}
                }
            },
            {"$sort": {"timestamp": -1}},
            {"$limit": 100000}
        ]

        # Ejecutar la consulta agregada
        try:
            result = list(collection.aggregate(pipeline))
            df = pd.DataFrame(result)  # Convertir el resultado a un DataFrame
            print("Datos obtenidos de MongoDB exitosamente.")
            return df
        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")
            return None
    else:
        print("No se pudo conectar a la base de datos.")
    return None

# Procesar DataFrame
def process_mongo_dataframe(df):
    if df is not None and not df.empty:
        # Convertir columnas relevantes a tipos adecuados
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df.fillna('', inplace=True)

        # Renombrar columnas para mayor claridad
        df.rename(columns={
            "_id": "id",
            "sgSpeedMs": "speed",
            "license": "vehicle_license"
        }, inplace=True)
        return df
    else:
        print("El DataFrame está vacío o no se generó correctamente.")
        return df


arvoResponse = get_mongo_data()

@st.cache_data
def get_mongo_data_processed():
    arvoResponse = get_mongo_data()
    processed_data = process_mongo_dataframe(arvoResponse)
    return processed_data

# Connection to aurora
# Now we can use the key directly, no need to decode it from base64 or modify it
with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_key_file:
    temp_key_file.write(pem_key)
    temp_key_file_path = temp_key_file.name

try:
    # Load the private key from the temporary file
    mypkey = paramiko.RSAKey.from_private_key_file(temp_key_file_path)
    print("Private key loaded successfully.")
finally:
    # Clean up the temporary file after loading the private key
    os.remove(temp_key_file_path)

# Env varibles for the creation of the ssh tunnel
sql_hostname = st.secrets["database"]["sql_hostname"]
sql_username = st.secrets["database"]["sql_username"]
sql_password = st.secrets["database"]["sql_password"]
sql_main_database = st.secrets["database"]["sql_main_database"]
sql_port = st.secrets["database"]["sql_port"]
ssh_host = st.secrets["ssh"]["ssh_host"]
ssh_user = st.secrets["ssh"]["ssh_user"]
ssh_port = st.secrets["ssh"]["ssh_port"]

# Locations of PV-Kigo
locations = [
    "AHOME",
    "Chignahuapan",
    "Chihuahua",
    "Corregidora",
    "Durango",
    "Guadalajara",
    "Hermosillo",
    "Huamantla",
    "Ibarra",
    "La Chorrera",
    "Mazatlán",
    "Mineral de Reforma",
    "Monterrey Infracción Digital",
    "Nogales",
    "Orizaba",
    "Orizaba(grua)",
    "Prueba ID",
    "Puebla",
    "Puebla ID Pruebas",
    "San Martín Texmelucan",
    "San Martín Texmelucan Transito",
    "San Nicolas de los Garza",
    "San Pedro Cholula",
    "San Pedro Garza García",
    "San Pedro Garza García ID",
    "TEST Puebla - La Chorrera",
    "Teotihuacan",
    "Tepeji del Río",
    "Tlaxcala Feria",
    "Tlaxcala ID",
    "Toluca",
    "Tonalá",
    "Torreon",
    "Tula",
    "Tuxtla Gutiérrez ",
    "Zacapoaxtla",
    "Zacapoaxtla Infracción Digital",
    "Zacatlán",
    "Zapopan",
    "Zaragoza"
]

#Selection of the date to fetch data
# Create two columns: left and right
col1, col2 = st.columns(2)

with col1:
    d = st.date_input("Date", value=datetime.date(2024, 12, 23))

with col2:
    location_selected = st.selectbox('Project:', locations, index=locations.index('Zacatlán') if 'Zacatlán' in locations else 0)


# Access to MongoDb
mongo_df = get_mongo_data()

# Mongo database filter to user only data from the selected date
if mongo_df is not None:
    mongo_df = process_mongo_dataframe(mongo_df)
    mongo_df['date_lecture'] = mongo_df['timestamp'].dt.date
    print(mongo_df.head())
    if d is not None:
        filtered_df = mongo_df[mongo_df["date_lecture"] == d]
    else:
        st.write("No date selected.")
else:
    print("No se obtuvieron datos de MongoDB.")

b = ', '.join(f"'{value}'" for value in filtered_df['vehicle_license'].to_list())

# Auror Connection and fetching data
@st.cache_data
def df_aurora_fetch(location_selected, b, d):
    query = f'''SELECT T.paidminutes, T.date, T.expires, T.licenseplate, Z.name
            FROM CARGOMOVIL_PD.PKM_TRANSACTION T
            JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z
            ON T.zoneid = Z.id
            WHERE Z.name LIKE '%{location_selected}%'
            AND DATE(T.date) = '{d}'
            AND T.licenseplate IN ({b})
            ORDER BY T.date DESC;'''

    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
             ssh_username=ssh_user,
             ssh_pkey=mypkey,
         remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                           passwd=sql_password, db=sql_main_database,
                           port=tunnel.local_bind_port)
        queryf = query
        data_croce = pd.read_sql_query(queryf, conn)
        conn.close()
    return data_croce

data_croce = df_aurora_fetch(location_selected, b, d)

# Final Dataframe
joined_df = pd.merge(mongo_df, data_croce, left_on='vehicle_license', right_on='licenseplate', how='inner')

# Select specific columns and apply the condition
selected_df = joined_df[['vehicle_license', 'confidence', 'latitude', 'longitude', 'timestamp', 'date', 'expires', 'image0Url', 'image1Url']].copy()
selected_df.rename(columns={
    'timestamp': 'validation_time',
    'date': 'paymentdate',
    'expires': 'expiretime'
}, inplace=True)

# Apply the condition and create a 'status' column
selected_df['status'] = selected_df.apply(
    lambda row: 'Multable' if row['validation_time'] > row['expiretime'] else
                'En Tiempo' if row['paymentdate'] <= row['validation_time'] <= row['expiretime'] else
                'Pendiente de Validación',
    axis=1
)

# Convertion TimeZone from UTC to America/Mexico_City
selected_df['validation_time'] = pd.to_datetime(selected_df['validation_time'], utc=True).dt.tz_convert('America/Mexico_City')
selected_df['paymentdate'] = pd.to_datetime(selected_df['paymentdate'], utc=True).dt.tz_convert('America/Mexico_City')
selected_df['expiretime'] = pd.to_datetime(selected_df['expiretime'], utc=True).dt.tz_convert('America/Mexico_City')

#Column configuration to the construction of the final table
column_configuration = {
    "vehicle_license": st.column_config.TextColumn(
        "License Plate", help="The license of the user", max_chars=100
    ),
    "date": st.column_config.TextColumn("Date"),
    "status": st.column_config.TextColumn("Status"),
    "expires": st.column_config.TextColumn("Expiration Date"),
    "image0Url": st.column_config.ImageColumn("Image 1"), # Important that the ImageCoumn function only works with pd DF
    "image1Url": st.column_config.ImageColumn("Image 2"),
    "timestamp": st.column_config.TextColumn("Date Validation")
}

# Fetching of the final table with the correspont configurations
st.data_editor(
    selected_df,
    column_config=column_configuration,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
)

# Creation and configuration of the map
def plot_map(data, selected_df):
    fig = px.scatter_mapbox(data,
                            lat='latitude',  # refers to latitude column in the DataFrame
                            lon='longitude',  # refers to longitude column in the DataFrame
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            size_max=15,
                            hover_data={'latitude': True,
                                        'longitude': True,
                                        'vehicle_license': True},
                            zoom=10)

    # Highlight points from selected_df in orange
    fig.add_scattermapbox(lat=selected_df['latitude'],
                          lon=selected_df['longitude'],
                          mode='markers',
                          marker=dict(size=10, color='orange'),
                          name='In Kigo',
                          hoverinfo='lat+lon+text',  # To display latitude, longitude, and additional info
                          hovertext=selected_df.apply(lambda row: f"Vehicle: "
                                                                  f"{row['vehicle_license']}<br>Status: "
                                                                  f"{row['status']}", axis=1)
                          )

    fig.update_layout(mapbox_style="open-street-map", dragmode='zoom')
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    st.plotly_chart(fig)


# Use the plotting function
plot_map_func = plot_map(filtered_df, selected_df)

# Counting each status
status_counts = selected_df['status'].value_counts()

# Preparing data
data = [
    go.Bar(
        x=status_counts.index,  # index represents the status
        y=status_counts.values  # value is the count of each status
    )
]

# Creating layout
layout = go.Layout(title="Count of each Status", xaxis=dict(title='Status'), yaxis=dict(title='Count'))

# Creating figure and add data
fig = go.Figure(data=data, layout=layout)

# Plotting
st.plotly_chart(fig)

# Full Arvoo
full_vehicle_lectures = len(filtered_df)

# Count distinct lectures ARVOO
distinct_lectures = filtered_df['vehicle_license'].nunique()

# Calculate the average confidence
average_confidence = filtered_df['confidence'].mean()

st.title("Metrics ARVOO")
# Display metrics in 3 columns
col1, col2, col3 = st.columns(3)

col1.metric("Full Vehicle-Lectures", full_vehicle_lectures)
col2.metric("Distinct Lectures", distinct_lectures)
col3.metric("Average Confidence", f"{average_confidence:.2f}")

# Full Arvoo
full_vehicle_lectures_kigo = len(selected_df)

# Count distinct lectures ARVOO
distinct_lectures_kigo = selected_df['vehicle_license'].nunique()

# Calculate the average confidence
average_confidence_kigo = selected_df['confidence'].mean()

st.title("Metrics Kigo Coincidence")
# Display metrics in 3 column
col1, col2, col3 = st.columns(3)

col1.metric("Full Vehicle-Lectures", full_vehicle_lectures_kigo)
col2.metric("Distinct Lectures", distinct_lectures_kigo)
col3.metric("Average Confidence", f"{average_confidence_kigo:.2f}")

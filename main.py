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

st.set_page_config(layout="wide")

pem_key = st.secrets['pem']['private_key']

# Configuración de MongoDB
mongo_config = {
    "uri": st.secrets["mongo_credentials"]["uri"],
    "database": st.secrets["mongo_credentials"]["database"],
    "collection": st.secrets["mongo_credentials"]["collection"]
}

# Conexión a MongoDB
def connect_to_mongo():
    try:
        client = MongoClient(mongo_config["uri"])
        print("Conexión a MongoDB exitosa.")
        return client[mongo_config["database"]]
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return None

# Obtener datos de MongoDB con la consulta agregada
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

sql_hostname = st.secrets["database"]["sql_hostname"]
sql_username = st.secrets["database"]["sql_username"]
sql_password = st.secrets["database"]["sql_password"]
sql_main_database = st.secrets["database"]["sql_main_database"]
sql_port = st.secrets["database"]["sql_port"]
ssh_host = st.secrets["ssh"]["ssh_host"]
ssh_user = st.secrets["ssh"]["ssh_user"]
ssh_port = st.secrets["ssh"]["ssh_port"]

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

d = st.date_input("Date Lectures", value=None)
st.write("Select a Date:", d)


# Obtener y procesar datos de MongoDB
mongo_df = get_mongo_data()
if mongo_df is not None:
    mongo_df = process_mongo_dataframe(mongo_df)
    print(mongo_df.head())
else:
    print("No se obtuvieron datos de MongoDB.")

b = ', '.join(f"'{value}'" for value in mongo_df['vehicle_license'].to_list())

location_selected = st.selectbox('Selecciona un Projecto:', locations)

query = f'''SELECT T.paidminutes, T.date, T.expires, T.licenseplate, Z.name
            FROM CARGOMOVIL_PD.PKM_TRANSACTION T
            JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z
            ON T.zoneid = Z.id
            WHERE Z.name LIKE '%{location_selected}%'
            AND DATE(T.date) = '{d}'
            AND T.licenseplate IN ({b})
            ORDER BY T.date DESC LIMIT 100;'''

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

selected_df['validation_time'] = pd.to_datetime(selected_df['validation_time'], utc=True).dt.tz_convert('America/Mexico_City')
selected_df['paymentdate'] = pd.to_datetime(selected_df['paymentdate'], utc=True).dt.tz_convert('America/Mexico_City')
selected_df['expiretime'] = pd.to_datetime(selected_df['expiretime'], utc=True).dt.tz_convert('America/Mexico_City')


column_configuration = {
    "vehicle_license": st.column_config.TextColumn(
        "License Plate", help="The license of the user", max_chars=100
    ),
    "date": st.column_config.TextColumn("Date"),
    "status": st.column_config.TextColumn("Status"),
    "expires": st.column_config.TextColumn("Expiration Date"),
    "image0Url": st.column_config.ImageColumn("Image 1"),
    "image1Url": st.column_config.ImageColumn("Image 2"),
    "timestamp": st.column_config.TextColumn("Date Validation")
}

st.data_editor(
    selected_df,
    column_config=column_configuration,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
)


def plot_map(data):
    fig = px.scatter_mapbox(data,
                            lat='latitude',  # refers to latitude column in the DataFrame
                            lon='longitude',  # refers to longitude column in the DataFrame
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            size_max=15,
                            hover_data={'latitude': True,
                                        'longitude': True,
                                        'vehicle_license': True},
                            zoom=10)

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

    st.plotly_chart(fig)


# Use the plotting function
plot_map_func = plot_map(get_mongo_data_processed())

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
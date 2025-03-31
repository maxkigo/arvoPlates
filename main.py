import pytz
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


# Función para mostrar mensajes de error estilizados
def show_error(message):
    st.markdown(f"""
    <div style="background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; 
                border-left: 5px solid #f5c6cb; margin: 10px 0;">
        ❌ {message}
    </div>
    """, unsafe_allow_html=True)


# Función para verificar si un DataFrame está vacío
def is_empty(df):
    return df is None or df.empty


try:
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


    # Mongo data fetch
    def get_mongo_data():
        db = connect_to_mongo()
        if db is not None:
            collection = db[mongo_config["collection"]]
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
            try:
                result = list(collection.aggregate(pipeline))
                df = pd.DataFrame(result)
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
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df.fillna('', inplace=True)
            df.rename(columns={
                "_id": "id",
                "sgSpeedMs": "speed",
                "license": "vehicle_license"
            }, inplace=True)
            return df
        else:
            print("El DataFrame está vacío o no se generó correctamente.")
            return df


    @st.cache_data
    def get_mongo_data_processed():
        arvoResponse = get_mongo_data()
        processed_data = process_mongo_dataframe(arvoResponse)
        return processed_data


    # Connection to aurora
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_key_file:
        temp_key_file.write(pem_key)
        temp_key_file_path = temp_key_file.name

    try:
        mypkey = paramiko.RSAKey.from_private_key_file(temp_key_file_path)
        print("Private key loaded successfully.")
    finally:
        os.remove(temp_key_file_path)

    # Env variables
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
        "Zaragoza"]

    # Selection of the date to fetch data
    col1, col2 = st.columns(2)
    with col1:
        d = st.date_input("Date", value=datetime.date(2024, 12, 23))
    with col2:
        location_selected = st.selectbox('Project:', locations,
                                         index=locations.index('Zacatlán') if 'Zacatlán' in locations else 0)

    # Access to MongoDb
    mongo_df = get_mongo_data()

    # Mongo database filter
    if mongo_df is not None:
        mongo_df = process_mongo_dataframe(mongo_df)
        mongo_df['date_lecture'] = mongo_df['timestamp'].dt.date
        if d is not None:
            filtered_df = mongo_df[mongo_df["date_lecture"] == d]
        else:
            st.write("No date selected.")
            filtered_df = pd.DataFrame()
    else:
        print("No se obtuvieron datos de MongoDB.")
        filtered_df = pd.DataFrame()

    # Verificar si hay datos antes de continuar
    if is_empty(filtered_df):
        show_error("No hay datos de ARVOO para la fecha seleccionada")
        st.stop()

    try:
        b = ', '.join(f"'{value}'" for value in filtered_df['vehicle_license'].to_list())
    except Exception as e:
        show_error("Error al procesar las placas de vehículos")
        st.stop()


    # Auror Connection and fetching data
    @st.cache_data
    def df_aurora_fetch(location_selected, b, d):
        try:
            query = f'''SELECT T.paidminutes, CONVERT_TZ(T.date, 'UTC', 'America/Mexico_City') AS date, 
                        CONVERT_TZ(T.expires, 'UTC', 'America/Mexico_City') AS expires, 
                        T.licenseplate, Z.name
                        FROM CARGOMOVIL_PD.PKM_TRANSACTION T
                        JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z
                        ON T.zoneid = Z.id
                        WHERE Z.name LIKE '%{location_selected}%'
                        AND DATE(CONVERT_TZ(T.date, 'UTC', 'America/Mexico_City')) = '{d}'
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
                data_croce = pd.read_sql_query(query, conn)
                conn.close()

            return data_croce
        except Exception as e:
            print(f"Error en la consulta SQL: {e}")
            return pd.DataFrame()


    data_croce = df_aurora_fetch(location_selected, b, d)

    # Procesamiento solo si hay datos
    if not is_empty(data_croce):
        joined_df = pd.merge(mongo_df, data_croce, left_on='vehicle_license', right_on='licenseplate', how='inner')

        if not is_empty(joined_df):
            selected_df = joined_df[['vehicle_license', 'confidence', 'latitude', 'longitude',
                                     'timestamp', 'date', 'expires', 'image0Url', 'image1Url']].copy()
            selected_df.rename(columns={
                'timestamp': 'validation_time',
                'date': 'paymentdate',
                'expires': 'expiretime'
            }, inplace=True)

            try:
                selected_df['validation_time'] = pd.to_datetime(selected_df['validation_time'], utc=True).dt.tz_convert(
                    'America/Mexico_City')
                selected_df['expiretime'] = pd.to_datetime(selected_df['expiretime']).dt.tz_localize(
                    'America/Mexico_City')
                selected_df['paymentdate'] = pd.to_datetime(selected_df['paymentdate']).dt.tz_localize(
                    'America/Mexico_City')

                current_date = datetime.datetime.now()
                current_date = pd.to_datetime(current_date, utc=True).tz_convert('America/Mexico_City')
                selected_df['remaining_time'] = (selected_df['expiretime'] - current_date).dt.total_seconds() / 60
            except Exception as e:
                show_error(f"Error al procesar fechas: {str(e)}")
                selected_df = pd.DataFrame()
        else:
            show_error("No hay coincidencias entre los datos de ARVOO y Kigo")
            selected_df = pd.DataFrame()
    else:
        show_error("No se encontraron datos en Kigo para las placas detectadas")
        selected_df = pd.DataFrame()

    # Mostrar datos de ARVOO
    column_configuration_mr = {
        "timestamp": st.column_config.DatetimeColumn("Date Validation"),
        "latitude": st.column_config.NumberColumn("Latitude"),
        "longitude": st.column_config.NumberColumn("Longitude"),
        "vehicle_license": st.column_config.TextColumn("License Plate"),
        "confidence": st.column_config.NumberColumn("Confidence"),
        "image0Url": st.column_config.ImageColumn("Image 1"),
        "image1Url": st.column_config.ImageColumn("Image 2")
    }

    if not is_empty(mongo_df):
        try:
            mongo_df['timestamp'] = pd.to_datetime(mongo_df['timestamp'], utc=True).dt.tz_convert('America/Mexico_City')
            arvoo_df = mongo_df[['timestamp', 'latitude', 'longitude', 'vehicle_license',
                                 'confidence', 'image0Url', 'image1Url']].reset_index(drop=True).copy()
            arvoo_df = arvoo_df[arvoo_df['timestamp'].dt.date == d]

            st.title("LPR Camera Data Lectures")
            st.data_editor(
                arvoo_df,
                column_config=column_configuration_mr,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed"
            )
        except Exception as e:
            show_error(f"Error al mostrar datos de ARVOO: {str(e)}")

    # Procesar datos agrupados solo si hay datos
    if not is_empty(selected_df):
        try:
            selected_df['date'] = selected_df['paymentdate'].dt.date
            grouped_df = selected_df.groupby(['vehicle_license', 'date'], as_index=False).agg({
                'remaining_time': 'sum',
                'validation_time': 'max',
                'paymentdate': 'max',
                'expiretime': 'max',
                'confidence': 'mean',
                'latitude': 'last',
                'longitude': 'last',
                'image0Url': 'last',
                'image1Url': 'last'
            })

            grouped_df.rename(columns={
                'remaining_time': 'total_remaining_time',
                'validation_time': 'last_validation_time'
            }, inplace=True)


            def determine_status(row):
                try:
                    remaining_at_validation = (row['expiretime'] - row['last_validation_time']).total_seconds() / 60
                    if remaining_at_validation <= 0:
                        return 'Multable'
                    elif row['total_remaining_time'] > 0:
                        return 'En Tiempo'
                    else:
                        return 'Expirado'
                except:
                    return 'Indeterminado'


            grouped_df['status'] = grouped_df.apply(determine_status, axis=1)
            grouped_df['total_remaining_time'] = grouped_df['total_remaining_time'].apply(
                lambda x: f"{int(x // 60)}h {int(x % 60)}m" if x > 0 else "0h 0m")

            column_configuration = {
                "vehicle_license": st.column_config.TextColumn("License Plate", help="The license of the user",
                                                               max_chars=100),
                "status": st.column_config.TextColumn("Status"),
                "expires": st.column_config.TextColumn("Expiration Date"),
                "image0Url": st.column_config.ImageColumn("Image 1"),
                "image1Url": st.column_config.ImageColumn("Image 2"),
                "timestamp": st.column_config.TextColumn("Date Validation")
            }

            st.title("Kigo Verification Results")
            st.data_editor(
                grouped_df,
                column_config=column_configuration,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed"
            )

            # Mostrar mapa solo si hay datos de ubicación
            if 'latitude' in filtered_df.columns and 'longitude' in filtered_df.columns:
                try:
                    def plot_map(data, selected_df):
                        fig = px.scatter_mapbox(data,
                                                lat='latitude',
                                                lon='longitude',
                                                color_continuous_scale=px.colors.cyclical.IceFire,
                                                size_max=15,
                                                hover_data={'latitude': True, 'longitude': True,
                                                            'vehicle_license': True},
                                                zoom=10)

                        if not is_empty(selected_df):
                            fig.add_scattermapbox(
                                lat=selected_df['latitude'],
                                lon=selected_df['longitude'],
                                mode='markers',
                                marker=dict(size=10, color='orange'),
                                name='In Kigo',
                                hoverinfo='lat+lon+text',
                                hovertext=selected_df.apply(
                                    lambda row: f"Vehicle: {row['vehicle_license']}<br>Status: {row['status']}", axis=1)
                            )

                        fig.update_layout(mapbox_style="open-street-map", dragmode='zoom')
                        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                        st.plotly_chart(fig)


                    plot_map(filtered_df, grouped_df if not is_empty(grouped_df) else pd.DataFrame())
                except Exception as e:
                    show_error(f"Error al mostrar el mapa: {str(e)}")

            # Mostrar gráfico de barras de status
            if 'status' in grouped_df.columns:
                try:
                    status_counts = grouped_df['status'].value_counts()
                    fig = go.Figure(
                        data=[go.Bar(x=status_counts.index, y=status_counts.values)],
                        layout=go.Layout(title="Count of each Status",
                                         xaxis=dict(title='Status'),
                                         yaxis=dict(title='Count'))
                    )
                    st.plotly_chart(fig)
                except Exception as e:
                    show_error(f"Error al mostrar gráfico de status: {str(e)}")

            # Mostrar métricas
            st.title("Metrics ARVOO")
            col1, col2, col3 = st.columns(3)
            col1.metric("Full Vehicle-Lectures", len(filtered_df))
            col2.metric("Distinct Lectures", filtered_df['vehicle_license'].nunique())
            col3.metric("Average Confidence", f"{filtered_df['confidence'].mean():.2f}")

            st.title("Metrics Kigo Coincidence")
            col1, col2, col3 = st.columns(3)
            col1.metric("Full Vehicle-Lectures", len(selected_df))
            col2.metric("Distinct Lectures", selected_df['vehicle_license'].nunique())
            col3.metric("Average Confidence", f"{selected_df['confidence'].mean():.2f}")

        except Exception as e:
            show_error(f"Error al procesar los resultados: {str(e)}")

except Exception as e:
    show_error(f"Error inesperado en la aplicación: {str(e)}")


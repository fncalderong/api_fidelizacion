# Cargo las librerias
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import xgboost as xgb
import datetime
import pytz
import json
import sys
import hashlib

# Modelo Xgboost
modelo = xgb.Booster({'nthread': 4})  # init model
modelo.load_model("archivos/xgb_lite.model")  # load data

# Agrego las credenciales
credentials = service_account.Credentials.from_service_account_file('credenciales.json')
project_id = 'credibancoml'
# Defino el cliente en BigQuery
client = bigquery.Client(credentials= credentials,project=project_id)

# Defino la función que genera la data para el modelo
class SUPERAPP_DESCRIPTIVOS:
    def __init__(self):
        self.client = []
    def cosa(self, results):
        tokens = results.get("tokens")
        info_tarjetas= results.get("info_tarjetas")
        if (info_tarjetas[0]==0) | (info_tarjetas[0]==1):
            tz = pytz.timezone('America/Bogota')
            Fecha = np.int(datetime.datetime.now(tz=tz).strftime('%Y%m%d'))
            if type(tokens) == list:
                tipo_dato = np.repeat(np.nan, len(tokens))
                longitud_dato = np.repeat(np.nan, len(tokens))
                i = 0
                while i < len(tokens):
                    tipo_dato[i] = int(np.where(type(tokens[i]) == str, 1, 0))
                    i += 1
                if len(tokens) != 0:
                    if sum(tipo_dato) == len(tokens):
                        if (info_tarjetas[0]==1):

                            i=0
                        while i < len(tokens):
                            longitud_dato[i] = int(np.where(len(tokens[i]) == 64, 1, 0))
                            i += 1
                        if sum(longitud_dato) == len(tokens):
                            # ========================= Funcion 1: Hace la consulta en BigQuery
                            # Hago la consultas
                            consulta = 'SELECT * FROM `credibancoml.Recomienda.Prueba` WHERE NU_TARJETA_SHA256 IN ("' + '","'.join(tuple(tokens)) + '")'
                            query_job = client.query(consulta)
                            # Convierto los resultados en pandas data.frame
                            DATOS = query_job.to_dataframe()
                            if (DATOS.shape[0] == 0):
                                sys.exit('No hay informacion de las tarjetas')

                            #DATOS = pd.read_csv("C:/Users/credibanco_ml/Desktop/DATOS.csv")
                            # ========================= Funcion 2: Transformacion de la tabla
                            BINES = pd.read_excel("archivos/BINES.xlsx")
                            datos = pd.merge(DATOS, BINES, left_on='BIN', right_on='CD_BIN', how='left')

                            ##########
                            datos_tot = datos[
                                ["NU_TARJETA_SHA256", "ANTIGUEDAD", "DIF_TIEMPO_UC", "CAT_FREQ", "PRO_MENS_COMPRAS",
                                 "PRO_MENS_ESTBL",
                                 "CIUDAD_PRINCIPAL", "NM_BIN", "NM_PRODUCTO"]]
                            # Se agrega la zona horaria de Colombia
                            tz = pytz.timezone('America/Bogota')
                            Fecha = datetime.datetime.now(tz=tz).strftime('%Y/%m/%d  %H:%M:%S')

                            # ========================= Funcion 4: Devuelve las predicciones en un .json
                            tarjetas_nombre = pd.DataFrame(datos_tot['NU_TARJETA_SHA256'].drop_duplicates()).reset_index(
                                drop=True)
                            json_data = []  # Crea una lista vacia para guardar los resultados
                            for i in range(tarjetas_nombre.shape[0]):
                                # df_tarjeta_selec = datos_tot.loc[datos_tot['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i,0]].drop(['NU_TARJETA_SHA256'], axis=1).to_json(orient='records')
                                df_tarjeta_selec = datos_tot.loc[
                                    datos_tot['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i, 0]].drop(
                                    ['NU_TARJETA_SHA256'], axis=1)
                                df_tarjeta_selec = df_tarjeta_selec.fillna("NULL")
                                json_1 = pd.DataFrame({'NU_TARJETA_SHA256': [tarjetas_nombre.loc[i, :][0]]}).to_json(
                                    orient='records')
                                json1_data = json.loads(json_1)[0]
                                json_2 = df_tarjeta_selec.to_json(orient='records')
                                json2_data = json.loads(json_2)[0]
                                json1_data["Valores"] = json2_data
                                co = json.dumps(json1_data)
                                json_tarjeta = json.loads(co)
                                json_data.append(json_tarjeta)
                            # Reultado final en formarto .json
                            json_data_2 = [{'Fecha': Fecha, 'Tarjeta': json_data[0:tarjetas_nombre.shape[0]]}]
                            respuesta =json.dumps(json_data_2)
                        else: respuesta = "0"
                    else: respuesta = "1"
                else: respuesta = "2"
            else: respuesta = "3"
        else: respuesta = "4"
        print(respuesta)
        return(respuesta)

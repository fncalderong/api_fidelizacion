import pandas as pd
import numpy as np
import datetime
import pytz
import json
import hashlib
import psycopg2

connection = psycopg2.connect(user = "postgres",
                                  password = "Fncalderon91G",
                                  host = "35.232.151.83",
                                  port = "5432",
                                  database = "postgres")

cursor = connection.cursor()


# Defino la función que genera la data para el modelo
class SIFCO_MODEL:
    def __init__(self):
        self.client = []
    def sifco(self, results):
        tokens = results.get("tokens")
        info_tarjetas = results.get("info_tarjetas")
        if type(info_tarjetas) == list:
            if len(info_tarjetas) != 0:
                if (info_tarjetas[0] == 0) | (info_tarjetas[0] == 1):
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
                                if (info_tarjetas[0] == 1):
                                    val_index = 0
                                    cosa = []
                                    while val_index < len(tokens):
                                        cosa.append(hashlib.sha256(str.encode(tokens[val_index])).hexdigest())
                                        val_index += 1
                                    tokens = cosa[:]
                                i = 0
                                while i < len(tokens):
                                    longitud_dato[i] = int(np.where(len(tokens[i]) == 64, 1, 0))
                                    i += 1
                                if sum(longitud_dato) == len(tokens):
                                    consulta = "SELECT *  FROM sifco  WHERE nu_tarjeta_sha256 IN ('" + "','".join(tuple(tokens)) + "')"
                                    cursor = connection.cursor()
                                    cursor.execute(consulta)
                                    DATOS = pd.DataFrame(cursor.fetchall())
                                    if (DATOS.shape[0] != 0):
                                        colnames_1 = [desc[0] for desc in cursor.description]
                                        colnames = [colum.upper() for colum in colnames_1]
                                        DATOS.columns = colnames
                                        DATOS['CLUSTER']=DATOS['CLUSTER'].astype('int64')
                                        # Se agrega la zona horaria de Colombia
                                        tz = pytz.timezone('America/Bogota')
                                        Fecha = datetime.datetime.now(tz=tz).strftime('%Y/%m/%d  %H:%M:%S')
                                        # ========================= Funcion 4: Devuelve las predicciones en un .json
                                        tarjetas_nombre = pd.DataFrame(DATOS['NU_TARJETA_SHA256'].drop_duplicates()).reset_index(drop=True)
                                        json_data = []  # Crea una lista vacia para guardar los resultados
                                        for i in range(tarjetas_nombre.shape[0]):
                                            df_tarjeta_selec = DATOS.loc[DATOS['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i, 0]]
                                            df_tarjeta_selec = df_tarjeta_selec.fillna("NULL")
                                            # Crea el primer JSON con la info de la tarjeta y a que cluster pertenece
                                            json_1 = df_tarjeta_selec[['NU_TARJETA_SHA256','CLUSTER']].to_json(orient='records')
                                            json1_data = json.loads(json_1)[0]
                                            # Crea el segundo JSON con la info adicional
                                            json_2 = df_tarjeta_selec.drop(['NU_TARJETA_SHA256', 'CLUSTER'], axis=1).to_json(orient='records')
                                            json2_data = json.loads(json_2)[0]
                                            # se crea un solo json por tarjeta en el que anida el json2 en el json1
                                            json1_data["Valores"] = json2_data
                                            co = json.dumps(json1_data)
                                            json_tarjeta = json.loads(co)
                                            json_data.append(json_tarjeta)
                                        respuesta = json_data[0:tarjetas_nombre.shape[0]]
                                    else:
                                        respuesta = "5"
                                else: respuesta = "0"
                            else: respuesta = "1"
                        else: respuesta = "2"
                    else: respuesta = "3"
                else: respuesta = "4"
            else:respuesta = "7"
        else: respuesta = "6"
        return (respuesta)

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

# Modelo Xgboost
modelo = xgb.Booster({'nthread': 4})  # init model
modelo.load_model("archivos/xgb_lite.model")  # load data

# Agrego las credenciales
credentials = service_account.Credentials.from_service_account_file('credenciales.json')
project_id = 'credibancoml'
# Defino el cliente en BigQuery
client = bigquery.Client(credentials= credentials,project=project_id)

# Defino la función que genera la data para el modelo
class SUPERAPP:
    def __init__(self):
        self.client = []
    def datos_totales(self, results):
        tokens = results.get("tokens")
        token_prueba = type(results.get("tokens"))
        print("Este es el tipo del token")
        print(token_prueba)
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
                    i=0
                    while i < len(tokens):
                        longitud_dato[i] = int(np.where(len(tokens[i]) == 64, 1, 0))
                        i += 1
                    if sum(longitud_dato) == len(tokens):
                        # ========================= Funcion 1: Hace la consulta en BigQuery
                        # Hago la consultas
                        consulta = 'SELECT *, CAST(FECHA AS STRING) AS FECHA_D  FROM `credibancoml.Recomienda.Prueba` WHERE NU_TARJETA_SHA256 IN ("' + '","'.join(tuple(tokens)) + '")'
                        query_job = client.query(consulta)
                        # Convierto los resultados en pandas data.frame
                        DATOS = query_job.to_dataframe()
                        if (DATOS.shape[0] == 0):
                            sys.exit('No hay informacion de las tarjetas')
                        Top = 5
                        #DATOS = pd.read_csv("C:/Users/credibanco_ml/Desktop/DATOS.csv")
                        # ========================= Funcion 2: Transformacion de la tabla
                        BINES = pd.read_excel("archivos/BINES.xlsx")
                        datos = pd.merge(DATOS, BINES, left_on='BIN', right_on='CD_BIN', how='left')
                        datos['MAESTRO_ELECTRON'] = np.where(datos.NM_PRODUCTO.isin(['MAESTRO', 'ELECTRON']), 1, 0)
                        datos['ORO'] = np.where(
                            np.logical_or(datos['NM_PRODUCTO'].str.find('GOLD') != -1, datos['NM_PRODUCTO'].str.find('ORO') != -1), 1,0)
                        datos['BLACK_UP'] = np.where(np.logical_or(np.logical_or(datos['NM_PRODUCTO'].str.find('BLACK') != -1,
                                                                                 datos['NM_PRODUCTO'].str.find('SIGNATURE') != -1),
                                                                   datos['NM_PRODUCTO'].str.find('PLATINUM') != -1), 1, 0)
                        datos['INTERNACIONAL'] = np.where(datos['NM_PRODUCTO'].str.find('INTERNACIONAL') != -1, 1, 0)
                        datos['EMPRESARIAL'] = np.where(np.logical_or(np.logical_or(datos['NM_PRODUCTO'].str.find('EMPRESARIAL') != -1,
                                                                                    datos['NM_PRODUCTO'].str.find('CORPORATIVA') != -1),
                                                                      datos['NM_PRODUCTO'].str.find('BUSINESS') != -1), 1, 0)
                        ##########
                        datos['SUPER_APP_UC_SUPERMERCADOS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "SUPERMERCADOS", 1, 0)
                        datos['SUPER_APP_UC_RESTAURANTES'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "RESTAURANTES", 1, 0)
                        datos['SUPER_APP_UC_EDS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "EDS", 1, 0)
                        datos['SUPER_APP_UC_MI_ARMARIO'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "MI ARMARIO", 1, 0)
                        datos['SUPER_APP_UC_DROGUERIAS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "DROGUERIAS", 1, 0)
                        datos['SUPER_APP_UC_TIENDAS_ALMACENES'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "TIENDAS Y ALMACENES", 1,
                                                                           0)
                        datos['SUPER_APP_UC_TECNOLOGIA'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "TECNOLOGIA", 1, 0)
                        datos['SUPER_APP_UC_VIAJES_TURISMO'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "VIAJES Y TURISMO", 1, 0)
                        datos['SUPER_APP_UC_CINE_TEATRO'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "CINE Y TEATRO", 1, 0)
                        datos['SUPER_APP_UC_HOGAR'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "HOGAR", 1, 0)
                        datos['SUPER_APP_UC_ENTRETENIMIENTO'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "ENTRETENIMIENTO", 1, 0)
                        datos['SUPER_APP_UC_DISCOTECA_BARES'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "DISCOTECA Y BARES", 1, 0)
                        datos['SUPER_APP_UC_MASCOTAS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "MASCOTAS", 1, 0)
                        datos['SUPER_APP_UC_PELUQUERIAS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "PELUQUERIAS", 1, 0)
                        datos['SUPER_APP_UC_TRANSPORTE'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "TRANSPORTE", 1, 0)
                        datos['SUPER_APP_UC_SERVICIOS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "SERVICIOS", 1, 0)
                        datos['SUPER_APP_UC_EDUCACION'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "EDUCACION", 1, 0)
                        datos['SUPER_APP_UC_SECTORFINANCIERO'] = np.where(
                            datos['CATEGORIA_SUPER_APP_UC'] == "SECTOR FINANCIERO Y ASEGURADOR", 1, 0)
                        datos['SUPER_APP_UC_EVENTOS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "EVENTOS", 1, 0)
                        datos['SUPER_APP_UC_GIMNASIOS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "GIMNASIOS", 1, 0)
                        datos['SUPER_APP_UC_PARQUEADEROS'] = np.where(datos['CATEGORIA_SUPER_APP_UC'] == "PARQUEADEROS", 1, 0)
                        ##########
                        datos['FREQ_DESP_UC_SUPERMERCADOS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "SUPERMERCADOS", 1, 0)
                        datos['FREQ_DESP_UC_RESTAURANTES'] = np.where(datos['CAT_FREQ_DESP_UC'] == "RESTAURANTES", 1, 0)
                        datos['FREQ_DESP_UC_NA'] = np.where(datos['CAT_FREQ_DESP_UC'] == "", 1, 0)
                        datos['FREQ_DESP_UC_EDS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "EDS", 1, 0)
                        datos['FREQ_DESP_UC_MI_ARMARIO'] = np.where(datos['CAT_FREQ_DESP_UC'] == "MI ARMARIO", 1, 0)
                        datos['FREQ_DESP_UC_DROGUERIAS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "DROGUERIAS", 1, 0)
                        datos['FREQ_DESP_UC_TIENDAS_ALMACENES'] = np.where(datos['CAT_FREQ_DESP_UC'] == "TIENDAS Y ALMACENES", 1, 0)
                        datos['FREQ_DESP_UC_TECNOLOGIA'] = np.where(datos['CAT_FREQ_DESP_UC'] == "TECNOLOGIA", 1, 0)
                        datos['FREQ_DESP_UC_VIAJES_TURISMO'] = np.where(datos['CAT_FREQ_DESP_UC'] == "VIAJES Y TURISMO", 1, 0)
                        datos['FREQ_DESP_UC_CINE_TEATRO'] = np.where(datos['CAT_FREQ_DESP_UC'] == "CINE Y TEATRO", 1, 0)
                        datos['FREQ_DESP_UC_HOGAR'] = np.where(datos['CAT_FREQ_DESP_UC'] == "HOGAR", 1, 0)
                        datos['FREQ_DESP_UC_ENTRETENIMIENTO'] = np.where(datos['CAT_FREQ_DESP_UC'] == "ENTRETENIMIENTO", 1, 0)
                        datos['FREQ_DESP_UC_DISCOTECA_BARES'] = np.where(datos['CAT_FREQ_DESP_UC'] == "DISCOTECA Y BARES", 1, 0)
                        datos['FREQ_DESP_UC_MASCOTAS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "MASCOTAS", 1, 0)
                        datos['FREQ_DESP_UC_PELUQUERIAS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "PELUQUERIAS", 1, 0)
                        datos['FREQ_DESP_UC_TRANSPORTE'] = np.where(datos['CAT_FREQ_DESP_UC'] == "TRANSPORTE", 1, 0)
                        datos['FREQ_DESP_UC_SERVICIOS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "SERVICIOS", 1, 0)
                        datos['FREQ_DESP_UC_EDUCACION'] = np.where(datos['CAT_FREQ_DESP_UC'] == "EDUCACION", 1, 0)
                        datos['FREQ_DESP_UC_SECTORFINANCIERO'] = np.where(datos['CAT_FREQ_DESP_UC'] == "SECTOR FINANCIERO Y ASEGURADOR",1, 0)
                        datos['FREQ_DESP_UC_EVENTOS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "EVENTOS", 1, 0)
                        datos['FREQ_DESP_UC_GIMNASIOS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "GIMNASIOS", 1, 0)
                        datos['FREQ_DESP_UC_PARQUEADEROS'] = np.where(datos['CAT_FREQ_DESP_UC'] == "PARQUEADEROS", 1, 0)
                        ##########
                        datos['CAT_FREQ_SUPERMERCADOS'] = np.where(datos['CAT_FREQ'] == "SUPERMERCADOS", 1, 0)
                        datos['CAT_FREQ_RESTAURANTES'] = np.where(datos['CAT_FREQ'] == "RESTAURANTES", 1, 0)
                        datos['CAT_FREQ_EDS'] = np.where(datos['CAT_FREQ'] == "EDS", 1, 0)
                        datos['CAT_FREQ_MI_ARMARIO'] = np.where(datos['CAT_FREQ'] == "MI ARMARIO", 1, 0)
                        datos['CAT_FREQ_DROGUERIAS'] = np.where(datos['CAT_FREQ'] == "DROGUERIAS", 1, 0)
                        datos['CAT_FREQ_TIENDAS_ALMACENES'] = np.where(datos['CAT_FREQ'] == "TIENDAS Y ALMACENES", 1, 0)
                        datos['CAT_FREQ_TECNOLOGIA'] = np.where(datos['CAT_FREQ'] == "TECNOLOGIA", 1, 0)
                        datos['CAT_FREQ_VIAJES_TURISMO'] = np.where(datos['CAT_FREQ'] == "VIAJES Y TURISMO", 1, 0)
                        datos['CAT_FREQ_CINE_TEATRO'] = np.where(datos['CAT_FREQ'] == "CINE Y TEATRO", 1, 0)
                        datos['CAT_FREQ_HOGAR'] = np.where(datos['CAT_FREQ'] == "HOGAR", 1, 0)
                        datos['CAT_FREQ_ENTRETENIMIENTO'] = np.where(datos['CAT_FREQ'] == "ENTRETENIMIENTO", 1, 0)
                        datos['CAT_FREQ_DISCOTECA_BARES'] = np.where(datos['CAT_FREQ'] == "DISCOTECA Y BARES", 1, 0)
                        datos['CAT_FREQ_MASCOTAS'] = np.where(datos['CAT_FREQ'] == "MASCOTAS", 1, 0)
                        datos['CAT_FREQ_PELUQUERIAS'] = np.where(datos['CAT_FREQ'] == "PELUQUERIAS", 1, 0)
                        datos['CAT_FREQ_TRANSPORTE'] = np.where(datos['CAT_FREQ'] == "TRANSPORTE", 1, 0)
                        datos['CAT_FREQ_SERVICIOS'] = np.where(datos['CAT_FREQ'] == "SERVICIOS", 1, 0)
                        datos['CAT_FREQ_EDUCACION'] = np.where(datos['CAT_FREQ'] == "EDUCACION", 1, 0)
                        datos['CAT_FREQ_SECTORFINANCIERO'] = np.where(datos['CAT_FREQ'] == "SECTOR FINANCIERO Y ASEGURADOR", 1, 0)
                        datos['CAT_FREQ_EVENTOS'] = np.where(datos['CAT_FREQ'] == "EVENTOS", 1, 0)
                        datos['CAT_FREQ_GIMNASIOS'] = np.where(datos['CAT_FREQ'] == "GIMNASIOS", 1, 0)
                        datos['CAT_FREQ_PARQUEADEROS'] = np.where(datos['CAT_FREQ'] == "PARQUEADEROS", 1, 0)
                        ##########
                        datos['DIA_CALC'] = pd.DatetimeIndex(pd.to_datetime(datos['FECHA'].astype(str), format='%Y%m%d')).day
                        datos['MES_CALC'] = pd.DatetimeIndex(pd.to_datetime(datos['FECHA'].astype(str), format='%Y%m%d')).month
                        ##########
                        datos_tot = datos[["FECHA", "CAT_FREQ_EDS", "CAT_FREQ_RESTAURANTES", "EDS", "RESTAURANTES",
                                           "FREQ_DESP_UC_SUPERMERCADOS", "DROGUERIAS", "SUPERMERCADOS",
                                           "PRO_MENS_COMPRAS", "MI_ARMARIO", "PRO_MENS_ESTBL", "CAT_FREQ_MI_ARMARIO",
                                           "SUPER_APP_UC_SUPERMERCADOS", "CAT_FREQ_SUPERMERCADOS", "DIF_TIEMPO_UC",
                                           "MES_CALC", "SUPERMERCADOS_DIA_4", "DIA_CALC", "CAT_FREQ_DROGUERIAS",
                                           "RESTAURANTES_DIA_6", "SUPERMERCADOS_DIA_7", "RESTAURANTES_DIA_5",
                                           "SUPERMERCADOS_DIA_3", "ANTIGUEDAD", "DIF_TIEMPO_UPC", "SUPERMERCADOS_DIA_6",
                                           "SUPERMERCADOS_DIA_2", "SUPERMERCADOS_DIA_1", "SUPERMERCADOS_DIA_5",
                                           "RESTAURANTES_DIA_3"]]
                        # Se agrega la zona horaria de Colombia
                        ct = datetime.datetime.now(tz=tz)
                        HORA = ct.strftime('%H')
                        # Se ingresa la hora del sistema en el modelo
                        datos_tot.loc[:, ('HORA_Y3')] = int(HORA)
                        # Se reemplaza None por NA
                        if sum(datos_tot.dtypes == 'object') >= 1:
                            datos_tot.fillna(np.nan, inplace=True)
                        # ========================= Funcion 3: Hace la consulta en BigQuery
                        Categorias = ['CINE Y TEATRO', 'DISCOTECA Y BARES', 'DROGUERIAS', 'EDS', 'EDUCACION', 'ENTRETENIMIENTO',
                                      'EVENTOS', 'GIMNASIOS', 'HOGAR', 'MASCOTAS', 'MI ARMARIO', 'OTROS', 'PARQUEADEROS', 'PELUQUERIAS',
                                      'RESTAURANTES', 'SECTOR FINANCIERO Y ASEGURADOR', 'SERVICIOS', 'SUPERMERCADOS', 'TECNOLOGIA',
                                      'TIENDAS Y ALMACENES', 'TRANSPORTE', 'VIAJES Y TURISMO']
                        dtest = xgb.DMatrix(datos_tot)
                        probs = pd.DataFrame(modelo.predict(dtest))
                        probs.columns = Categorias
                        # probs = probs.apply(np.mean, axis=0).to_frame().T #Se agrega esta linea
                        probs["NU_TARJETA_SHA256"] = DATOS["NU_TARJETA_SHA256"]
                        ########## Guardo las predicciones en un DataFrame
                        df_Final = np.tile('OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO', (datos_tot.shape[0] * Top, 3))
                        for i in range(datos_tot.shape[0]):
                            df_parcial = pd.DataFrame(
                                {'serie': probs.iloc[i, 0:22].sort_values(ascending=False).head(Top).astype('str'),
                                 'NU_TARJETA_SHA256': probs.iloc[i, 22]})
                            df_parcial.reset_index(inplace=True)
                            df_parcial = df_parcial.values
                            df_Final[(i * Top):((i + 1) * Top)] = df_parcial
                        df_Final = pd.DataFrame(df_Final)
                        df_Final.columns = ["CATEGORIA", "PROBABILIDAD", "NU_TARJETA_SHA256"]
                        # ========================= Funcion 4: Devuelve las predicciones en un .json
                        tarjetas_nombre = pd.DataFrame(df_Final['NU_TARJETA_SHA256'].drop_duplicates()).reset_index(drop=True)
                        json_data = []  # Crea una lista vacia para guardar los resultados
                        for i in range(tarjetas_nombre.shape[0]):
                            df_tarjeta_selec = df_Final.loc[
                                df_Final['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i, 0], ["CATEGORIA", "PROBABILIDAD"]]
                            json_1 = pd.DataFrame({'NU_TARJETA_SHA256': [tarjetas_nombre.loc[i, :][0]]}).to_json(orient='records')
                            json1_data = json.loads(json_1)[0]
                            json_2 = df_tarjeta_selec.set_index('CATEGORIA').T.to_json(orient='records')
                            json2_data = json.loads(json_2)[0]
                            json1_data["Categorias"] = json2_data
                            co = json.dumps(json1_data)
                            json_tarjeta = json.loads(co)
                            json_data.append(json_tarjeta)
                        # Reultado final en formarto .json
                        json_data_2 = [{'Fecha': Fecha, 'Hora': HORA, 'Tarjeta': json_data[0:tarjetas_nombre.shape[0]]}]
                        respuesta = json.dumps(json_data_2)
                    else: respuesta = "0"
                else: respuesta = "1"
            else: respuesta = "2"
        else: respuesta = "3"
        print(respuesta)
        return(respuesta)

# Cargo las librerias
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import datetime
import pytz
import json
import hashlib



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
        if type(info_tarjetas) == list:
            if len(info_tarjetas) != 0:
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
                                    val_index=0
                                    cosa=[]
                                    while val_index < len(tokens):
                                        cosa.append(hashlib.sha256(str.encode(tokens[val_index])).hexdigest())
                                        val_index += 1
                                    tokens=cosa[:]
                                i = 0
                                while i < len(tokens):
                                    longitud_dato[i] = int(np.where(len(tokens[i]) == 64, 1, 0))
                                    i += 1
                                if sum(longitud_dato) == len(tokens):
                                    consulta = 'SELECT  NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,CASE WHEN CAT_FREQ="CINE Y TEATRO" THEN CINE_Y_TEATRO_SUM WHEN CAT_FREQ="DISCOTECA Y BARES" THEN DISCOTECA_Y_BARES_SUM  WHEN CAT_FREQ="DROGUERIAS" THEN DROGUERIAS_SUM WHEN CAT_FREQ="EDS" THEN EDS_SUM WHEN CAT_FREQ="EDUCACION" THEN EDUCACION_SUM WHEN CAT_FREQ="ENTRETENIMIENTO" THEN ENTRETENIMIENTO_SUM WHEN CAT_FREQ="EVENTOS" THEN EVENTOS_SUM WHEN CAT_FREQ="GIMNASIOS" THEN GIMNASIOS_SUM WHEN CAT_FREQ="HOGAR" THEN HOGAR_SUM WHEN CAT_FREQ="MASCOTAS" THEN MASCOTAS_SUM WHEN CAT_FREQ="MI ARMARIO" THEN MI_ARMARIO_SUM WHEN CAT_FREQ="OTROS" THEN OTROS_SUM WHEN CAT_FREQ="PARQUEADEROS" THEN PARQUEADEROS_SUM WHEN CAT_FREQ="PELUQUERIAS" THEN PELUQUERIAS_SUM WHEN CAT_FREQ="RESTAURANTES" THEN RESTAURANTES_SUM WHEN CAT_FREQ="SECTOR FINANCIERO Y ASEGURADOR" THEN SECTOR_FINANCIERO_Y_ASEGURADOR_SUM WHEN CAT_FREQ="SERVICIOS" THEN SERVICIOS_SUM WHEN CAT_FREQ="SUPERMERCADOS" THEN SUPERMERCADOS_SUM WHEN CAT_FREQ="TECNOLOGIA" THEN TECNOLOGIA_SUM WHEN CAT_FREQ="TIENDAS Y ALMACENES" THEN TIENDAS_Y_ALMACENES_SUM WHEN CAT_FREQ="TRANSPORTE" THEN TRANSPORTE_SUM WHEN CAT_FREQ="VIAJES Y TURISMO" THEN VIAJES_Y_TURISMO_SUM  END AS CONTEO_FREQ    FROM( SELECT NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,(COALESCE(CINE_Y_TEATRO_DIA_1,0)+COALESCE(CINE_Y_TEATRO_DIA_2,0)+COALESCE(CINE_Y_TEATRO_DIA_3,0)+COALESCE(CINE_Y_TEATRO_DIA_4,0)+COALESCE(CINE_Y_TEATRO_DIA_5,0)+COALESCE(CINE_Y_TEATRO_DIA_6,0)+COALESCE(CINE_Y_TEATRO_DIA_7,0)) AS CINE_Y_TEATRO_SUM,(COALESCE(DISCOTECA_Y_BARES_DIA_1,0)+COALESCE(DISCOTECA_Y_BARES_DIA_2,0)+COALESCE(DISCOTECA_Y_BARES_DIA_3,0)+COALESCE(DISCOTECA_Y_BARES_DIA_4,0)+COALESCE(DISCOTECA_Y_BARES_DIA_5,0)+COALESCE(DISCOTECA_Y_BARES_DIA_6,0)+COALESCE(DISCOTECA_Y_BARES_DIA_7,0)) AS DISCOTECA_Y_BARES_SUM,    (COALESCE(DROGUERIAS_DIA_1,0)+COALESCE(DROGUERIAS_DIA_2,0)+COALESCE(DROGUERIAS_DIA_3,0)+COALESCE(DROGUERIAS_DIA_4,0)+COALESCE(DROGUERIAS_DIA_5,0)+COALESCE(DROGUERIAS_DIA_6,0)+COALESCE(DROGUERIAS_DIA_7,0)) AS DROGUERIAS_SUM,(COALESCE(EDS_DIA_1,0)+COALESCE(EDS_DIA_2,0)+COALESCE(EDS_DIA_3,0)+COALESCE(EDS_DIA_4,0)+COALESCE(EDS_DIA_5,0)+COALESCE(EDS_DIA_6,0)+COALESCE(EDS_DIA_7,0) ) AS EDS_SUM,(COALESCE(EDUCACION_DIA_1,0)+COALESCE(EDUCACION_DIA_2,0)+COALESCE(EDUCACION_DIA_3,0)+COALESCE(EDUCACION_DIA_4,0)+COALESCE(EDUCACION_DIA_5,0)+COALESCE(EDUCACION_DIA_6,0)+COALESCE(EDUCACION_DIA_7,0)) AS EDUCACION_SUM,(COALESCE(ENTRETENIMIENTO_DIA_1,0)+COALESCE(ENTRETENIMIENTO_DIA_2,0)+COALESCE(ENTRETENIMIENTO_DIA_3,0)+COALESCE(ENTRETENIMIENTO_DIA_4,0)+COALESCE(ENTRETENIMIENTO_DIA_5,0)+COALESCE(ENTRETENIMIENTO_DIA_6,0)+COALESCE(ENTRETENIMIENTO_DIA_7,0)) AS ENTRETENIMIENTO_SUM,(COALESCE(MI_ARMARIO_DIA_1,0)+COALESCE(MI_ARMARIO_DIA_2,0)+COALESCE(MI_ARMARIO_DIA_3,0)+COALESCE(MI_ARMARIO_DIA_4,0)+COALESCE(MI_ARMARIO_DIA_5,0)+COALESCE(MI_ARMARIO_DIA_6,0)+COALESCE(MI_ARMARIO_DIA_7,0)) AS MI_ARMARIO_SUM,(COALESCE(EVENTOS_DIA_1,0)+COALESCE(EVENTOS_DIA_2,0)+COALESCE(EVENTOS_DIA_3,0)+COALESCE(EVENTOS_DIA_4,0)+COALESCE(EVENTOS_DIA_5,0)+COALESCE(EVENTOS_DIA_6,0)+COALESCE(EVENTOS_DIA_7,0)) AS EVENTOS_SUM,(COALESCE(OTROS_DIA_1,0)+COALESCE(OTROS_DIA_2,0)+COALESCE(OTROS_DIA_3,0)+COALESCE(OTROS_DIA_4,0)+COALESCE(OTROS_DIA_5,0)+COALESCE(OTROS_DIA_6,0)+COALESCE(OTROS_DIA_7,0)) AS OTROS_SUM,(COALESCE(GIMNASIOS_DIA_1,0)+COALESCE(GIMNASIOS_DIA_2,0)+COALESCE(GIMNASIOS_DIA_3,0)+COALESCE(GIMNASIOS_DIA_4,0)+COALESCE(GIMNASIOS_DIA_5,0)+COALESCE(GIMNASIOS_DIA_6,0)+COALESCE(GIMNASIOS_DIA_7,0)) AS GIMNASIOS_SUM,(COALESCE(PARQUEADEROS_DIA_1,0)+COALESCE(PARQUEADEROS_DIA_2,0)+COALESCE(PARQUEADEROS_DIA_3,0)+COALESCE(PARQUEADEROS_DIA_4,0)+COALESCE(PARQUEADEROS_DIA_5,0)+COALESCE(PARQUEADEROS_DIA_6,0)+COALESCE(PARQUEADEROS_DIA_7,0)  ) AS PARQUEADEROS_SUM,(COALESCE(HOGAR_DIA_1,0)+COALESCE(HOGAR_DIA_2,0)+COALESCE(HOGAR_DIA_3,0)+COALESCE(HOGAR_DIA_4,0)+COALESCE(HOGAR_DIA_5,0)+COALESCE(HOGAR_DIA_6,0)+COALESCE(HOGAR_DIA_7,0)) AS HOGAR_SUM,(COALESCE(MASCOTAS_DIA_1,0)+COALESCE(MASCOTAS_DIA_2,0)+COALESCE(MASCOTAS_DIA_3,0)+COALESCE(MASCOTAS_DIA_4,0)+COALESCE(MASCOTAS_DIA_5,0)+COALESCE(MASCOTAS_DIA_6,0)+COALESCE(MASCOTAS_DIA_7,0)) AS MASCOTAS_SUM,(COALESCE(PELUQUERIAS_DIA_1,0)+COALESCE(PELUQUERIAS_DIA_2,0)+COALESCE(PELUQUERIAS_DIA_3,0)+COALESCE(PELUQUERIAS_DIA_4,0)+COALESCE(PELUQUERIAS_DIA_5,0)+COALESCE(PELUQUERIAS_DIA_6,0)+COALESCE(PELUQUERIAS_DIA_7,0)) AS PELUQUERIAS_SUM,(COALESCE(RESTAURANTES_DIA_1,0)+COALESCE(RESTAURANTES_DIA_2,0)+COALESCE(RESTAURANTES_DIA_3,0)+COALESCE(RESTAURANTES_DIA_4,0)+COALESCE(RESTAURANTES_DIA_5,0)+COALESCE(RESTAURANTES_DIA_6,0)+COALESCE(RESTAURANTES_DIA_7,0)) AS RESTAURANTES_SUM,(COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_1,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_2,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_3,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_4,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_5,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_6,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_7,0)) AS SECTOR_FINANCIERO_Y_ASEGURADOR_SUM,(COALESCE(SERVICIOS_DIA_1,0)+COALESCE(SERVICIOS_DIA_2,0)+COALESCE(SERVICIOS_DIA_3,0)+COALESCE(SERVICIOS_DIA_4,0)+COALESCE(SERVICIOS_DIA_5,0)+COALESCE(SERVICIOS_DIA_6,0)+COALESCE(SERVICIOS_DIA_7,0)) AS SERVICIOS_SUM,(COALESCE(SUPERMERCADOS_DIA_1,0)+COALESCE(SUPERMERCADOS_DIA_2,0)+COALESCE(SUPERMERCADOS_DIA_3,0)+COALESCE(SUPERMERCADOS_DIA_4,0)+COALESCE(SUPERMERCADOS_DIA_5,0)+COALESCE(SUPERMERCADOS_DIA_6,0)+COALESCE(SUPERMERCADOS_DIA_7,0)) AS SUPERMERCADOS_SUM,(COALESCE(TECNOLOGIA_DIA_1,0)+COALESCE(TECNOLOGIA_DIA_2,0)+COALESCE(TECNOLOGIA_DIA_3,0)+COALESCE(TECNOLOGIA_DIA_4,0)+COALESCE(TECNOLOGIA_DIA_5,0)+COALESCE(TECNOLOGIA_DIA_6,0)+COALESCE(TECNOLOGIA_DIA_7,0)) AS TECNOLOGIA_SUM,(COALESCE(TIENDAS_Y_ALMACENES_DIA_1,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_2,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_3,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_4,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_5,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_6,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_7,0)) AS TIENDAS_Y_ALMACENES_SUM,(COALESCE(TRANSPORTE_DIA_1,0)+COALESCE(TRANSPORTE_DIA_2,0)+COALESCE(TRANSPORTE_DIA_3,0)+COALESCE(TRANSPORTE_DIA_4,0)+COALESCE(TRANSPORTE_DIA_5,0)+COALESCE(TRANSPORTE_DIA_6,0)+COALESCE(TRANSPORTE_DIA_7,0)) AS TRANSPORTE_SUM , (COALESCE(VIAJES_Y_TURISMO_DIA_1,0)+COALESCE(VIAJES_Y_TURISMO_DIA_2,0)+COALESCE(VIAJES_Y_TURISMO_DIA_3,0)+COALESCE(VIAJES_Y_TURISMO_DIA_4,0)+COALESCE(VIAJES_Y_TURISMO_DIA_5,0)+COALESCE(VIAJES_Y_TURISMO_DIA_6,0)+COALESCE(VIAJES_Y_TURISMO_DIA_7,0)) AS VIAJES_Y_TURISMO_SUM FROM `credibancoml.Recomienda.Prueba` WHERE NU_TARJETA_SHA256 IN ("' + '","'.join(tuple(tokens)) + '")) AS BASE_1'
                                    query_job = client.query(consulta)
                                    # Convierto los resultados en pandas data.frame
                                    DATOS = query_job.to_dataframe()
                                    if (DATOS.shape[0] != 0):
                                        # ========================= Funcion 2: Transformacion de la tabla
                                        BINES = pd.read_excel("archivos/BINES.xlsx")
                                        datos = pd.merge(DATOS, BINES, left_on='BIN', right_on='CD_BIN', how='left')
                                        datos_tot = datos[["NU_TARJETA_SHA256", "CONTEO_FREQ", "ANTIGUEDAD", "DIF_TIEMPO_UC", "CAT_FREQ",
                                             "PRO_MENS_COMPRAS", "PRO_MENS_ESTBL",
                                             "CIUDAD_PRINCIPAL", "NM_BIN", "NM_PRODUCTO"]]
                                        # Se agrega la zona horaria de Colombia
                                        tz = pytz.timezone('America/Bogota')
                                        Fecha = datetime.datetime.now(tz=tz).strftime('%Y/%m/%d  %H:%M:%S')
                                        # ========================= Funcion 4: Devuelve las predicciones en un .json
                                        tarjetas_nombre = pd.DataFrame(datos_tot['NU_TARJETA_SHA256'].drop_duplicates()).reset_index(drop=True)
                                        json_data = []  # Crea una lista vacia para guardar los resultados
                                        for i in range(tarjetas_nombre.shape[0]):
                                            df_tarjeta_selec = datos_tot.loc[
                                                datos_tot['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i, 0]].drop(
                                                ['NU_TARJETA_SHA256', 'CONTEO_FREQ'], axis=1)
                                            df_tarjeta_selec = df_tarjeta_selec.fillna("NULL")
                                            json_1 = pd.DataFrame(
                                                {'NU_TARJETA_SHA256': [tarjetas_nombre.loc[i, :][0]]}).to_json(orient='records')
                                            json1_data = json.loads(json_1)[0]
                                            json_2 = df_tarjeta_selec.to_json(orient='records')
                                            json2_data = json.loads(json_2)[0]
                                            json1_data["Valores"] = json2_data
                                            co = json.dumps(json1_data)
                                            json_tarjeta = json.loads(co)
                                            json_data.append(json_tarjeta)
                                        # Reultado final en formarto .json
                                        my_tab = pd.crosstab(index=datos_tot['CIUDAD_PRINCIPAL'], columns="count").sort_values(by=['count'], ascending=False)
                                        Lista = list(my_tab['count'])
                                        ciudad = int(my_tab.reset_index()['CIUDAD_PRINCIPAL'][Lista.index(max(Lista))])

                                        CAT_FREQ_1 = datos_tot[['CAT_FREQ', 'CONTEO_FREQ']].groupby(['CAT_FREQ']).sum().sort_values(by=['CONTEO_FREQ'], ascending=False)
                                        Lista = list(CAT_FREQ_1['CONTEO_FREQ'])
                                        CAT_FREQ_VAL = CAT_FREQ_1.reset_index()['CAT_FREQ'][Lista.index(max(Lista))]

                                        summari = pd.DataFrame({'Antiguedad': [max(datos_tot['ANTIGUEDAD'])],
                                                                'Dif_tiempo_UC': [min(datos_tot['DIF_TIEMPO_UC'])],
                                                                'Pro_mens_compras': [np.mean(datos_tot['PRO_MENS_COMPRAS'])],
                                                                'Pro_mens_estbl': [np.mean(datos_tot['PRO_MENS_ESTBL'])],
                                                                'Ciudad Principal': [ciudad], 'Categoria': [CAT_FREQ_VAL]})

                                        # Reultado final en formarto .json
                                        json_data_2 = [ {'Fecha': Fecha, 'General': json.loads(summari.to_json(orient='records'))[0],'Tarjeta': json_data[0:tarjetas_nombre.shape[0]]}]

                                        respuesta =json.dumps(json_data_2)
                                    else: respuesta= "5"
                                else:respuesta = "0"
                            else: respuesta = "1"
                        else: respuesta = "2"
                    else: respuesta = "3"
                else: respuesta = "4"
            else: respuesta = "7"
        else: respuesta = "6"
        print(respuesta)
        return(respuesta)

# Cargo las librerias
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import datetime
import pytz
import json
import hashlib
import psycopg2
import math

# Agrego las credenciales
#credentials = service_account.Credentials.from_service_account_file('credenciales.json')
#project_id = 'credibancoml'
# Defino el cliente en BigQuery
#client = bigquery.Client(credentials= credentials,project=project_id)
connection = psycopg2.connect(user = "postgres",
                                  password = "Fncalderon91G",
                                  host = "35.232.151.83",
                                  port = "5432",
                                  database = "postgres")

cursor = connection.cursor()


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
                                    #consulta = 'SELECT  NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,CASE WHEN CAT_FREQ="CINE Y TEATRO" THEN CINE_Y_TEATRO_SUM WHEN CAT_FREQ="DISCOTECA Y BARES" THEN DISCOTECA_Y_BARES_SUM  WHEN CAT_FREQ="DROGUERIAS" THEN DROGUERIAS_SUM WHEN CAT_FREQ="EDS" THEN EDS_SUM WHEN CAT_FREQ="EDUCACION" THEN EDUCACION_SUM WHEN CAT_FREQ="ENTRETENIMIENTO" THEN ENTRETENIMIENTO_SUM WHEN CAT_FREQ="EVENTOS" THEN EVENTOS_SUM WHEN CAT_FREQ="GIMNASIOS" THEN GIMNASIOS_SUM WHEN CAT_FREQ="HOGAR" THEN HOGAR_SUM WHEN CAT_FREQ="MASCOTAS" THEN MASCOTAS_SUM WHEN CAT_FREQ="MI ARMARIO" THEN MI_ARMARIO_SUM WHEN CAT_FREQ="OTROS" THEN OTROS_SUM WHEN CAT_FREQ="PARQUEADEROS" THEN PARQUEADEROS_SUM WHEN CAT_FREQ="PELUQUERIAS" THEN PELUQUERIAS_SUM WHEN CAT_FREQ="RESTAURANTES" THEN RESTAURANTES_SUM WHEN CAT_FREQ="SECTOR FINANCIERO Y ASEGURADOR" THEN SECTOR_FINANCIERO_Y_ASEGURADOR_SUM WHEN CAT_FREQ="SERVICIOS" THEN SERVICIOS_SUM WHEN CAT_FREQ="SUPERMERCADOS" THEN SUPERMERCADOS_SUM WHEN CAT_FREQ="TECNOLOGIA" THEN TECNOLOGIA_SUM WHEN CAT_FREQ="TIENDAS Y ALMACENES" THEN TIENDAS_Y_ALMACENES_SUM WHEN CAT_FREQ="TRANSPORTE" THEN TRANSPORTE_SUM WHEN CAT_FREQ="VIAJES Y TURISMO" THEN VIAJES_Y_TURISMO_SUM  END AS CONTEO_FREQ    FROM( SELECT NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,(COALESCE(CINE_Y_TEATRO_DIA_1,0)+COALESCE(CINE_Y_TEATRO_DIA_2,0)+COALESCE(CINE_Y_TEATRO_DIA_3,0)+COALESCE(CINE_Y_TEATRO_DIA_4,0)+COALESCE(CINE_Y_TEATRO_DIA_5,0)+COALESCE(CINE_Y_TEATRO_DIA_6,0)+COALESCE(CINE_Y_TEATRO_DIA_7,0)) AS CINE_Y_TEATRO_SUM,(COALESCE(DISCOTECA_Y_BARES_DIA_1,0)+COALESCE(DISCOTECA_Y_BARES_DIA_2,0)+COALESCE(DISCOTECA_Y_BARES_DIA_3,0)+COALESCE(DISCOTECA_Y_BARES_DIA_4,0)+COALESCE(DISCOTECA_Y_BARES_DIA_5,0)+COALESCE(DISCOTECA_Y_BARES_DIA_6,0)+COALESCE(DISCOTECA_Y_BARES_DIA_7,0)) AS DISCOTECA_Y_BARES_SUM,    (COALESCE(DROGUERIAS_DIA_1,0)+COALESCE(DROGUERIAS_DIA_2,0)+COALESCE(DROGUERIAS_DIA_3,0)+COALESCE(DROGUERIAS_DIA_4,0)+COALESCE(DROGUERIAS_DIA_5,0)+COALESCE(DROGUERIAS_DIA_6,0)+COALESCE(DROGUERIAS_DIA_7,0)) AS DROGUERIAS_SUM,(COALESCE(EDS_DIA_1,0)+COALESCE(EDS_DIA_2,0)+COALESCE(EDS_DIA_3,0)+COALESCE(EDS_DIA_4,0)+COALESCE(EDS_DIA_5,0)+COALESCE(EDS_DIA_6,0)+COALESCE(EDS_DIA_7,0) ) AS EDS_SUM,(COALESCE(EDUCACION_DIA_1,0)+COALESCE(EDUCACION_DIA_2,0)+COALESCE(EDUCACION_DIA_3,0)+COALESCE(EDUCACION_DIA_4,0)+COALESCE(EDUCACION_DIA_5,0)+COALESCE(EDUCACION_DIA_6,0)+COALESCE(EDUCACION_DIA_7,0)) AS EDUCACION_SUM,(COALESCE(ENTRETENIMIENTO_DIA_1,0)+COALESCE(ENTRETENIMIENTO_DIA_2,0)+COALESCE(ENTRETENIMIENTO_DIA_3,0)+COALESCE(ENTRETENIMIENTO_DIA_4,0)+COALESCE(ENTRETENIMIENTO_DIA_5,0)+COALESCE(ENTRETENIMIENTO_DIA_6,0)+COALESCE(ENTRETENIMIENTO_DIA_7,0)) AS ENTRETENIMIENTO_SUM,(COALESCE(MI_ARMARIO_DIA_1,0)+COALESCE(MI_ARMARIO_DIA_2,0)+COALESCE(MI_ARMARIO_DIA_3,0)+COALESCE(MI_ARMARIO_DIA_4,0)+COALESCE(MI_ARMARIO_DIA_5,0)+COALESCE(MI_ARMARIO_DIA_6,0)+COALESCE(MI_ARMARIO_DIA_7,0)) AS MI_ARMARIO_SUM,(COALESCE(EVENTOS_DIA_1,0)+COALESCE(EVENTOS_DIA_2,0)+COALESCE(EVENTOS_DIA_3,0)+COALESCE(EVENTOS_DIA_4,0)+COALESCE(EVENTOS_DIA_5,0)+COALESCE(EVENTOS_DIA_6,0)+COALESCE(EVENTOS_DIA_7,0)) AS EVENTOS_SUM,(COALESCE(OTROS_DIA_1,0)+COALESCE(OTROS_DIA_2,0)+COALESCE(OTROS_DIA_3,0)+COALESCE(OTROS_DIA_4,0)+COALESCE(OTROS_DIA_5,0)+COALESCE(OTROS_DIA_6,0)+COALESCE(OTROS_DIA_7,0)) AS OTROS_SUM,(COALESCE(GIMNASIOS_DIA_1,0)+COALESCE(GIMNASIOS_DIA_2,0)+COALESCE(GIMNASIOS_DIA_3,0)+COALESCE(GIMNASIOS_DIA_4,0)+COALESCE(GIMNASIOS_DIA_5,0)+COALESCE(GIMNASIOS_DIA_6,0)+COALESCE(GIMNASIOS_DIA_7,0)) AS GIMNASIOS_SUM,(COALESCE(PARQUEADEROS_DIA_1,0)+COALESCE(PARQUEADEROS_DIA_2,0)+COALESCE(PARQUEADEROS_DIA_3,0)+COALESCE(PARQUEADEROS_DIA_4,0)+COALESCE(PARQUEADEROS_DIA_5,0)+COALESCE(PARQUEADEROS_DIA_6,0)+COALESCE(PARQUEADEROS_DIA_7,0)  ) AS PARQUEADEROS_SUM,(COALESCE(HOGAR_DIA_1,0)+COALESCE(HOGAR_DIA_2,0)+COALESCE(HOGAR_DIA_3,0)+COALESCE(HOGAR_DIA_4,0)+COALESCE(HOGAR_DIA_5,0)+COALESCE(HOGAR_DIA_6,0)+COALESCE(HOGAR_DIA_7,0)) AS HOGAR_SUM,(COALESCE(MASCOTAS_DIA_1,0)+COALESCE(MASCOTAS_DIA_2,0)+COALESCE(MASCOTAS_DIA_3,0)+COALESCE(MASCOTAS_DIA_4,0)+COALESCE(MASCOTAS_DIA_5,0)+COALESCE(MASCOTAS_DIA_6,0)+COALESCE(MASCOTAS_DIA_7,0)) AS MASCOTAS_SUM,(COALESCE(PELUQUERIAS_DIA_1,0)+COALESCE(PELUQUERIAS_DIA_2,0)+COALESCE(PELUQUERIAS_DIA_3,0)+COALESCE(PELUQUERIAS_DIA_4,0)+COALESCE(PELUQUERIAS_DIA_5,0)+COALESCE(PELUQUERIAS_DIA_6,0)+COALESCE(PELUQUERIAS_DIA_7,0)) AS PELUQUERIAS_SUM,(COALESCE(RESTAURANTES_DIA_1,0)+COALESCE(RESTAURANTES_DIA_2,0)+COALESCE(RESTAURANTES_DIA_3,0)+COALESCE(RESTAURANTES_DIA_4,0)+COALESCE(RESTAURANTES_DIA_5,0)+COALESCE(RESTAURANTES_DIA_6,0)+COALESCE(RESTAURANTES_DIA_7,0)) AS RESTAURANTES_SUM,(COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_1,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_2,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_3,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_4,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_5,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_6,0)+COALESCE(SECTOR_FINANCIERO_Y_ASEGURADOR_DIA_7,0)) AS SECTOR_FINANCIERO_Y_ASEGURADOR_SUM,(COALESCE(SERVICIOS_DIA_1,0)+COALESCE(SERVICIOS_DIA_2,0)+COALESCE(SERVICIOS_DIA_3,0)+COALESCE(SERVICIOS_DIA_4,0)+COALESCE(SERVICIOS_DIA_5,0)+COALESCE(SERVICIOS_DIA_6,0)+COALESCE(SERVICIOS_DIA_7,0)) AS SERVICIOS_SUM,(COALESCE(SUPERMERCADOS_DIA_1,0)+COALESCE(SUPERMERCADOS_DIA_2,0)+COALESCE(SUPERMERCADOS_DIA_3,0)+COALESCE(SUPERMERCADOS_DIA_4,0)+COALESCE(SUPERMERCADOS_DIA_5,0)+COALESCE(SUPERMERCADOS_DIA_6,0)+COALESCE(SUPERMERCADOS_DIA_7,0)) AS SUPERMERCADOS_SUM,(COALESCE(TECNOLOGIA_DIA_1,0)+COALESCE(TECNOLOGIA_DIA_2,0)+COALESCE(TECNOLOGIA_DIA_3,0)+COALESCE(TECNOLOGIA_DIA_4,0)+COALESCE(TECNOLOGIA_DIA_5,0)+COALESCE(TECNOLOGIA_DIA_6,0)+COALESCE(TECNOLOGIA_DIA_7,0)) AS TECNOLOGIA_SUM,(COALESCE(TIENDAS_Y_ALMACENES_DIA_1,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_2,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_3,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_4,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_5,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_6,0)+COALESCE(TIENDAS_Y_ALMACENES_DIA_7,0)) AS TIENDAS_Y_ALMACENES_SUM,(COALESCE(TRANSPORTE_DIA_1,0)+COALESCE(TRANSPORTE_DIA_2,0)+COALESCE(TRANSPORTE_DIA_3,0)+COALESCE(TRANSPORTE_DIA_4,0)+COALESCE(TRANSPORTE_DIA_5,0)+COALESCE(TRANSPORTE_DIA_6,0)+COALESCE(TRANSPORTE_DIA_7,0)) AS TRANSPORTE_SUM , (COALESCE(VIAJES_Y_TURISMO_DIA_1,0)+COALESCE(VIAJES_Y_TURISMO_DIA_2,0)+COALESCE(VIAJES_Y_TURISMO_DIA_3,0)+COALESCE(VIAJES_Y_TURISMO_DIA_4,0)+COALESCE(VIAJES_Y_TURISMO_DIA_5,0)+COALESCE(VIAJES_Y_TURISMO_DIA_6,0)+COALESCE(VIAJES_Y_TURISMO_DIA_7,0)) AS VIAJES_Y_TURISMO_SUM FROM `credibancoml.Recomienda.Prueba` WHERE NU_TARJETA_SHA256 IN ("' + '","'.join(tuple(tokens)) + '")) AS BASE_1'
                                    #query_job = client.query(consulta)
                                    # Convierto los resultados en pandas data.frame
                                    #DATOS = query_job.to_dataframe()
                                    consulta = "SELECT  NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,NU_TRX_MES_1,NU_TRX_MES_2,NU_TRX_MES_3,NU_TRX_MES_4,NU_TRX_MES_5,NU_TRX_MES_6,TOT_TRX_MES_1,TOT_TRX_MES_2,TOT_TRX_MES_3,TOT_TRX_MES_4,TOT_TRX_MES_5,TOT_TRX_MES_6,PERCENTIL_NU_TRX,PERCENTIL_VL_TRX,TRX_TOTAL,VALOR_TOTAL,CAT_TOP_1 ,CAT_TOP_2,CAT_TOP_3,CAT_TOP_4,CAT_TOP_5,NUM_TRX_CAT_TOP_1,NUM_TRX_CAT_TOP_2,NUM_TRX_CAT_TOP_3,NUM_TRX_CAT_TOP_4,NUM_TRX_CAT_TOP_5,VL_CAT_TOP_1,VL_CAT_TOP_2,VL_CAT_TOP_3,VL_CAT_TOP_4,VL_CAT_TOP_5, CASE  WHEN CAT_FREQ='CINE Y TEATRO' THEN CINE_Y_TEATRO_SUM WHEN CAT_FREQ='DISCOTECA Y BARES' THEN DISCOTECA_Y_BARES_SUM WHEN CAT_FREQ='DROGUERIAS' THEN DROGUERIAS_SUM WHEN CAT_FREQ='EDS' THEN EDS_SUM WHEN CAT_FREQ='EDUCACION' THEN EDUCACION_SUM WHEN CAT_FREQ='ENTRETENIMIENTO' THEN ENTRETENIMIENTO_SUM WHEN CAT_FREQ='EVENTOS' THEN EVENTOS_SUM WHEN CAT_FREQ='GIMNASIOS' THEN GIMNASIOS_SUM WHEN CAT_FREQ='HOGAR' THEN HOGAR_SUM WHEN CAT_FREQ='MASCOTAS' THEN MASCOTAS_SUM WHEN CAT_FREQ='MI ARMARIO' THEN MI_ARMARIO_SUM WHEN CAT_FREQ='OTROS' THEN OTROS_SUM WHEN CAT_FREQ='PARQUEADEROS' THEN PARQUEADEROS_SUM WHEN CAT_FREQ='PELUQUERIAS' THEN PELUQUERIAS_SUM WHEN CAT_FREQ='RESTAURANTES' THEN RESTAURANTES_SUM WHEN CAT_FREQ='SECTOR FINANCIERO Y ASEGURADOR' THEN SECTOR_FINANCIERO_Y_ASEGURADOR_SUM WHEN CAT_FREQ='SERVICIOS' THEN SERVICIOS_SUM WHEN CAT_FREQ='SUPERMERCADOS' THEN SUPERMERCADOS_SUM WHEN CAT_FREQ='TECNOLOGIA' THEN TECNOLOGIA_SUM WHEN CAT_FREQ='TIENDAS Y ALMACENES' THEN TIENDAS_Y_ALMACENES_SUM WHEN CAT_FREQ='TRANSPORTE' THEN TRANSPORTE_SUM WHEN CAT_FREQ='VIAJES Y TURISMO' THEN VIAJES_Y_TURISMO_SUM  END AS CONTEO_FREQ FROM( SELECT NU_TARJETA_SHA256,BIN,ANTIGUEDAD,DIF_TIEMPO_UC,PRO_MENS_COMPRAS,CIUDAD_PRINCIPAL,PRO_MENS_ESTBL,CAT_FREQ,NU_TRX_MES_1 ,NU_TRX_MES_2,NU_TRX_MES_3,NU_TRX_MES_4,NU_TRX_MES_5,NU_TRX_MES_6,CAST(TOT_TRX_MES_1 AS NUMERIC) AS TOT_TRX_MES_1,CAST(TOT_TRX_MES_2 AS NUMERIC) AS TOT_TRX_MES_2,CAST(TOT_TRX_MES_3 AS NUMERIC) AS TOT_TRX_MES_3,CAST(TOT_TRX_MES_4 AS NUMERIC) AS TOT_TRX_MES_4,CAST(TOT_TRX_MES_5 AS NUMERIC) AS TOT_TRX_MES_5, CAST(TOT_TRX_MES_6 AS NUMERIC) AS TOT_TRX_MES_6,PERCENTIL_NU_TRX,PERCENTIL_VL_TRX,TRX_TOTAL,CAST(VALOR_TOTAL AS NUMERIC) AS VALOR_TOTAL,CAT_TOP_1 ,CAT_TOP_2,CAT_TOP_3,CAT_TOP_4,CAT_TOP_5,NUM_TRX_CAT_TOP_1,NUM_TRX_CAT_TOP_2,NUM_TRX_CAT_TOP_3,NUM_TRX_CAT_TOP_4,NUM_TRX_CAT_TOP_5,CAST(VL_CAT_TOP_1 AS NUMERIC) AS VL_CAT_TOP_1,CAST(VL_CAT_TOP_2 AS NUMERIC) AS VL_CAT_TOP_2,CAST(VL_CAT_TOP_3 AS NUMERIC) AS VL_CAT_TOP_3,CAST(VL_CAT_TOP_4 AS NUMERIC) AS VL_CAT_TOP_4,CAST(VL_CAT_TOP_5 AS NUMERIC) AS VL_CAT_TOP_5, (COALESCE(CAST(cine_y_teatro_dia_1 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_2 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_3 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_4 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_5 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_6 AS NUMERIC),0)+COALESCE (CAST(cine_y_teatro_dia_7  AS NUMERIC),0)) AS CINE_Y_TEATRO_SUM,(COALESCE(CAST(discoteca_y_bares_dia_1 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_2 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_3 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_4 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_5 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_6 AS NUMERIC),0)+COALESCE (CAST(discoteca_y_bares_dia_7  AS NUMERIC),0)) AS DISCOTECA_Y_BARES_SUM,    (COALESCE(CAST(droguerias_dia_1 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_2 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_3 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_4 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_5 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_6 AS NUMERIC),0)+COALESCE (CAST(droguerias_dia_7  AS NUMERIC),0)) AS DROGUERIAS_SUM,(COALESCE(CAST(eds_dia_1 AS NUMERIC),0)+COALESCE (CAST(eds_dia_2 AS NUMERIC),0)+COALESCE (CAST(eds_dia_3 AS NUMERIC),0)+COALESCE (CAST(eds_dia_4 AS NUMERIC),0)+COALESCE (CAST(eds_dia_5 AS NUMERIC),0)+COALESCE (CAST(eds_dia_6 AS NUMERIC),0)+COALESCE (CAST(eds_dia_7 AS NUMERIC),0) ) AS EDS_SUM,(COALESCE(CAST(educacion_dia_1 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_2 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_3 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_4 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_5 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_6 AS NUMERIC),0)+COALESCE (CAST(educacion_dia_7  AS NUMERIC),0)) AS EDUCACION_SUM,(COALESCE(CAST(entretenimiento_dia_1 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_2 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_3 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_4 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_5 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_6 AS NUMERIC),0)+COALESCE (CAST(entretenimiento_dia_7  AS NUMERIC),0)) AS ENTRETENIMIENTO_SUM,(COALESCE(CAST(mi_armario_dia_1 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_2 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_3 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_4 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_5 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_6 AS NUMERIC),0)+COALESCE (CAST(mi_armario_dia_7  AS NUMERIC),0)) AS MI_ARMARIO_SUM,(COALESCE(CAST(eventos_dia_1 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_2 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_3 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_4 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_5 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_6 AS NUMERIC),0)+COALESCE (CAST(eventos_dia_7  AS NUMERIC),0)) AS EVENTOS_SUM,(COALESCE(CAST(otros_dia_1 AS NUMERIC),0)+COALESCE (CAST(otros_dia_2 AS NUMERIC),0)+COALESCE (CAST(otros_dia_3 AS NUMERIC),0)+COALESCE (CAST(otros_dia_4 AS NUMERIC),0)+COALESCE (CAST(otros_dia_5 AS NUMERIC),0)+COALESCE (CAST(otros_dia_6 AS NUMERIC),0)+COALESCE (CAST(otros_dia_7  AS NUMERIC),0)) AS OTROS_SUM,(COALESCE(CAST(gimnasios_dia_1 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_2 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_3 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_4 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_5 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_6 AS NUMERIC),0)+COALESCE (CAST(gimnasios_dia_7  AS NUMERIC),0)) AS GIMNASIOS_SUM,(COALESCE(CAST(parqueaderos_dia_1 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_2 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_3 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_4 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_5 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_6 AS NUMERIC),0)+COALESCE (CAST(parqueaderos_dia_7 AS NUMERIC),0)  ) AS PARQUEADEROS_SUM,(COALESCE(CAST(hogar_dia_1 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_2 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_3 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_4 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_5 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_6 AS NUMERIC),0)+COALESCE (CAST(hogar_dia_7  AS NUMERIC),0)) AS HOGAR_SUM,(COALESCE(CAST(mascotas_dia_1 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_2 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_3 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_4 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_5 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_6 AS NUMERIC),0)+COALESCE (CAST(mascotas_dia_7  AS NUMERIC),0)) AS MASCOTAS_SUM,(COALESCE(CAST(peluquerias_dia_1 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_2 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_3 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_4 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_5 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_6 AS NUMERIC),0)+COALESCE (CAST(peluquerias_dia_7  AS NUMERIC),0)) AS PELUQUERIAS_SUM,(COALESCE(CAST(restaurantes_dia_1 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_2 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_3 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_4 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_5 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_6 AS NUMERIC),0)+COALESCE (CAST(restaurantes_dia_7  AS NUMERIC),0)) AS RESTAURANTES_SUM,(COALESCE(CAST(sector_financiero_y_asegurador_dia_1 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_2 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_3 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_4 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_5 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_6 AS NUMERIC),0)+COALESCE (CAST(sector_financiero_y_asegurador_dia_7  AS NUMERIC),0)) AS SECTOR_FINANCIERO_Y_ASEGURADOR_SUM,(COALESCE(CAST(servicios_dia_1 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_2 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_3 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_4 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_5 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_6 AS NUMERIC),0)+COALESCE (CAST(servicios_dia_7  AS NUMERIC),0)) AS SERVICIOS_SUM,(COALESCE(CAST(supermercados_dia_1 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_2 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_3 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_4 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_5 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_6 AS NUMERIC),0)+COALESCE (CAST(supermercados_dia_7  AS NUMERIC),0)) AS SUPERMERCADOS_SUM,(COALESCE(CAST(tecnologia_dia_1 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_2 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_3 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_4 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_5 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_6 AS NUMERIC),0)+COALESCE (CAST(tecnologia_dia_7  AS NUMERIC),0)) AS TECNOLOGIA_SUM,(COALESCE(CAST(tiendas_y_almacenes_dia_1 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_2 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_3 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_4 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_5 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_6 AS NUMERIC),0)+COALESCE (CAST(tiendas_y_almacenes_dia_7  AS NUMERIC),0)) AS TIENDAS_Y_ALMACENES_SUM,(COALESCE(CAST(transporte_dia_1 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_2 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_3 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_4 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_5 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_6 AS NUMERIC),0)+COALESCE (CAST(transporte_dia_7  AS NUMERIC),0)) AS TRANSPORTE_SUM , (COALESCE(CAST(VIAJES_Y_TURISMO_DIA_1 AS NUMERIC),0)+COALESCE (CAST(VIAJES_Y_TURISMO_DIA_2 AS NUMERIC),0)+COALESCE (CAST(VIAJES_Y_TURISMO_DIA_3 AS NUMERIC),0)+COALESCE (CAST(VIAJES_Y_TURISMO_DIA_4 AS NUMERIC),0)+COALESCE (CAST(VIAJES_Y_TURISMO_DIA_5 AS NUMERIC),0)+COALESCE (CAST(VIAJES_Y_TURISMO_DIA_6 AS NUMERIC),0)+COALESCE(CAST(VIAJES_Y_TURISMO_DIA_7 AS NUMERIC),0)) AS VIAJES_Y_TURISMO_SUM FROM recomienda WHERE NU_TARJETA_SHA256 IN ('" + "','".join(tuple(tokens)) + "')) AS BASE_1"
                                    cursor = connection.cursor()
                                    cursor.execute(consulta)
                                    DATOS = pd.DataFrame(cursor.fetchall())
                                    if (DATOS.shape[0] != 0):

                                        colnames_1 = [desc[0] for desc in cursor.description]
                                        colnames = [colum.upper() for colum in colnames_1]
                                        DATOS.columns = colnames
                                        # ========================= Funcion 2: Transformacion de la tabla
                                        BINES = pd.read_excel("archivos/BINES.xlsx")
                                        DATOS['BIN'] = DATOS['BIN'].astype(int)
                                        datos = pd.merge(DATOS, BINES, left_on='BIN', right_on='CD_BIN', how='left')

                                        datos_tot = datos[
                                            ["NU_TARJETA_SHA256", "CONTEO_FREQ", "ANTIGUEDAD", "DIF_TIEMPO_UC",
                                             "CAT_FREQ",
                                             "PRO_MENS_COMPRAS", "PRO_MENS_ESTBL", "NU_TRX_MES_1", "NU_TRX_MES_2",
                                             "NU_TRX_MES_3", "NU_TRX_MES_4",
                                             "NU_TRX_MES_5", "NU_TRX_MES_6", "TOT_TRX_MES_1", "TOT_TRX_MES_2",
                                             "TOT_TRX_MES_3", "TOT_TRX_MES_4", "TOT_TRX_MES_5",
                                             "TOT_TRX_MES_6", "PERCENTIL_NU_TRX", "PERCENTIL_VL_TRX", "TRX_TOTAL",
                                             "VALOR_TOTAL", "CAT_TOP_1", "CAT_TOP_2",
                                             "CAT_TOP_3", "CAT_TOP_4", "CAT_TOP_5", "NUM_TRX_CAT_TOP_1",
                                             "NUM_TRX_CAT_TOP_2", "NUM_TRX_CAT_TOP_3", "NUM_TRX_CAT_TOP_4",
                                             "NUM_TRX_CAT_TOP_5", "VL_CAT_TOP_1", "VL_CAT_TOP_2", "VL_CAT_TOP_3",
                                             "VL_CAT_TOP_4", "VL_CAT_TOP_5",
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
                                            #------------------ informacion ultimos 6 meses
                                            json_3 = df_tarjeta_selec[[ "NU_TRX_MES_1", "NU_TRX_MES_2",
                                             "NU_TRX_MES_3", "NU_TRX_MES_4",
                                             "NU_TRX_MES_5", "NU_TRX_MES_6", "TOT_TRX_MES_1", "TOT_TRX_MES_2",
                                             "TOT_TRX_MES_3", "TOT_TRX_MES_4", "TOT_TRX_MES_5",
                                             "TOT_TRX_MES_6"]].to_json(orient='records')
                                            json3_data = json.loads(json_3)[0]
                                            #------------------ información top 5 cat
                                            json_4 = df_tarjeta_selec[[ "CAT_TOP_1", "CAT_TOP_2",
                                             "CAT_TOP_3", "CAT_TOP_4", "CAT_TOP_5", "NUM_TRX_CAT_TOP_1",
                                             "NUM_TRX_CAT_TOP_2", "NUM_TRX_CAT_TOP_3", "NUM_TRX_CAT_TOP_4",
                                             "NUM_TRX_CAT_TOP_5", "VL_CAT_TOP_1", "VL_CAT_TOP_2", "VL_CAT_TOP_3",
                                             "VL_CAT_TOP_4", "VL_CAT_TOP_5"]].to_json(orient='records')
                                            json4_data = json.loads(json_4)[0]
                                            #--------------------
                                            json_2 = df_tarjeta_selec[[ "ANTIGUEDAD", "DIF_TIEMPO_UC","CAT_FREQ",
                                                                        "PRO_MENS_COMPRAS", "PRO_MENS_ESTBL", "PERCENTIL_NU_TRX",
                                                                       "PERCENTIL_VL_TRX", "TRX_TOTAL","VALOR_TOTAL",
                                                                       "CIUDAD_PRINCIPAL", "NM_BIN", "NM_PRODUCTO"]].to_json(orient='records')
                                            json2_data = json.loads(json_2)[0]
                                            # agrupar en valores la infor de los ultimos medes y top5---
                                            json2_data["Ultimos 6 Meses"] = json3_data
                                            json2_data["Top 5"] = json4_data
                                            #----------------------------
                                            json1_data["Valores"] = json2_data
                                            co = json.dumps(json1_data)
                                            json_tarjeta = json.loads(co)
                                            json_data.append(json_tarjeta)
                                        # Reultado final en formarto .json
                                        #datos_tot.replace(to_replace=[None], value= np.nan, inplace=True)
                                        datos_tot['CIUDAD_PRINCIPAL']=np.where(datos_tot['CIUDAD_PRINCIPAL'].isnull(), 0,datos_tot['CIUDAD_PRINCIPAL'])

                                        my_tab = pd.crosstab(index=datos_tot['CIUDAD_PRINCIPAL'], columns="count", dropna = False).sort_values(by=['count'], ascending=False)
                                        Lista = list(my_tab['count'])
                                        ciudad = int(my_tab.reset_index()['CIUDAD_PRINCIPAL'][Lista.index(max(Lista))])

                                        CAT_FREQ_1 = datos_tot[['CAT_FREQ', 'CONTEO_FREQ']].groupby(['CAT_FREQ']).sum().sort_values(by=['CONTEO_FREQ'], ascending=False)
                                        Lista = list(CAT_FREQ_1['CONTEO_FREQ'])
                                        CAT_FREQ_VAL = CAT_FREQ_1.reset_index()['CAT_FREQ'][Lista.index(max(Lista))]

                                        summari = pd.DataFrame({'Antiguedad': [max(datos_tot['ANTIGUEDAD'])],
                                                                'Dif_tiempo_UC': [min(datos_tot['DIF_TIEMPO_UC'])],
                                                                'Pro_mens_compras': [np.mean(datos_tot['PRO_MENS_COMPRAS'].astype(float))],
                                                                'Pro_mens_estbl': [np.mean(datos_tot['PRO_MENS_ESTBL'].astype(float))],
                                                                'Pro_Cantidad_Trx_Total': [np.mean(datos_tot['TRX_TOTAL'].astype(float))],
                                                                'Pro_VL_Total': [np.mean(datos_tot['VALOR_TOTAL'].astype(float))],
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
        return(respuesta)

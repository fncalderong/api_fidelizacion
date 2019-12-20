# -*- coding: utf-8 -*-
"""
Editor de Spyder

Este es un archivo temporal.
"""
# Cargo las librerias
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import numpy as np

# Defino la función que genera la data para el modelo
def datos_totales(tokens, Fecha, Bines):
    def BigQuery(TOKEN, FECHA):
        # Agrego las credenciales
        credentials = service_account.Credentials.from_service_account_file('C:/Users/credibanco_ml/PycharmProjects/Api_Modelo/credenciales.json')
        project_id = 'credibancoml'
        # Defino el cliente en BigQuery
        client = bigquery.Client(credentials= credentials,project=project_id)
        # Hago la consultas
        consulta = 'SELECT *, CAST(FECHA AS STRING) AS FECHA_D  FROM `credibancoml.Recomienda.Full` WHERE FECHA =' + str(FECHA) + ' AND NU_TARJETA_SHA256 IN ("' + '","'.join(tuple(tokens["NU_TARJETA_SHA256"].to_list())) + '") LIMIT 10'
        query_job = client.query(consulta)
        # Convierto los resultados en pandas data.frame
        df = query_job.to_dataframe()
        return(df)
    DATOS = BigQuery(TOKEN = tokens, FECHA = Fecha)
    ########## Uno los datos
    datos=pd.merge(DATOS, BINES, left_on = 'BIN', right_on = 'CD_BIN', how = 'left')
    datos['MAESTRO_ELECTRON']=np.where(datos.NM_PRODUCTO.isin(['MAESTRO','ELECTRON']),1,0)
    datos['ORO']=np.where(np.logical_or(datos['NM_PRODUCTO'].str.find('GOLD')!=-1 , datos['NM_PRODUCTO'].str.find('ORO')!=-1),1,0)
    datos['BLACK_UP']=np.where(np.logical_or( np.logical_or( datos['NM_PRODUCTO'].str.find('BLACK')!=-1 , datos['NM_PRODUCTO'].str.find('SIGNATURE')!=-1),datos['NM_PRODUCTO'].str.find('PLATINUM')!=-1),1,0)
    datos['INTERNACIONAL']=np.where( datos['NM_PRODUCTO'].str.find('INTERNACIONAL')!=-1 ,1,0)
    datos['EMPRESARIAL']=np.where(np.logical_or( np.logical_or( datos['NM_PRODUCTO'].str.find('EMPRESARIAL')!=-1 , datos['NM_PRODUCTO'].str.find('CORPORATIVA')!=-1),datos['NM_PRODUCTO'].str.find('BUSINESS')!=-1),1,0)
    ##########
    datos['SUPER_APP_UC_SUPERMERCADOS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="SUPERMERCADOS",1,0)
    datos['SUPER_APP_UC_RESTAURANTES']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="RESTAURANTES",1,0)
    datos['SUPER_APP_UC_EDS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="EDS",1,0)
    datos['SUPER_APP_UC_MI_ARMARIO']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="MI ARMARIO",1,0)
    datos['SUPER_APP_UC_DROGUERIAS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="DROGUERIAS",1,0)
    datos['SUPER_APP_UC_TIENDAS_ALMACENES']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="TIENDAS Y ALMACENES",1,0)
    datos['SUPER_APP_UC_TECNOLOGIA']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="TECNOLOGIA",1,0)
    datos['SUPER_APP_UC_VIAJES_TURISMO']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="VIAJES Y TURISMO",1,0)
    datos['SUPER_APP_UC_CINE_TEATRO']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="CINE Y TEATRO",1,0)
    datos['SUPER_APP_UC_HOGAR']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="HOGAR",1,0)
    datos['SUPER_APP_UC_ENTRETENIMIENTO']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="ENTRETENIMIENTO",1,0)
    datos['SUPER_APP_UC_DISCOTECA_BARES']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="DISCOTECA Y BARES",1,0)
    datos['SUPER_APP_UC_MASCOTAS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="MASCOTAS",1,0)
    datos['SUPER_APP_UC_PELUQUERIAS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="PELUQUERIAS",1,0)
    datos['SUPER_APP_UC_TRANSPORTE']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="TRANSPORTE",1,0)
    datos['SUPER_APP_UC_SERVICIOS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="SERVICIOS",1,0)
    datos['SUPER_APP_UC_EDUCACION']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="EDUCACION",1,0)
    datos['SUPER_APP_UC_SECTORFINANCIERO']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="SECTOR FINANCIERO Y ASEGURADOR",1,0)
    datos['SUPER_APP_UC_EVENTOS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="EVENTOS",1,0)
    datos['SUPER_APP_UC_GIMNASIOS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="GIMNASIOS",1,0)
    datos['SUPER_APP_UC_PARQUEADEROS']=np.where(datos['CATEGORIA_SUPER_APP_UC']=="PARQUEADEROS",1,0)
    ##########
    datos['FREQ_DESP_UC_SUPERMERCADOS']=np.where(datos['CAT_FREQ_DESP_UC']=="SUPERMERCADOS",1,0)
    datos['FREQ_DESP_UC_RESTAURANTES']=np.where(datos['CAT_FREQ_DESP_UC']=="RESTAURANTES",1,0)
    datos['FREQ_DESP_UC_NA']=np.where(datos['CAT_FREQ_DESP_UC']=="",1,0)
    datos['FREQ_DESP_UC_EDS']=np.where(datos['CAT_FREQ_DESP_UC']=="EDS",1,0)
    datos['FREQ_DESP_UC_MI_ARMARIO']=np.where(datos['CAT_FREQ_DESP_UC']=="MI ARMARIO",1,0)
    datos['FREQ_DESP_UC_DROGUERIAS']=np.where(datos['CAT_FREQ_DESP_UC']=="DROGUERIAS",1,0)
    datos['FREQ_DESP_UC_TIENDAS_ALMACENES']=np.where(datos['CAT_FREQ_DESP_UC']=="TIENDAS Y ALMACENES",1,0)
    datos['FREQ_DESP_UC_TECNOLOGIA']=np.where(datos['CAT_FREQ_DESP_UC']=="TECNOLOGIA",1,0)
    datos['FREQ_DESP_UC_VIAJES_TURISMO']=np.where(datos['CAT_FREQ_DESP_UC']=="VIAJES Y TURISMO",1,0)
    datos['FREQ_DESP_UC_CINE_TEATRO']=np.where(datos['CAT_FREQ_DESP_UC']=="CINE Y TEATRO",1,0)
    datos['FREQ_DESP_UC_HOGAR']=np.where(datos['CAT_FREQ_DESP_UC']=="HOGAR",1,0)
    datos['FREQ_DESP_UC_ENTRETENIMIENTO']=np.where(datos['CAT_FREQ_DESP_UC']=="ENTRETENIMIENTO",1,0)
    datos['FREQ_DESP_UC_DISCOTECA_BARES']=np.where(datos['CAT_FREQ_DESP_UC']=="DISCOTECA Y BARES",1,0)
    datos['FREQ_DESP_UC_MASCOTAS']=np.where(datos['CAT_FREQ_DESP_UC']=="MASCOTAS",1,0)
    datos['FREQ_DESP_UC_PELUQUERIAS']=np.where(datos['CAT_FREQ_DESP_UC']=="PELUQUERIAS",1,0)
    datos['FREQ_DESP_UC_TRANSPORTE']=np.where(datos['CAT_FREQ_DESP_UC']=="TRANSPORTE",1,0)
    datos['FREQ_DESP_UC_SERVICIOS']=np.where(datos['CAT_FREQ_DESP_UC']=="SERVICIOS",1,0)
    datos['FREQ_DESP_UC_EDUCACION']=np.where(datos['CAT_FREQ_DESP_UC']=="EDUCACION",1,0)
    datos['FREQ_DESP_UC_SECTORFINANCIERO']=np.where(datos['CAT_FREQ_DESP_UC']=="SECTOR FINANCIERO Y ASEGURADOR",1,0)
    datos['FREQ_DESP_UC_EVENTOS']=np.where(datos['CAT_FREQ_DESP_UC']=="EVENTOS",1,0)
    datos['FREQ_DESP_UC_GIMNASIOS']=np.where(datos['CAT_FREQ_DESP_UC']=="GIMNASIOS",1,0)
    datos['FREQ_DESP_UC_PARQUEADEROS']=np.where(datos['CAT_FREQ_DESP_UC']=="PARQUEADEROS",1,0)
    ##########
    datos['CAT_FREQ_SUPERMERCADOS']=np.where(datos['CAT_FREQ']=="SUPERMERCADOS",1,0)
    datos['CAT_FREQ_RESTAURANTES']=np.where(datos['CAT_FREQ']=="RESTAURANTES",1,0)
    datos['CAT_FREQ_EDS']=np.where(datos['CAT_FREQ']=="EDS",1,0)
    datos['CAT_FREQ_MI_ARMARIO']=np.where(datos['CAT_FREQ']=="MI ARMARIO",1,0)
    datos['CAT_FREQ_DROGUERIAS']=np.where(datos['CAT_FREQ']=="DROGUERIAS",1,0)
    datos['CAT_FREQ_TIENDAS_ALMACENES']=np.where(datos['CAT_FREQ']=="TIENDAS Y ALMACENES",1,0)
    datos['CAT_FREQ_TECNOLOGIA']=np.where(datos['CAT_FREQ']=="TECNOLOGIA",1,0)
    datos['CAT_FREQ_VIAJES_TURISMO']=np.where(datos['CAT_FREQ']=="VIAJES Y TURISMO",1,0)
    datos['CAT_FREQ_CINE_TEATRO']=np.where(datos['CAT_FREQ']=="CINE Y TEATRO",1,0)
    datos['CAT_FREQ_HOGAR']=np.where(datos['CAT_FREQ']=="HOGAR",1,0)
    datos['CAT_FREQ_ENTRETENIMIENTO']=np.where(datos['CAT_FREQ']=="ENTRETENIMIENTO",1,0)
    datos['CAT_FREQ_DISCOTECA_BARES']=np.where(datos['CAT_FREQ']=="DISCOTECA Y BARES",1,0)
    datos['CAT_FREQ_MASCOTAS']=np.where(datos['CAT_FREQ']=="MASCOTAS",1,0)
    datos['CAT_FREQ_PELUQUERIAS']=np.where(datos['CAT_FREQ']=="PELUQUERIAS",1,0)
    datos['CAT_FREQ_TRANSPORTE']=np.where(datos['CAT_FREQ']=="TRANSPORTE",1,0)
    datos['CAT_FREQ_SERVICIOS']=np.where(datos['CAT_FREQ']=="SERVICIOS",1,0)
    datos['CAT_FREQ_EDUCACION']=np.where(datos['CAT_FREQ']=="EDUCACION",1,0)
    datos['CAT_FREQ_SECTORFINANCIERO']=np.where(datos['CAT_FREQ']=="SECTOR FINANCIERO Y ASEGURADOR",1,0)
    datos['CAT_FREQ_EVENTOS']=np.where(datos['CAT_FREQ']=="EVENTOS",1,0)
    datos['CAT_FREQ_GIMNASIOS']=np.where(datos['CAT_FREQ']=="GIMNASIOS",1,0)
    datos['CAT_FREQ_PARQUEADEROS']=np.where(datos['CAT_FREQ']=="PARQUEADEROS",1,0)
    ##########
    datos['DIA_CALC']=pd.DatetimeIndex(pd.to_datetime(datos['FECHA'].astype(str), format='%Y%m%d')).day
    datos['MES_CALC']=pd.DatetimeIndex(pd.to_datetime(datos['FECHA'].astype(str), format='%Y%m%d')).month
    ##########
    datos_tot = datos[["FECHA","Y3_COMP_HORA", "HORA_Y3", "CAT_FREQ_EDS",
            "CAT_FREQ_RESTAURANTES", "EDS", "RESTAURANTES", "FREQ_DESP_UC_SUPERMERCADOS",
            "DROGUERIAS", "SUPERMERCADOS", "PRO_MENS_COMPRAS", "MI_ARMARIO", "PRO_MENS_ESTBL",
            "CAT_FREQ_MI_ARMARIO", "SUPER_APP_UC_SUPERMERCADOS", "CAT_FREQ_SUPERMERCADOS",
            "DIF_TIEMPO_UC", "MES_CALC", "SUPERMERCADOS_DIA_4", "DIA_CALC", "CAT_FREQ_DROGUERIAS",
            "RESTAURANTES_DIA_6", "SUPERMERCADOS_DIA_7", "RESTAURANTES_DIA_5", "SUPERMERCADOS_DIA_3",
            "ANTIGUEDAD", "DIF_TIEMPO_UPC", "SUPERMERCADOS_DIA_6", "SUPERMERCADOS_DIA_2",
            "SUPERMERCADOS_DIA_1", "SUPERMERCADOS_DIA_5", "RESTAURANTES_DIA_3"]]
    return(datos_tot)


# Leo las tarjetas
Tarjetas = pd.read_json('archivos/tokens.json', orient='records')
Tarjetas

# Leo los Bines
BINES = pd.read_excel("archivos/BINES.xlsx")
BINES

data = datos_totales(tokens = Tarjetas, Fecha = 20181123, Bines = BINES)
data
data.dtypes

modelo = xgb.Booster({'nthread': 4})  # init model
modelo.load_model('archivos/xgb_lite.model')  # load data

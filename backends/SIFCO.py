import pandas as pd
import numpy as np
import json
import hashlib
import psycopg2
#from backends_subir_archivo.subir_info import subir

#subir().subir_informacion(json_data=json_postgres)
#d=subir().subir_informacion(json_data=json_postgres)
#d=subir(json_data=json_postgres)


connection = psycopg2.connect(user = "postgres",
                              password = "Fncalderon91G",
                              host = "35.232.151.83",
                              port = "5432",
                              database = "postgres")

cursor = connection.cursor()
#https://pynative.com/python-postgresql-insert-update-delete-table-data-to-perform-crud-operations/ #URL para las consultas en postgress
# Defino la función que genera la data para el modelo
class SIFCO:
    def __init__(self, results):
        self.client = []
    #def sifco(self, results):
        tokens = results.get("tokens")
        info_tarjetas = results.get("info_tarjetas")
        if type(info_tarjetas) == list:
            if len(info_tarjetas) != 0:
                if (info_tarjetas[0] == 0) | (info_tarjetas[0] == 1):
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
                                                                                # ========================= Funcion 4: Devuelve las predicciones en un .json
                                        tarjetas_nombre = pd.DataFrame(DATOS['NU_TARJETA_SHA256'].drop_duplicates()).reset_index(drop=True)
                                        json_data = []  # Crea una lista vacia para guardar los resultados
                                        json_postgres = [] # se crea una lista para almacenar la infoque se sube a la base de postgresql
                                        for i in range(tarjetas_nombre.shape[0]):
                                            df_tarjeta_selec = DATOS.loc[DATOS['NU_TARJETA_SHA256'] == tarjetas_nombre.iloc[i, 0]]
                                            df_tarjeta_selec = df_tarjeta_selec.fillna("NULL")
                                            # Crea el primer JSON con la info de la tarjeta y a que cluster pertenece
                                            json_1 = df_tarjeta_selec[['NU_TARJETA_SHA256','CLUSTER']].to_json(orient='records')
                                            json1_data = json.loads(json_1)[0]
                                            # Crea el segundo JSON con la info adicional general
                                            json_2 = df_tarjeta_selec[['MONTO_TRX_CLIENTE_HISTORICO','PROP_ACT_CLIENTE','PCKT_MCC_CANT_TRX','PCKT_MCC_TCK','ACTIVO_3M','PROP_QUANT_AMOUNT','NO_TRX_TIME','PCKT_QUANT_COMP_MCC','VIP_CARDHOLDER']].to_json(orient='records')
                                            json2_data = json.loads(json_2)[0]
                                            # se crea el JSON con la info historica
                                            json_3 = df_tarjeta_selec[['PERC_CANT_TRX_TOTAL_HISTORICO','PERC_CANT_CLIENTE_HISTORICO','PERC_CANT_TRX_COMPETENCIA_HISTORICO','PERC_MONTO_TRX_TOTAL_HISTORICO','PERC_MONTO_CLIENTE_HISTORICO','PERC_MONTO_TRX_COMPETENCIA_HISTORICO']].to_json(orient='records')
                                            json3_data = json.loads(json_3)[0]
                                            # se crea el JSON con la información en totales por mes
                                            json_4_a = df_tarjeta_selec[['CANT_TRX_TOTAL_MES_1','CANT_TRX_TOTAL_MES_2','CANT_TRX_TOTAL_MES_3','CANT_TRX_TOTAL_MES_4','CANT_TRX_TOTAL_MES_5','CANT_TRX_TOTAL_MES_6','MONTO_TRX_TOTAL_MES_1','MONTO_TRX_TOTAL_MES_2','MONTO_TRX_TOTAL_MES_3','MONTO_TRX_TOTAL_MES_4','MONTO_TRX_TOTAL_MES_5','MONTO_TRX_TOTAL_MES_6']].to_json(orient='records')
                                            json4_a_data = json.loads(json_4_a)[0]
                                            # se crea el JSON con la información en totales por cliente por mes
                                            json_4_b = df_tarjeta_selec[['CANT_TRX_CLIENTE_MES_1','CANT_TRX_CLIENTE_MES_2','CANT_TRX_CLIENTE_MES_3','CANT_TRX_CLIENTE_MES_4','CANT_TRX_CLIENTE_MES_5','CANT_TRX_CLIENTE_MES_6','MONTO_TRX_CLIENTE_MES_1','MONTO_TRX_CLIENTE_MES_2','MONTO_TRX_CLIENTE_MES_3','MONTO_TRX_CLIENTE_MES_4','MONTO_TRX_CLIENTE_MES_5','MONTO_TRX_CLIENTE_MES_6']].to_json(orient='records')
                                            json4_b_data = json.loads(json_4_b)[0]
                                            # se crea el JSON con la información en totales por competencia por mes
                                            json_4_c = df_tarjeta_selec[['CANT_TRX_COMPETENCIA_MES_1','CANT_TRX_COMPETENCIA_MES_2','CANT_TRX_COMPETENCIA_MES_3','CANT_TRX_COMPETENCIA_MES_4','CANT_TRX_COMPETENCIA_MES_5','CANT_TRX_COMPETENCIA_MES_6','MONTO_TRX_COMPETENCIA_MES_1','MONTO_TRX_COMPETENCIA_MES_2','MONTO_TRX_COMPETENCIA_MES_3','MONTO_TRX_COMPETENCIA_MES_4','MONTO_TRX_COMPETENCIA_MES_5','MONTO_TRX_COMPETENCIA_MES_6']].to_json(orient='records')
                                            json4_c_data = json.loads(json_4_c)[0]
                                            json_4={'Total':json4_a_data,'Cliente':json4_b_data,'Competencia':json4_c_data}
                                            # se crea el JSON con la información en percentiles por mes
                                            json_5_a = df_tarjeta_selec[['PERC_CANT_TRX_TOTAL_MES_1','PERC_CANT_TRX_TOTAL_MES_2','PERC_CANT_TRX_TOTAL_MES_3','PERC_CANT_TRX_TOTAL_MES_4','PERC_CANT_TRX_TOTAL_MES_5','PERC_CANT_TRX_TOTAL_MES_6','PERC_MONTO_TRX_TOTAL_MES_1','PERC_MONTO_TRX_TOTAL_MES_2','PERC_MONTO_TRX_TOTAL_MES_3','PERC_MONTO_TRX_TOTAL_MES_4','PERC_MONTO_TRX_TOTAL_MES_5','PERC_MONTO_TRX_TOTAL_MES_6']].to_json(orient='records')
                                            json5_a_data = json.loads(json_5_a)[0]
                                            # se crea el JSON con la información en percentiles por mes del cliente
                                            json_5_b = df_tarjeta_selec[['PERC_CANT_TRX_CLIENTE_MES_1','PERC_CANT_TRX_CLIENTE_MES_2','PERC_CANT_TRX_CLIENTE_MES_3','PERC_CANT_TRX_CLIENTE_MES_4','PERC_CANT_TRX_CLIENTE_MES_5','PERC_CANT_TRX_CLIENTE_MES_6','PERC_MONTO_TRX_CLIENTE_MES_1','PERC_MONTO_TRX_CLIENTE_MES_2','PERC_MONTO_TRX_CLIENTE_MES_3','PERC_MONTO_TRX_CLIENTE_MES_4','PERC_MONTO_TRX_CLIENTE_MES_5','PERC_MONTO_TRX_CLIENTE_MES_6']].to_json(orient='records')
                                            json5_b_data = json.loads(json_5_b)[0]
                                            # se crea el JSON con la información en percentiles por mes del competencia
                                            json_5_c = df_tarjeta_selec[['PERC_CANT_TRX_COMPETENCIA_MES_1','PERC_CANT_TRX_COMPETENCIA_MES_2','PERC_CANT_TRX_COMPETENCIA_MES_3','PERC_CANT_TRX_COMPETENCIA_MES_4','PERC_CANT_TRX_COMPETENCIA_MES_5','PERC_CANT_TRX_COMPETENCIA_MES_6','PERC_MONTO_TRX_COMPETENCIA_MES_1','PERC_MONTO_TRX_COMPETENCIA_MES_2','PERC_MONTO_TRX_COMPETENCIA_MES_3','PERC_MONTO_TRX_COMPETENCIA_MES_4','PERC_MONTO_TRX_COMPETENCIA_MES_5','PERC_MONTO_TRX_COMPETENCIA_MES_6']].to_json(orient='records')
                                            json5_c_data = json.loads(json_5_c)[0]
                                            json_5 = {'Total':json5_a_data,'Cliente':json5_b_data,'Competencia':json5_c_data}
                                            json_6 = {'Total':json_4,'Percentiles':json_5}
                                            # se agregan al JSON de la info adicional lo historico y lo mensual
                                            json2_data["Historicos"] = json3_data
                                            json2_data["Mensuales"] = json_6
                                            # se crea un solo json por tarjeta en el que anida el json2 en el json1
                                            json1_data["Valores"] = json2_data
                                            co = json.dumps(json1_data)
                                            json_tarjeta = json.loads(co)
                                            json_data.append(json_tarjeta)
                                            # se hace la parte de postgresql, con la info historica y general
                                            json_post = df_tarjeta_selec[['NU_TARJETA_SHA256','CLUSTER','MONTO_TRX_CLIENTE_HISTORICO','PCKT_MCC_CANT_TRX','PCKT_MCC_TCK','ACTIVO_3M','NO_TRX_TIME','PERC_CANT_TRX_TOTAL_HISTORICO','PERC_CANT_CLIENTE_HISTORICO','PERC_CANT_TRX_COMPETENCIA_HISTORICO','PERC_MONTO_TRX_TOTAL_HISTORICO','PERC_MONTO_CLIENTE_HISTORICO','PERC_MONTO_TRX_COMPETENCIA_HISTORICO']].to_json(orient='records')
                                            json_post_data = json.loads(json_post)[0]
                                            json_postgres.append(json_post_data)

                                        respuesta = json.dumps(json_data[0:tarjetas_nombre.shape[0]])
                                        #subir().subir_informacion(json_data=json_postgres)

                                    else:  respuesta = "5"
                                else: respuesta = "0"
                            else: respuesta = "1"
                        else: respuesta = "2"
                    else: respuesta = "3"
                else: respuesta = "4"
            else:respuesta = "7"
        else: respuesta = "6"
        self.value_1=respuesta
        if len(respuesta)>1:
            self.value_2 = json_postgres
        else:
            self.value_2 = []

        #return (respuesta)
    def __call__(self):
        return [self.value_1, self.value_2]







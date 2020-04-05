import pandas as pd
import datetime
import pytz
import psycopg2


class subir:
    def __init__(self):
        self.client = []
    def subir_informacion(self,json_data):
        # Se agrega la zona horaria de Colombia
        if len(json_data)>0:
            tz = pytz.timezone('America/Bogota')
            Fecha = datetime.datetime.now(tz=tz).strftime('%Y/%m/%d  %H:%M:%S')
            df_postgres = pd.DataFrame(json_data)
            df_postgres['FECHA'] = Fecha
            tuplas = [tuple(x) for x in df_postgres.values]
            nombres_col = ','.join([nombre.lower() for nombre in df_postgres.columns])
            try:

                conexion = psycopg2.connect(user="postgres",
                                              password="Fncalderon91G",
                                              host="35.232.151.83",
                                              port="5432",
                                              database="postgres")
                cursor = conexion.cursor()
                strings = ['%s' for i in range(len(nombres_col.split(',')))]
                sql_insert_query = """ INSERT INTO sifco_requests ("""+ nombres_col +""") VALUES ("""+','.join(strings)+""") """

                # executemany() to insert multiple rows rows
                result = cursor.executemany(sql_insert_query, tuplas)
                conexion.commit()
                cursor.close()
                conexion.close()

                print(cursor.rowcount, "Record inserted successfully into sifco_requests")

            except (Exception, psycopg2.Error) as error:
                print("Fallo la carga de los datos {}".format(error))


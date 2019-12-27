import os, json, base64, logging
from flask_api import FlaskAPI
from flask import request
from backends.Modelo_Calculo import BqClient
from backends.Modelo_Calculo_Gatos import Gatos
from backends.Modelo_SuperApp import SUPERAPP
from backends.Modelo_descriptivo import SUPERAPP_DESCRIPTIVOS

app = FlaskAPI(__name__)

@app.route('/')
def hello():
    print("asd")
    return """
<!DOCTYPE html>
<head>
   <title>Predicción Top 5 Categorías Supper App</title>
   <link rel="stylesheet" href="http://stash.compjour.org/assets/css/foundation.css">
</head>
<body style="width: 880px; margin: auto;">  
    <h2>Predicción categoría Super App</h2>
    <p>Esta API toma la información transaccional del usuario correspondiente a 1 año.
       predice y recomienda la categoría mas probable en una hora dada utilizando el modelo
       Xtrem Gradient Boosting (xgboost).</p>
</body>

<h4> Format </h4>
Este data set contiene las siguientes salidas:

<ul>
  <li> FECHA: La fecha y hora en la que se realiza la predicción.</li>
  <li> HORA: La hora en la que el usuario compra.</li>
  <li> NU_TARJETA_SHA256: Indica el número de tarjeta asociado al usuario encriptado con el algoritmo SHA256. </li>
  <li> CATEGORIA: Muestra el Top 5 de la categoría de la Supper App. </li>
  <li> PROBABILIDAD: Es la probabilidad con la que el usuario va a comprar en esa categoria para una hora dada.
</ul>
"""

@app.route("/pub-sub/", methods=['POST'])
def pob_sub():
    print(request.data)
    if request.data.get("message"):
        print(request.data.get("message"))
        return request.data, 200
    else:
        print("NO SE ENCUENTRA INFO")
        return "NADA DE NADA", 404

@app.route("/modelo/", methods=['POST'])
def modelo():
    print(request.data)
    # print(request.data.get("message"))
    response = BqClient().get_score(request.data)
    if response:
        return response, 200
    else:
        return u"No se encontró información", 404

@app.route("/modelo_gatos/", methods=['POST'])
def modelo_gatos():
    print(request.data)
    response = Gatos().prediccion(request.data)
    if response:
        return response, 200
    else:
        return u"No se encontró información", 404

@app.route("/SUPPER_APP/", methods=['POST'])
def SUPPER_APP():
    print(request.data)
    response = SUPERAPP().datos_totales(request.data)
    if response == "0":
        return u"Almenos una tarjeta no tiene 64 caracteres", 400
    elif response == "1":
        return u"Los tokens deben ser string 1", 400
    elif response == "2":
        return u"No se han ingresado tarjetas", 204
    elif response == "3":
        return u"El input no es una lista", 400
    elif response == "4":
        return u"No se encontró información", 404
    elif response:
        return response, 200
    else:
        return u"No se encontró información", 404


@app.route("/SUPPER_APP_DESCRIPTIVOS/", methods=['POST'])
def SUPPER_APP_DESCRIPTIVOS():
    print(request.data)
    response = SUPERAPP_DESCRIPTIVOS().cosa(request.data)
    if response == "0":
        return u"Almenos una tarjeta no tiene 64 caracteres", 400
    elif response == "6":
        return u"El input de informacion de tarjeta no es una lista", 400
    elif response == "7":
        return u"No se ha ingresado 'info_tarjetas'", 249
    elif response == "1":
        return u"Los tokens deben ser string ", 400
    elif response == "2":
        return u"No se han ingresado tarjetas", 301
    elif response == "3":
        return u"El input de tokens no es una lista", 400
    elif response == "4":
        return u"El input de info de tarjeta es desconocido", 400
    elif response == "5":
        return u"No se encontró información", 404
    elif response:
        return response, 200
    else:
        return u"No se encontró información", 404

if __name__ == '__main__':
    app.run(debug=True)
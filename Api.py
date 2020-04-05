import os, json, base64, logging
from flask_api import FlaskAPI
from flask import request
from flask import Response
from backends.SIFCO import SIFCO
from backends_subir_archivo.subir_info import subir

app = FlaskAPI(__name__)


@app.route('/')
def hello():
    print("asd")
    return """
<!DOCTYPE html>
<head>
   <title>Sistema de fidelización</title>
   <link rel="stylesheet" href="http://stash.compjour.org/assets/css/foundation.css">
</head>
<body style="width: 880px; margin: auto;">  
    <h2>Definición del tipo del cliente  en un comercio de interés/h2>
    <p>Esta API toma la información transaccional del usuario correspondiente a 1 año.
       De acuerdo a su comportamiento en el comercio aliado, en la competencia y 
       en MCC correspondiente, se define que tipo de cliente es para nuestro aliado</p>
</body>

<h4> Format </h4>
Este data set contiene las siguientes salidas:

<ul>
  <li> FECHA: La fecha y hora en la que se realiza la consulta.</li>
  <li> NU_TARJETA_SHA256: Indica el número de tarjeta asociado al usuario encriptado con el algoritmo SHA256. </li>
  <li> Información historica de los percentiles del comportamiento </li>
  <li> Información de la actividad de la tarjeta en el último año.
</ul>
"""


@app.route("/SIFCO_MODEL/", methods=['POST'])
def SIFCO_MODEL():
    # response = SIFCO().sifco(request.data)
    respuestas = SIFCO(request.data)
    response = respuestas()[0]
    subir().subir_informacion(json_data=respuestas()[1])
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
        return Response(response, mimetype='application/json'), 200
    else:
        return u"No se encontró información", 404


if __name__ == '__main__':
    app.run(debug=True)

import os, json, base64, logging
from flask_api import FlaskAPI
from flask import request
from backends.Modelo_Calculo import BqClient

# from backends.big_query import BqClient
app = FlaskAPI(__name__)
# os.environ.setdefault('FLASK_APP', 'main.py')
# os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', 'backends/credentials.json')


@app.route('/')
def hello():
    print("asd")
    return 'Aplicativo Flask pub-sub'



@app.route("/pub-sub/", methods=['POST'])
def pob_sub():
    print(request.data)
    if request.data.get("message"):
        print(request.data.get("message"))
    return request.data, 200


@app.route("/modelo/", methods=['GET'])
def modelo():
    print(request.args)
    # print(request.data.get("message"))
    response = BqClient().get_score(request.args)
    if response:
        return response, 200
    else:
        return u"No se encontró información", 404


if __name__ == '__main__':
    app.run(debug=True)

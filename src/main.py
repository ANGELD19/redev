import os
import datetime

from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import jwt_required, JWTManager

from src.application.auth_service import Auth



app = Flask(__name__)
app.config["CHARSET"] = "UTF-8"
configurations = os.environ

app.config["JWT_SECRET_KEY"] = configurations.get("JWT_SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = datetime.timedelta(hours=6)

jwt = JWTManager(app)

auth = Auth(app)

CORS(app, resources={r"/*": {"origins": "*", "send_wildcard": "True"}})

@app.route("/")     #Se esta decorando indicando que dentro de la aplicacion va a estar ligada a la ruta raiz de la app        
def index():        #linea 26/27 crea una vista que se expresa en forma de funcion 
    return "Testing, Flask!"

if __name__ == '__main__':     #__main__ es el nombre del archivo principal que se ejecuta, Este EJECUTA el servidor SOLO si este archivo es el principal para eso es el IF 
    app.run(debug=True,port=5000)   #Esto ubica un puerto para que se abra directamente 


@app.route("/auth/login", methods=["POST"])
def login():
    return auth.login()

from flask import Flask
from flask_restful import Resource, Api
from db import db

import psycopg2

app = Flask(__name__)

api = Api(app)

conn = psycopg2.connect("host=postgres dbname=flats user=admin password=admin")
cur = conn.cursor()

class Offers(Resource):
    def get(self):
        return ["offer1", "offer2"]

    def post(self):
        return "post offer"

api.add_resource(Offers, '/offers/')

if __name__ == '__main__':
    app.config['DEBUG'] = True
    app.run(host='0.0.0.0', port="5000")

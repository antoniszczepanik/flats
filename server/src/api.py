from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)

api = Api(app)

class HelloWorld(Resource):
    def get(self):

        return {'hello': 'asdfasdfworld'}

class Other(Resource):
    def get(self):
        return {'hello': 'Other'}

api.add_resource(HelloWorld, '/')
api.add_resource(Other, '/other/')

if __name__ == '__main__':
    app.config['DEBUG'] = True
    app.run(host='0.0.0.0', port="5000")

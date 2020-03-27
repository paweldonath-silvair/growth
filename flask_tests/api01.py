import requests
from flask import Flask
from flask_restful import Resource, Api

from test_utils import MAIN_URL, print_response


def run():
    app = Flask(__name__)
    api = Api(app)

    class HelloWorld(Resource):
        def get(self):
            return {'hello': 'world'}

    api.add_resource(HelloWorld, '/')

    app.run(debug=True)


def test():
    print_response(requests.get(f'{MAIN_URL}/'))

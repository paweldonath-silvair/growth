import requests
from flask import Flask, request
from flask_restful import Resource, Api

from test_utils import MAIN_URL, print_response


def run():
    app = Flask(__name__)
    api = Api(app)

    class Todo1(Resource):
        def get(self):
            # Default to 200 OK
            return {'task': 'Hello world'}

    class Todo2(Resource):
        def get(self):
            # Set the response code to 201
            return {'task': 'Hello world'}, 201

    class Todo3(Resource):
        def get(self):
            # Set the response code to 201 and return custom headers
            return {'task': 'Hello world'}, 201, {'Etag': 'some-opaque-string'}

    api.add_resource(Todo1, '/todo1')
    api.add_resource(Todo2, '/todo2')
    api.add_resource(Todo3, '/todo3')

    app.run(debug=True)


def test():
    print_response(requests.get(f'{MAIN_URL}/todo1'))
    print_response(requests.get(f'{MAIN_URL}/todo2'))
    print_response(requests.get(f'{MAIN_URL}/todo3'))

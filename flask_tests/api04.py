import requests
from flask import Flask, request
from flask_restful import Resource, Api, reqparse

from test_utils import MAIN_URL, print_response


def run():
    app = Flask(__name__)
    api = Api(app)

    class Todo1(Resource):
        # search anywhere
        parser = reqparse.RequestParser()
        parser.add_argument('age', type=int, default=0)
        parser.add_argument('name', default="xxx")

        def get(self):
            args = self.parser.parse_args()
            return {
                'name': args.get('name'),
                'age': args.get('age'),
            }

    class Todo2(Resource):
        parser = reqparse.RequestParser()
        parser.add_argument('name', location='args')  # query
        parser.add_argument('age', type=int, location='form')  # body
        parser.add_argument('position', location='headers')  # headers

        def get(self):
            args = self.parser.parse_args()
            return {
                'name': args.get('name'),
                'age': args.get('age'),
                'position': args.get('position'),
            }

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
    print_response(requests.get(f'{MAIN_URL}/todo1', data={'name': 'haha'}))
    print_response(requests.get(f'{MAIN_URL}/todo1', data={'age': '9'}))
    print_response(requests.get(f'{MAIN_URL}/todo1', data={'age': 7}))
    print_response(requests.get(f'{MAIN_URL}/todo1?name=buu'))

    print_response(requests.get(
        f'{MAIN_URL}/todo2?name=abc',
        data={"age": 13},
        headers={"position": "boss"}
    ))

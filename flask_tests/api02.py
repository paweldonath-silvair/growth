import requests
from flask import Flask, request
from flask_restful import Resource, Api

from test_utils import MAIN_URL, print_response


def run():
    app = Flask(__name__)
    api = Api(app)

    todos = {}

    class TodoSimple(Resource):
        def get(self, todo_id):
            return {todo_id: todos[todo_id]}

        def put(self, todo_id):
            todos[todo_id] = request.form['data']
            return {todo_id: todos[todo_id]}

    api.add_resource(TodoSimple, '/<string:todo_id>')

    app.run(debug=True)


def test():
    print_response(requests.put(f'{MAIN_URL}/todo1', data={'data': 'Remember the milk'}))
    print_response(requests.get(f'{MAIN_URL}/todo1'))
    print_response(requests.put(f'{MAIN_URL}/todo2', data={'data': 'Change my brakepads'}))
    print_response(requests.get(f'{MAIN_URL}/todo2'))

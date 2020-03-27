import json
import requests
import yaml

MAIN_URL = "http://localhost:5000"


def print_response(response: requests.Response):
    try:
        data = response.json()
    except Exception:
        data = response.content.decode("utf-8")
    print(yaml.dump(
        {
            "code": response.status_code,
            "data": data,
            "headers": dict(response.headers)
        }
    ))

import json


def hello(event, context):
    body = {
        "message": "Hello! Successfully access to Lambda",
        "input": event
    }

    response = {
        "statusCode": 200, # status 200은 성공이고, 그 내용이 body
        "body": json.dumps(body)
    }

    return response

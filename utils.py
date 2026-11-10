import json


def serializer(data):
    return json.dumps(data).encode("utf-8")


def deserializer(data):
    return json.loads(data.decode("utf-8"))

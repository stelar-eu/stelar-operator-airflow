import json

from apiflask import Schema
from apiflask.fields import Boolean, Integer, String, Dict, Nested, URL, List
from apiflask.validators import Length, OneOf, NoneOf


class Input(Schema):
    input = List(String, required=True)
    parameters = Dict(required=True)
    minio = Dict(required=True)

class Output(Schema):
    output = List(String, required=True)
    metrics = Dict(required=True)


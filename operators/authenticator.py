from airflow.models.baseoperator import BaseOperator
from jinja2 import Template
import requests
from airflow.models import Variable

class Authenticator(BaseOperator):
    def __init__(self, username, password, **kwargs):
        super().__init__(**kwargs)
        self.username = username
        self.password = password

    def execute(self, context):
        username = Template(self.username).render(**context)
        password = Template(self.password).render(**context)
        
        AUTH_URL = Variable.get("AUTH_URL")
        response = requests.post(AUTH_URL, json= {'username': username,
                                                  'password': password})
        j = response.json()
        if response.status_code == 200:
            for field in ['CKAN_Token', 'minio_id', 'minio_key']:
                context['ti'].xcom_push(key=field, value=j[field])
        else:
            raise ValueError(j['message'])
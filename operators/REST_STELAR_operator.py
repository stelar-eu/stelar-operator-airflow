from airflow.models.baseoperator import BaseOperator
from jinja2 import Template
import requests
from airflow.models import Variable
import json
import ast


class RESTStelarOperator(BaseOperator):
    def __init__(self, name, call_params, input_ids=[], package_id=None,
                 package_metadata=None, **kwargs):
        super().__init__(**kwargs)
        self.input_ids = input_ids
        
        tools = json.loads(Variable.get("tools"))
        if name not in tools:
            raise ValueError("Tool is not listed, available options are: ", tools.keys())
        
        self.name = tools[name]
        self.call_params = call_params
        self.package_id = package_id
        self.package_metadata = package_metadata

    def execute(self, context):
        try:
            for field in ['CKAN_Token', 'minio_id', 'minio_key']:
                context['ti'].xcom_pull(key=field)
        except Exception as e:
            raise ValueError("User not authorized")
            
        settings = {'klms_api': {
                                  'endpoint_url': Variable.get("DATA_API_URL"),
                                  'token': context['ti'].xcom_pull(key='CKAN_Token')
                                 },
                    'minio' : {'endpoint_url': Variable.get("MINIO_URL"),
                               'id': context['ti'].xcom_pull(key='minio_id'),
                               'key': context['ti'].xcom_pull(key='minio_key'),
                               'bucket': 'agroknow-bucket'
                        },
                    }
        
        # Init variables
        dag_id = context['task_instance'].dag_id
        task_id =  context['task_instance'].task_id
        run_id = context['run_id']
        date = context['execution_date'].strftime("%Y%m%d")
        
        experiment = '{}_{}'.format(dag_id, task_id)
        # title = 'Workflow for {} {}'.format(dag_id, date)
        title = 'Workflow for {} {}'.format(dag_id, run_id)
        
        # Render jinja variables
        run_id = Template(run_id).render(**context)
        # for key in ['title']:
            # if self.track_params[key] is not None:
        title = Template(title).render(**context)
        
        if type(self.call_params) == str:   # arguments from triggered config
            self.call_params = Template(self.call_params).render(**context)
            self.call_params = ast.literal_eval(self.call_params)
        
        for key, val in self.call_params.items():
            if type(val) == str:
                self.call_params[key] = Template(val).render(**context) 
                
        headers={'Api-Token': settings['klms_api']['token']}
        
        
        # Fetch files from Catalogue, if needed
        input_paths = []
        ckan_get_success = True
        url = settings['klms_api']['endpoint_url'] + 'api/v1/artifact'
        for no, res_id in enumerate(self.input_ids):
            print(res_id)
            res_id = Template(res_id).render(**context)
            print(res_id)
            if res_id.startswith('xcom'):
                xcom_ids = context['ti'].xcom_pull(key='resource_id').split(',')
                int_id = int(res_id.split('_')[1])
                res_id = xcom_ids[int_id]
            self.input_ids[no]= res_id
            
            response = requests.get(url, params= {'id': res_id}, headers=headers)
            if response.status_code == 200:
                j = response.json()
                if j['success']:
                    input_paths.append(j['result']['path'])
                else:
                    ckan_get_success = False
            else: 
                ckan_get_success = False
        # if 'xcom' in self.input_ids:
        #     self.input_ids.remove('xcom')
            
        # Call library     
        metrics = {}
        if ckan_get_success:
            tool_params = {'input': input_paths, 'parameters': self.call_params, 
                            'minio': settings['minio']}
            print(tool_params)
            response = requests.post(self.name, json=tool_params)
            j = response.json()
            print(j)
            output_paths = []
            tool_success = True
            if response.status_code == 200:
                # output_paths = j['output']
                # metrics = j['metrics']
                
                output_paths = j['output']
                metrics = j['metrics']
            else:
                # raise ValueError('Error in executing tool.')            
                tool_success = False
            
        # Publish to CKAN
        output_resource_ids = []
        if ckan_get_success and tool_success:
            output_package_ids = []
            url = settings['klms_api']['endpoint_url'] + 'api/v1/artifact/publish'
            ckan_publish_success = True
            for file in output_paths:
                ftype = file.split('/')[-1].split(".")[-1].upper()
                d = { "artifact_metadata":{
                            "url":file,
                            "name": f"Results of {experiment} task",
                            "description": f"This is the artifact uploaded to minio S3 in {ftype} format",
                            "format": ftype,
                            "resource_tags":["Artifact","MLFlow"]
                                        }
                    }
                if self.package_id is None:
                    if self.package_metadata is None:   # User provided no metadata to Operator
                        d["package_metadata"] = {
                            "title": title,
                            "tags":[{"name":"Artifact"},{"name":"Workflow"}],
                            "notes":"This is the test artifact uploaded to minio S3.",}
                    elif type(self.package_metadata) == str: # User provided metadata to Operator via Config
                        self.package_metadata = Template(self.package_metadata).render(**context)
                        if self.package_metadata == "": # User provided no metadata to config
                            d["package_metadata"] = {
                                "title": title,
                                "tags":[{"name":"Artifact"},{"name":"Workflow"}],
                                "notes":"This is the test artifact uploaded to minio S3.",}
                        else: # User provided metadata to Config
                            self.package_metadata = ast.literal_eval(self.package_metadata)
                            d["package_metadata"] = self.package_metadata   
                    elif type(self.package_metadata) == dict: # User provided metadata directly to Operator
                        d["package_metadata"] = self.package_metadata
                        
                else:
                    if self.package_id == 'xcom':
                        self.package_id = context['ti'].xcom_pull(key='package_id').split(',')[0]
                    d["package_metadata"] = {
                        "package_id": self.package_id
                    }
        
                response = requests.post(url, json=d, headers=headers)
                if response.status_code == 200:
                    j = response.json()
                    if j['success']:
                        output_resource_ids.append(j['result']['resource_id'])
                        output_package_ids.append(j['result']['package_id'])
                        self.package_id = j['result']['package_id']
                    else:
                        ckan_publish_success = False
                else:
                    ckan_publish_success = False
            context['ti'].xcom_push(key='resource_id', value=','.join(output_resource_ids))
            context['ti'].xcom_push(key='package_id', value=','.join(output_package_ids))
                

        # Call Tracking
        # if ckan_get_success and tool_success and ckan_publish_success:
        url = settings['klms_api']['endpoint_url'] + 'api/v1/track'
        
        track_params = {'input': self.input_ids, 'parameters': self.call_params, 
                        'output': output_resource_ids, 'metrics': metrics,
                        'settings': {'experiment': experiment,
                                      'tags': {'dag_id': dag_id, 
                                              'run_id': context['run_id'], 
                                              'task_id': task_id,
                                              # 'user': self.owner
                                              }}
                            }
        print(track_params)
        response = requests.post(url, json=track_params, headers=headers)
        if response.status_code != 200:
            raise ValueError('Error in tracking.')
                        
        if not ckan_get_success:
            raise ValueError('Error in getting files from CKAN.')
        if not tool_success:
            raise ValueError('Error in executing tool.')
        if not ckan_publish_success:
            raise ValueError('Error in publishing files to CKAN.')

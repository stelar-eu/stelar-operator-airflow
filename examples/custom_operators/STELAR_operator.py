from airflow.models.baseoperator import BaseOperator
from jinja2 import Template
import requests


class StelarOperator(BaseOperator):
    def __init__(self, url, call_params, settings,
                 input_ids=[], package_id=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.input_ids = input_ids
        self.url = url
        self.call_params = call_params
        self.settings = settings
        self.package_id = package_id

    def execute(self, context):
        
        # Init variables
        dag_id = context['task_instance'].dag_id
        task_id =  context['task_instance'].task_id
        run_id = context['run_id']
        date = context['execution_date'].strftime("%Y%m%d")
        
        experiment = '{}_{}'.format(dag_id, task_id)
        title = 'Workflow for {} {}'.format(dag_id, date)
        
        # Render jinja variables
        run_id = Template(run_id).render(**context)
        # for key in ['title']:
            # if self.track_params[key] is not None:
        title = Template(title).render(**context)
        for key, val in self.call_params.items():
            if type(val) == str:
                self.call_params[key] = Template(val).render(**context)        
        headers={'Api-Token': self.settings['klms_api']['token']}
        
        
        # Fetch files from Catalogue, if needed
        input_paths = []
        ckan_get_success = True
        url = self.settings['klms_api']['endpoint_url'] + 'api/v1/artifact'
        for res_id in self.input_ids:
            if res_id == 'xcom':
                self.input_ids += context['ti'].xcom_pull(key='resource_id').split(',')
                continue
            
            response = requests.get(url, params= {'id': res_id})
            if response.status_code == 200:
                j = response.json()
                if j['success']:
                    input_paths.append(j['result']['path'])
                else:
                    ckan_get_success = False
            else: 
                ckan_get_success = False
        if 'xcom' in self.input_ids:
            self.input_ids.remove('xcom')
            
        # Call library     
        metrics = {}
        if ckan_get_success:
            tool_params = {'input': input_paths, 'parameters': self.call_params, 
                            'minio': self.settings['minio']}
            print(tool_params)
            response = requests.post(self.url, json=tool_params)
            j = response.json()
            output_paths = []
            tool_success = True
            if response.status_code == 200:
                output_paths = j['output']
                metrics = j['metrics']
            else:
                # raise ValueError('Error in executing tool.')            
                tool_success = False
            
        # Publish to CKAN
        output_resource_ids = []
        if ckan_get_success and tool_success:
            output_package_ids = []
            url = self.settings['klms_api']['endpoint_url'] + 'api/v1/artifact/publish'
            ckan_publish_success = True
            for file in output_paths:
                ftype = file.split('/')[-1].split(".")[-1].upper()
                d = { "artifact_metadata":{
                            "url":file,
                            "name": f"Results of {experiment} task",
                            "description": f"This is the test artifact uploaded to minio S3 in {ftype} format",
                            "format": ftype,
                            "resource_tags":["Artifact","MLFlow"]
                                        }
                    }
                if self.package_id is None:
                    d["package_metadata"] = {
                            "title": title,
                            "tags":[{"name":"Artifact"},{"name":"Workflow"}],
                            "notes":"This is the test artifact uploaded to minio S3.",}
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
                    else:
                        ckan_publish_success = False
                else:
                    ckan_publish_success = False
            context['ti'].xcom_push(key='resource_id', value=','.join(output_resource_ids))
            context['ti'].xcom_push(key='package_id', value=','.join(output_package_ids))
                
        # Call Tracking
        # if ckan_get_success and tool_success and ckan_publish_success:
        url = self.settings['klms_api']['endpoint_url'] + 'api/v1/track'
        
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
from custom_operators.STELAR_operator import StelarOperator
from airflow import DAG
from datetime import datetime

settings = {'klms_api': {
                            'endpoint_url': 'DATA-API-ENDPOINT', #Change this
                          'token': 'CATALOG-TOKEN' #Change this
                         },
            'minio' : {'endpoint_url': 'DATA-STORAGE-ENDPOINT', #Change this
                       'id': "MINIO-ID", #Change this
                       'key': "MINIO-KEY", #Change this
                       'bucket': 'BUCKET' #Change this
                },
            }

with DAG(dag_id='Food_Incident_Articles', start_date=datetime(2023, 7, 6),
         end_date=datetime(2023, 7, 13),
         max_active_runs=1, 
         schedule="@daily",
         owner_links={"user": "mailto:user@org.gr"},
         description='Test workflow',
         ) as dag:
    
    pdir = 'LOCAL-DIR' # Change this

    date = '{{ execution_date.strftime("%Y%m%d") }}'

    download_task = StelarOperator(task_id="download", owner='user',
                                    url='DOWNLOAD_ENDPOINT', #Change this
                                    call_params = { 'prefix': date,
                                              'out': pdir,
                                              'output_file': f'{pdir}data/download_gdelt_{date}.csv',
                                    },
                                    settings=settings
                                    )
                                    
    deduplicate_task = StelarOperator(task_id="deduplicate", owner='user',
                                    input_ids = ['xcom'], package_id='xcom',
                                    url='DEDUPLICATE_ENDPOINT', #Change this
                                    call_params= {
                                        'col_id': 0,
                                        'col_text': 7,
                                        'delta': 0.7,
                                        'jointFilter': True,
                                        'posFilter': True,
                                        'verification_alg': 0,
                                        'output_file': f'{pdir}data/deduplicate_gdelt_{date}.csv',
                                    },
                                    settings=settings
                                  )

    download_task >> deduplicate_task

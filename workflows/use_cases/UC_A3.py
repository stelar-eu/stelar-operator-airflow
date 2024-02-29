from STELAR_Operator.operators.REST_STELAR_operator import RESTStelarOperator
from STELAR_Operator.operators.authenticator import Authenticator
from STELAR_Operator.operators.WebSocket_STELAR_operator import WebSocketStelarOperator
from airflow import DAG
from datetime import datetime

with DAG(dag_id='UC_A3', start_date=datetime(2024, 2, 10),
         max_active_runs=1, 
         schedule=None,
         owner_links={"user": "mailto:user@org.gr"},
         description='Entity Extraction on food recall incidents, accompanied by Entity Linking to a known entity dictionary.',
         tags=["use-cases", "authenticator", "entity_extraction", "entity_linking", 'AgroKnow']
         ) as dag:
    
    auth = Authenticator(task_id="authentication", owner='user',
                         username =  "{{ params.username }}",
                         password= "{{ params.password }}",
                         )

    extract_task = WebSocketStelarOperator(task_id="entity_extraction", owner='user',
                                        input_ids = ["{{ params.input[0] }}"],
                                        name='Entity_Extraction',
                                        call_params = "{{ params.tools['entity_extraction'] }}",
                                        package_metadata = "{{ params.package_metadata }}"
                                    )
                              
    linking_task = WebSocketStelarOperator(task_id="entity_linking", owner='user',
                                    input_ids = ['xcom_0', "{{ params.input[1] }}"],
                                    package_id='xcom',
                                    name='Entity_Linking',
                                    call_params = "{{ params.tools['entity_linking'] }}",
                                    package_metadata = "{{ params.package_metadata }}"
                                  )

    auth >> extract_task >> linking_task

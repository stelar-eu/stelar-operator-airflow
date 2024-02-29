from STELAR_Operator.operators.authenticator import Authenticator
from STELAR_Operator.operators.REST_STELAR_operator import RESTStelarOperator
from STELAR_Operator.operators.WebSocket_STELAR_operator import WebSocketStelarOperator
from airflow import DAG
import datetime

with DAG(dag_id='Demo_for_Entity_Linking',
         max_active_runs=1, 
         schedule=None,
         owner_links={"user": "mailto:user@org.gr"},
         description='Demo workflow for the operator of Entity Linking',
         start_date=datetime.datetime(2021, 1, 1),
         tags=["demos", "authenticator", "entity_linking"],
         ) as dag:
    

    auth = Authenticator(task_id="authentication", owner='user',
                         username =  "{{ params.username }}",
                         password= "{{ params.password }}",
                         )
                              
    linking_task = WebSocketStelarOperator(task_id="entity_linking", owner='user',
                                           input_ids = ["{{ params.input[0] }}", "{{ params.input[1] }}"],
                                    name='Entity_Linking',
                                    call_params = "{{ params.tools['entity_linking'] }}"
                                  )

    auth >> linking_task

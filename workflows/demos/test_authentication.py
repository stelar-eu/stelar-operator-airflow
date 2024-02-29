from STELAR_Operator.operators.authenticator import Authenticator
from airflow import DAG
import datetime

with DAG(dag_id='Demo_for_Authenticator',
         max_active_runs=1, 
         schedule=None,
         owner_links={"user": "mailto:user@org.gr"},
         description='Demo workflow for the operator of Authenticator',
         start_date=datetime.datetime(2021, 1, 1),
         tags=["demos", "authenticator"],
         ) as dag:
    

    auth = Authenticator(task_id="authentication", owner='user',
                         username =  "{{ params.username }}",
                         password= "{{ params.password }}",
                         )
                              

    auth

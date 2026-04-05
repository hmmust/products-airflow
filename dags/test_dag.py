from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def print_second_hello():
    print("This is the Second Print!")
with DAG(dag_id="first_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"]) as dag:
    task1 = BashOperator(task_id="print_hello",
                         bash_command="echo 'Hello from Linux!' ")
    task2 = PythonOperator(task_id="second_print",
                           python_callable=print_second_hello)
    #task1.set_downstream(task2)
    task1 >> task2
    #task2 << task1


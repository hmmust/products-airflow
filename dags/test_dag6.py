from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def print_first_hello():
    print("This is 1")
def print_second_hello():
    print("This is 2")
def print_third_hello():
    print("This is 3")
with DAG(dag_id="sixth_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"]) as dag:
    task1 = PythonOperator(task_id="first_print",
                           python_callable=print_first_hello)
    with TaskGroup("transform") as trasform:
        task2 = PythonOperator(task_id="second_print",
                            python_callable=print_second_hello)
        task3 = PythonOperator(task_id="third_print",
                            python_callable=print_third_hello)
        
    
    task1 >> trasform



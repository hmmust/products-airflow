from datetime import datetime, timedelta
from airflow.decorators import dag,task,task_group

@task_group
def extract():
    @task
    def print_second_hello():
        print("This is the Second Print!")
    @task
    def print_third_hello():
        print("This is the Third Print!")
    task1 = print_second_hello()
    task2 = print_third_hello()

@dag(dag_id="fifth_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"])
def my_dag():
    @task
    def print_first_hello():
        print("This is the First Print!")
    
    task3=print_first_hello() 
    task3 >> extract()
my_dag()



from datetime import datetime, timedelta
from airflow.decorators import dag,task
@dag(dag_id="second_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"])
def my_dag():
    @task
    def print_first_hello():
        print("This is the First Print!")
    @task
    def print_second_hello():
        print("This is the Second Print!")
    print_first_hello() >> print_second_hello()
my_dag()



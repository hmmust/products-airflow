from datetime import datetime, timedelta
from airflow.decorators import dag,task
@dag(dag_id="fourth_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"])
def my_dag():
    @task
    def print_first_hello(messsage):
        print("This is the First Print!",messsage)
        return {"success":True}
    @task
    def print_second_hello(inputs):
        print("This is the Second Print!",inputs.get("success"))
    task1=print_first_hello("Hossam") 
    task1 >> print_second_hello(task1)
my_dag()



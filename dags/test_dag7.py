from datetime import datetime, timedelta
from airflow.decorators import dag,task
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd

@dag(dag_id="customer_extract",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"])
def my_dag():
    @task
    def extract_customer_github():
        customerHook = HttpHook (http_conn_id="github_customers",
                                 method="GET")
        response= customerHook.run("/customers_filtered.json")
        response.raise_for_status()
        customers = response.json()
        customers_df = pd.DataFrame(customers)
        print(customers_df)
        return {"success":True}
    @task
    def print_second_hello(inputs):
        print("This is the Second Print!",inputs.get("success"))
    task1=extract_customer_github() 
    task1 >> print_second_hello(task1)
my_dag()



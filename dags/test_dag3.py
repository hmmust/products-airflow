from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def print_first_hello(**inputs):
    print("This is ",inputs.get("message"))
    return {"success":True}
def print_second_hello(task_instance,**kargs):
    result= task_instance.xcom_pull(task_ids= "first_print")
    print("This is ",result.get("success"))
    
with DAG(dag_id="third_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"]) as dag:
    task1 = PythonOperator(task_id="first_print",
                           python_callable=print_first_hello,
                           op_kwargs={"message":"Hossam"})
    task2 = PythonOperator(task_id="second_print",
                           python_callable=print_second_hello)
    #task1.set_downstream(task2)
    task1 >> task2
    #task2 << task1


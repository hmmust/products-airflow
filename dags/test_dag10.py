from datetime import datetime, timedelta
from airflow.decorators import dag,task
@dag(dag_id="map_dag",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Product pipeline","UOP"])
def my_dag():
    Files = ["File1.csv","File2.csv","File3.csv"]
    @task
    def process_file(filename):
        print(filename)
        return filename
    
    results = process_file.expand(filename= Files)

my_dag()



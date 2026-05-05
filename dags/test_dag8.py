from datetime import datetime, timedelta
from airflow.decorators import dag,task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from io import StringIO

@dag(dag_id="artist_etl",start_date=datetime(2026,4,5),
         catchup=False, schedule=timedelta(days=1),
         default_args={"owner":"Hossam Musta"},
         tags=["Artists pipeline","UOP"])
def my_dag():
    FILES= ["/artists/artists-01.csv.json","/artists/artists-01.csv.json"]
    @task
    def extract_schema():
        customerHook = HttpHook (http_conn_id="github_artists",
                                 method="GET")
        response= customerHook.run("/schemas.json")
        response.raise_for_status()
        schema = response.json()
        artists_schema = schema['artists']
        artists_schema = sorted(artists_schema,
                        key= lambda a:a['column_position'])
        cols = [col['column_name'] for col in artists_schema]
        
        return cols
    @task
    def extract_artists(cols):
        artistHook = HttpHook (http_conn_id="github_artists",
                                 method="GET")
        for file in FILES:
            response= artistHook.run(file)
            response.raise_for_status()
            

            file1 = pd.read_csv(StringIO(response.text),
                            header=None,names=cols)
            fitdbHook = PostgresHook(postgres_conn_id="fitdb")
            conn_uri = fitdbHook.get_sqlalchemy_engine()
            file1.to_sql("artists",conn_uri,index=False,
                    if_exists="append")

    task1 = extract_schema()
    task1> extract_artists(task1)
my_dag()



from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import json
import pandas as pd
from datetime import date

@dag(default_args={'owner': 'airflow'}, schedule_interval=None, start_date=days_ago(2))
def my_test_dag():
   @task()
   def fetch_data_to_local():
        url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
        response = requests.get(url)

    # convert the response to a pandas dataframe, then save as csv to the data
    # folder in our project directory
        df = pd.DataFrame(json.loads(response.content))
        df = df.set_index("date_of_interest")

    # for integrity reasons, let's attach the current date to the filename
        df.to_csv("/Users/vchauhan/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))
        #return df
   fetch_data_to_local()
   
tutorial_etl_dag = my_test_dag()
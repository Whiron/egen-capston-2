
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
import json
import pandas as pd
from google.cloud import bigquery
import logging
from io import StringIO

def fetch_data():
    print("-----fetch data--------")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('stock_market_storage')
    blob = bucket.blob('stock_data.csv')
    print('------Accessing storage-----')
    data = pd.read_csv(StringIO(blob.download_as_string().decode('utf-8')),sep=",",index_col=0)
    data=data[:101]
    data = data.to_json(orient="columns")
    return data


def analyze_data(**kwargs):
    print("-----analysing data-------")
    print('arguments',str(kwargs))
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='extract_data')
    df=pd.read_json(data)
    df["diff_pct"] = ((df["c"] - df["o"]) * 100) / df["o"]
    df = df.sort_values(by=["diff_pct"], ascending=False)
    new_df = pd.DataFrame()
    new_df['Ticker'] = df['T']
    new_df['Percentage_Change'] = df['diff_pct']
    new_df['Day_High'] = df['h']
    new_df['Day_Low'] = df['l']
    new_df['Day_Open'] = df['o']
    new_df['Day_Close'] = df['c']
    new_df['Volume_Traded'] = df['v']
    new_df=new_df.to_json(orient="columns")
    print("-----Transformed data------")
    print(json.dumps(new_df))
    return new_df


def load_data(**kwargs):
    datasetID = "stock_market_data"
    task_instance = kwargs["task_instance"]
    data = task_instance.xcom_pull(task_ids="transform_data")
    print("Received dataframes")
    print(str(data))
    data=pd.read_json(data)
    bqclient = bigquery.client.Client(project="egen-project-1-327215")
    dataset = bqclient.dataset(datasetID)
    dataset.location = "US"
    try:
        bqclient.create_dataset(datasetID)
        bqclient.create_table("stock_market_analysis")
    except:
        pass
    bqclient.load_table_from_dataframe(data, "egen-project-1-327215.stock_market_data.stock_market_analysis")

default_args={
    "depends on past" : False,
    "email" : [],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "owner" : "airflow",
    "retries" : 3,
    "retry_delay" : timedelta(minutes=1)
}
dag = DAG(dag_id='composer_trigger_dag',start_date=days_ago(1),default_args=default_args,schedule_interval=None)

start=DummyOperator(task_id="start",dag=dag)
end=DummyOperator(task_id="end",dag=dag)

extract_step = PythonOperator(
    task_id='extract_data',
    python_callable=fetch_data,
    dag=dag
)

transform_step = PythonOperator(
task_id='transform_data',
python_callable=analyze_data,
provide_context=True,
dag=dag
)



load_step = PythonOperator(
task_id='load_data',
python_callable=load_data,
provide_context=True,
dag=dag
)

start >> extract_step >> transform_step >> load_step >> end
from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


from airflow.providers.google.cloud.transfers.local import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

import requests
import json
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2025, 4, 24),
    schedule=None,
    catchup=False,
    tags=["papers_pricing"],
)


def papers_pricing():
    bucket_name= "data-engineer-test-suzano"
    chromedriver_path = "C:\\Users\\josef\\projects\\data-engineer-test-suzano\\plugins\\chromedriver.exe"
    file_path = "C:\\Users\\josef\\projects\\data-engineer-test-suzano\\include\\datasets\\usd_cny.parquet"

    def init_driver(chromedriver_path: str) -> webdriver.Chrome:   
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=pt-BR')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(executable_path=chromedriver_path)
        return webdriver.Chrome(service=service, options=options)
    
    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    def fetch_usd_cny_data(driver: webdriver.Chrome) -> pd.DataFrame:
        init_driver(chromedriver_path )

        url = "https://api.investing.com/api/financialdata/2111/historical/chart/?interval=P1M&period=MAX&pointscount=160"
        driver.get(url)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        tag = soup.find("pre")
        if not tag:
            raise ValueError()

        json_data = json.loads(tag.text)
        data = json_data.get("data", [])

        df = pd.DataFrame(data, columns=["timestamp", "close", "open", "high", "low", "volume", "unknown"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df[df["date"] >= "1991-01-01"]
        df.drop(columns=["timestamp", "unknown"], inplace=True)

        return df[["date", "close", "open", "high", "low", "volume"]]
    
    def export_to_parquet(df: pd.DataFrame, output_path: str) -> None:
        df.to_parquet(output_path, index=False)

    def upload_parquet_to_gcs(bucket_name: str, file_path: str, gcs_dest_path: str) -> LocalFilesystemToGCSOperator:
        return LocalFilesystemToGCSOperator(
            task_id='upload_parquet_to_gcs',
            src="include\datasets\usd_cny.parquet",
            dst="raw/usd_cny.parquet",
            bucket=bucket_name,
            mime_type='application/octet-stream',
        )
    
        create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',
    )


driver = init_driver(chromedriver_path)
df = df_usd_cny = fetch_usd_cny_data(driver)
export_to_parquet(df, "C:\\Users\\josef\\projects\\data-engineer-test-suzano\\include\\datasets\\usd_cny.parquet")








































def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Asset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json")
            r.raise_for_status()
            number_of_people_in_space = r.json()["number"]
            list_of_people_in_space = r.json()["people"]
        except Exception:
            print("API currently not available, using hardcoded data instead.")
            number_of_people_in_space = 12
            list_of_people_in_space = [
                {"craft": "ISS", "name": "Oleg Kononenko"},
                {"craft": "ISS", "name": "Nikolai Chub"},
                {"craft": "ISS", "name": "Tracy Caldwell Dyson"},
                {"craft": "ISS", "name": "Matthew Dominick"},
                {"craft": "ISS", "name": "Michael Barratt"},
                {"craft": "ISS", "name": "Jeanette Epps"},
                {"craft": "ISS", "name": "Alexander Grebenkin"},
                {"craft": "ISS", "name": "Butch Wilmore"},
                {"craft": "ISS", "name": "Sunita Williams"},
                {"craft": "Tiangong", "name": "Li Guangsu"},
                {"craft": "Tiangong", "name": "Li Cong"},
                {"craft": "Tiangong", "name": "Ye Guangfu"},
            ]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
    )


# Instantiate the DAG
example_astronauts()

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models.baseoperator import chain

from datetime import datetime
import pandas as pd
import json

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType


@dag(
    start_date=datetime(2025, 4, 25),
    schedule=None,
    catchup=False,
    tags=["papers_pricing"],
)
def papers():
    bucket_name = "data-engineer-test-suzano"
    dataset_name = "papers"
    

    @task
    def fetch_and_upload():
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from bs4 import BeautifulSoup
        import time

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless=new")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=pt-BR")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("useAutomationExtension", False)


        driver = webdriver.Chrome(options=options)

        time.sleep(5)
        driver.get("https://api.investing.com/api/financialdata/2111/historical/chart/?interval=P1M&period=MAX&pointscount=160")
        soup = BeautifulSoup(driver.page_source, "html.parser")
    
        tag = soup.find("pre")
        if not tag:
            raise ValueError("Conteúdo esperado não encontrado")

        data = json.loads(tag.text).get("data", [])
        df = pd.DataFrame(data, columns=["timestamp", "close", "open", "high", "low", "volume", "unknown"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df[df["date"] >= "1991-01-01"]
        df.drop(columns=["timestamp", "unknown"], inplace=True)
        df = df[["date", "close", "open", "high", "low", "volume"]]
        df.to_csv("/usr/local/airflow/include/datasets/usd_cny.csv", index=False)

        driver.quit()

    
    upload_usd_cny_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_usd_cny_csv_to_gcs",
        src="/usr/local/airflow/include/datasets/usd_cny.csv",
        dst="raw/usd_cny.csv",
        bucket=bucket_name,
        gcp_conn_id="gcp",
        mime_type="application/octet-stream",
    )


    create_papers_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_papers_dataset",
        dataset_id=dataset_name,
        gcp_conn_id="gcp",
    )

    papers_gcs_to_raw = aql.load_file(
        task_id="papers_gcs_to_raw",
        input_file=File(
            f"gs://{bucket_name}/raw/usd_cny.csv",
            conn_id="gcp",
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name="usd_cny_monthly",
            conn_id="gcp",
            metadata=Metadata(schema=dataset_name)
        ),
        use_native_support=True,
        native_support_kwargs={
            "encoding": "ISO_8859_1",
        }
    )

    chain(
        fetch_and_upload(),
        upload_usd_cny_csv_to_gcs,
        create_papers_dataset,
        papers_gcs_to_raw
    )

papers()
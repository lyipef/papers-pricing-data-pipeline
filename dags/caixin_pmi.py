from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.models.baseoperator import chain
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType


@dag(
    start_date=datetime(2025, 4, 25),
    schedule=None,
    catchup=False,
    tags=["caixin_pmi"],
)
def caixin_pmi():
    BUCKET_NAME = "data-engineer-test-suzano"
    DATASET_NAME = "papers"
    

    @task
    def fetch_and_upload():
        base_url = 'https://www.mql5.com/en/economic-calendar/china/caixin-services-pmi/history?page='
        page = 1
        data_total = []

        while True:
            url = f'{base_url}{page}'
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                data = []
                items = soup.find_all('div', class_='event-table-history__item')

                if soup.find('div', class_='ec-event__table__export'):
                    print("Export tag found. Stopping pagination.")
                    break
                
                for item in items:
                    period = item.find('div', class_='event-table-history__period')
                    actual = item.find('div', class_='event-table-history__actual') or \
                             item.find('div', class_='event-table-history__actual_red') or \
                             item.find('div', class_='event-table-history__actual_green')
                    forecast = item.find('div', class_='event-table-history__forecast')
                    previous = item.find('div', class_='event-table-history__previous')

                    data.append({
                        'Period': period.get_text(strip=True) if period else None,
                        'Actual': actual.get_text(strip=True) if actual else None,
                        'Forecast': forecast.get_text(strip=True) if forecast else None,
                        'Previous': previous.get_text(strip=True) if previous else None
                    })

                if page == 4:
                    break
                if not data:
                    break
                
                data_total.extend(data)
                page += 1
            else:
                break
            
        df = pd.DataFrame(data_total)

        df['date'] = pd.to_datetime(df['Period'] + ' 1', format='%b %Y %d')
        df.rename(columns={
            'Actual': 'actual_state',
            'Forecast': 'forecast',
            'Previous': 'close' 
        }, inplace=True)
        df.columns = df.columns.str.lower()
        df = df[["date", "actual_state", "close", "forecast"]]
        df.to_csv("/usr/local/airflow/include/datasets/caixin_pmi.csv", index=False)
    
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src="/usr/local/airflow/include/datasets/caixin_pmi.csv",
        dst="raw/caixin_pmi.csv",
        bucket=BUCKET_NAME,
        gcp_conn_id="gcp",
        mime_type="application/octet-stream",
    )


    create_papers_dataset_if_not_exist = BigQueryCreateEmptyDatasetOperator(
        task_id="create_papers_dataset",
        dataset_id=DATASET_NAME,
        gcp_conn_id="gcp",
    )

    papers_gcs_to_raw = aql.load_file(
        task_id="papers_gcs_to_raw",
        input_file=File(
            f"gs://{BUCKET_NAME}/raw/caixin_pmi.csv",
            conn_id="gcp",
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name="caixin_pmi_monthly",
            conn_id="gcp",
            metadata=Metadata(schema=DATASET_NAME)
        ),
        use_native_support=True,
        native_support_kwargs={
            "encoding": "ISO_8859_1",
        }
    )

    chain(
        fetch_and_upload(),
        upload_csv_to_gcs,
        create_papers_dataset_if_not_exist,
        papers_gcs_to_raw
    )

caixin_pmi()
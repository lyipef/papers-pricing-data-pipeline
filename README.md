# Papers Pricing Data Pipeline

Automation project to collect and store economic data from [Investing.com](https://br.investing.com).

This pipeline extracts historical data for the USD/CNY (US Dollar/Chinese Yuan) pair and loads it into a scalable cloud-based data structure on **Google Cloud**.

---


## Tech Stack

| Layer           | Tool                      |
|-----------------|----------------------------|
| Orchestration   | Apache Airflow             |
| Web Scraping    | Selenium + BeautifulSoup   |
| Cloud Storage   | Google Cloud Storage (GCS) |
| Database        | BigQuery                   |
| ETL Facilitator | Astro SDK                  |
| Containerization| Docker                     |

## How It Works

![Diagram](src/scraper/Diagrama%20em%20branco.png)

### fetch_and_upload
- Uses Selenium to scrape historical USD/CNY monthly data.
- Saves a CSV file locally inside the container.

### upload_usd_cny_csv_to_gcs
- Uploads the generated CSV to a specific path in a GCS bucket.

### create_papers_dataset
- Creates the `papers` dataset in BigQuery if it does not already exist.

### papers_gcs_to_raw
- Loads the CSV from GCS into BigQuery using Astro SDK, into the `usd_cny_monthly` table.

## To run this project you must.

### Install Astro CLI

Installation
1. Open Windows PowerShell as an administrator and then run the following command:

winget install -e --id Astronomer.Astro

2. Run astro version to confirm the Astro CLI is installed properly.
https://www.astronomer.io/docs/astro/cli/install-cli/?tab=windowswithwinget#install-the-astro-cli

### Install Docker
https://www.docker.com/products/docker-desktop/

### Clone the GitHub repo

In your terminal:

Clone the repo using Github CLI or Git CLI

```bash
git clone https://github.com/lyipef/data-engineer-test-suzano-filipe-freitas.git
```

### Reinitialize the Airflow project
Open the code editor terminal:
```bash
astro dev init
```
It will ask: ```You are not in an empty directory. Are you sure you want to initialize a project? (y/n)```
Type ```y``` and the project will be reinitialized.


### Build the project
In the code editor terminal, type:

```bash
astro dev start
```
The default Airflow endpoint is http://localhost:8080/

- Default username: admin
- Default password: admin

### Create the GCP project
In your browser go to https://console.cloud.google.com/ and create a project, recomended something like:  ```papers-pricing```

Copy your project ID and save it for later.

#### Create a Bucket on GCP

With the project selected, go to https://console.cloud.google.com/storage/browser and create a Bucket.
Use the name ```<yourname>_papers_pricing```.
And change the variable ```bucket_name``` value to your bucket name at the ```dags\papers.py``` file.

#### Create an service account for the project

Go to the IAM tab, and create the Service account with the name ```papers-pricing```.
Give admin access to GCS and BigQuery, and export the json keys. Rename the file to service_account.json and put inside the folder ```include/gcp/``` (you will have to create this folder).

#### Build a connection in your airflow

In your airflow, at the http://localhost:8080/, login and go to Admin → Connections.
Create a new connection and use this configs:
- id: gcp
- type: Google Cloud
- Keypath Path: `/usr/local/airflow/include/gcp/service_account.json`

Save it.

### All set, start the DAG

With your Airflow running, go to http://localhost:8080/ and click on DAGs, and click on the papers DAG.
Then, start the DAG (play button on the upper right side).

It will go step by step, and if everything was followed, you will get a green execution at the end.
Check in your GCP Storage account if the file was uploaded succesfully, in your BigQuery tab if the tables was been built and in your Soda dashboard if everithing is fine.
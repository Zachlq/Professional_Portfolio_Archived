
import json as json
import pandas as pd 
import os
import requests 
from google.cloud import bigquery
from google.cloud import storage 
import datetime 
from pynytimes import NYTAPI
from google.cloud.bigquery import SchemaField

def get_stories():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    nyt_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?q=coronavirus&api-key=API_KEY'
    nyt = requests.get(nyt_url)
    nyt_json = nyt.json()
    nyt_data = nyt_json['response']['docs']
    
    schema = [bigquery.SchemaField("abstract", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("web_url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("snippet", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("lead_paragraph", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("print_section", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("print_page", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("multimedia", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("rank", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("caption", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("credit", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("height", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("width", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("legacy", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("xlarge", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("xlargewidth", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("xlargeheight", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("crop_name", "STRING", mode="NULLABLE")
        ])
    ]),
    bigquery.SchemaField("headline", "RECORD", mode="REPEATED",
                        fields=[
                            bigquery.SchemaField("main", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("content_kicker", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("print_headline", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("seo", "STRING", mode="NULLABLE"),
                            bigquery.SchemaField("sub", "STRING", mode="NULLABLE"),
                                                ])]
    client = bigquery.Client()
    dataset_id = 'nyt'
    table_id = 'covid_articles'
    
    dataset_ref = client.dataset(dataset_id)
    table_id = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.write_disposition='WRITE_TRUNCATE'
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect=False
    job_config.ignore_unknown_values=True 

    job = client.load_table_from_json(
    nyt_data,
    table_id,
    location='US',
    job_config=job_config)
    
    job.result()
    
 if __name__ == "__main__":
  get_stories()

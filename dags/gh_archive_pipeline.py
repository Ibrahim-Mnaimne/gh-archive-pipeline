from datetime import datetime, timedelta
import gzip
import json
import logging
import os

import boto3
import pandas as pd
import psycopg2
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'ibrahim',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(minutes=15),
}


def fetch_gharchive(**context):
    logical_date = context['logical_date']
    hour = logical_date.hour
    date_part = logical_date.strftime('%Y-%m-%d')
    url = f'https://data.gharchive.org/{date_part}-{hour}.json.gz'
    path = f'/tmp/gharchive/{date_part}-{hour}.json.gz'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    logger.info("Fetching %s", url)
    logger.info("Writing to %s", path)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return path


def load_to_s3(**context):
    logical_date = context['logical_date']
    ti = context['ti']
    local_path = ti.xcom_pull(task_ids='fetch_gharchive')
    s3_key = f'raw/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/hour={logical_date.hour:02d}/events.json.gz'
    bucket_name = 'equities-pipeline-ibrahim-4471'
    s3 = boto3.client('s3')
    logger.info("Uploading %s to s3://%s/%s", local_path, bucket_name, s3_key)
    s3.upload_file(local_path, bucket_name, s3_key)
    os.remove(local_path)
    return s3_key


def transform(**context):
    ti = context['ti']
    raw_s3_key = ti.xcom_pull(task_ids='load_to_s3')
    logical_date = context['logical_date']
    hour = logical_date.hour
    date_part = logical_date.strftime('%Y-%m-%d')
    bucket_name = 'equities-pipeline-ibrahim-4471'
    local_raw_path = f'/tmp/gharchive/raw-{date_part}-{hour}.json.gz'
    local_parquet_path = f'/tmp/gharchive/events-{date_part}-{hour}.parquet'
    s3 = boto3.client('s3')
    logger.info("Downloading %s to %s", raw_s3_key, local_raw_path)
    s3.download_file(bucket_name, raw_s3_key, local_raw_path)
    rows = []
    with gzip.open(local_raw_path, 'rt') as f:
        for line in f:
            event = json.loads(line)
            rows.append({
                "event_id":    event.get("id"),
                "event_type":  event.get("type"),
                "actor_id":    event.get("actor", {}).get("id"),
                "actor_login": event.get("actor", {}).get("login"),
                "repo_id":     event.get("repo", {}).get("id"),
                "repo_name":   event.get("repo", {}).get("name"),
                "is_public":   event.get("public"),
                "created_at":  event.get("created_at"),
            })
    df = pd.DataFrame(rows)
    df['created_at'] = pd.to_datetime(df['created_at'])
    logger.info("Parsed %s events for hour %s", len(df), hour)
    df.to_parquet(local_parquet_path, engine='pyarrow', compression='snappy', index=False)
    processed_s3_key = f'processed/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/hour={logical_date.hour:02d}/events.parquet'
    logger.info("Uploading %s to s3://%s/%s", local_parquet_path, bucket_name, processed_s3_key)
    s3.upload_file(local_parquet_path, bucket_name, processed_s3_key)
    os.remove(local_parquet_path)
    os.remove(local_raw_path)
    return processed_s3_key


def compute_aggregates(**context):
    ti = context['ti']
    processed_s3_key = ti.xcom_pull(task_ids='transform')
    logical_date = context['logical_date']
    hour = logical_date.hour
    date_part = logical_date.strftime('%Y-%m-%d')
    bucket = 'equities-pipeline-ibrahim-4471'
    local_silver_path = f'/tmp/gharchive/silver-{date_part}-{hour}.parquet'
    local_agg_path = f'/tmp/gharchive/agg-events-by-type-{date_part}-{hour}.parquet'
    s3 = boto3.client('s3')
    logger.info("Downloading %s to %s", processed_s3_key, local_silver_path)
    s3.download_file(bucket, processed_s3_key, local_silver_path)
    df = pd.read_parquet(local_silver_path, engine='pyarrow')
    agg = df.groupby('event_type').size().reset_index(name='event_count')
    agg['event_hour'] = logical_date
    agg = agg[['event_hour', 'event_type', 'event_count']]
    agg.to_parquet(local_agg_path, engine='pyarrow', compression='snappy', index=False)
    logger.info("Aggregated to %s rows for hour %s", len(agg), hour)
    aggregate_s3_key = f'aggregates/year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}/hour={logical_date.hour:02d}/events_by_type.parquet'
    logger.info("Uploading %s to s3://%s/%s", local_agg_path, bucket, aggregate_s3_key)
    s3.upload_file(local_agg_path, bucket, aggregate_s3_key)
    os.remove(local_agg_path)
    os.remove(local_silver_path)
    return aggregate_s3_key


def load_to_postgres(**context):
    ti = context['ti']
    gold_s3_key = ti.xcom_pull(task_ids='compute_aggregates')
    logical_date = context['logical_date']
    hour = logical_date.hour
    date_part = logical_date.strftime('%Y-%m-%d')
    bucket = 'equities-pipeline-ibrahim-4471'
    local_gold_path = f'/tmp/gharchive/gold-{date_part}-{hour}.parquet'
    s3 = boto3.client('s3')
    logger.info("Downloading %s to %s", gold_s3_key, local_gold_path)
    s3.download_file(bucket, gold_s3_key, local_gold_path)
    df = pd.read_parquet(local_gold_path, engine='pyarrow')
    response = boto3.client('secretsmanager', region_name='eu-north-1').get_secret_value(SecretId='airflow/rds/credentials')
    secret = json.loads(response['SecretString'])
    with psycopg2.connect(
        host=secret['host'],
        port=secret['port'],
        dbname=secret['dbname'],
        user=secret['username'],
        password=secret['password'],
    ) as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO event_type_hourly (event_hour, event_type, event_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (event_hour, event_type)
                DO UPDATE SET event_count = EXCLUDED.event_count
            """
            rows = [(row['event_hour'], row['event_type'], int(row['event_count']))
                    for _, row in df.iterrows()]
            cur.executemany(sql, rows)
    conn.close()
    os.remove(local_gold_path)
    logger.info("Upserted %s rows for hour %s", len(df), hour)
    return len(df)


with DAG(
    dag_id='gh_archive_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    max_active_runs=3,
    default_args=default_args,
) as dag:

    fetch = PythonOperator(task_id='fetch_gharchive', python_callable=fetch_gharchive)
    load_s3 = PythonOperator(task_id='load_to_s3', python_callable=load_to_s3)
    tf = PythonOperator(task_id='transform', python_callable=transform)
    compute = PythonOperator(task_id='compute_aggregates', python_callable=compute_aggregates)
    load_pg = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)

    fetch >> load_s3 >> tf >> compute >> load_pg

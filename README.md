# gh-archive-pipeline

Hourly batch pipeline that pulls GitHub event data from GH Archive, processes it through a bronze/silver/gold medallion architecture on AWS S3, and loads pre-computed aggregates into RDS Postgres, orchestrated by self-hosted Apache Airflow on EC2.

## Architecture

```
GH Archive  (hourly JSON.gz, public HTTP)
       |
       v
  fetch_gharchive     -->  downloads file to /tmp on the worker
       |
       v
  load_to_s3          -->  uploads to S3 BRONZE (raw .json.gz)
       |
       v
  transform           -->  parses, flattens, writes Parquet to S3 SILVER
       |
       v
  compute_aggregates  -->  groups by event_type, writes Parquet to S3 GOLD
       |
       v
  load_to_postgres    -->  upserts gold rows into RDS Postgres
```

- **Bronze (S3 raw):** Raw GH Archive JSON.gz files, one per hour, stored raw with no change as the source of truth. Each file is ~24 MB compressed and contains ~165k events.
- **Silver (S3 processed):** Flattened Parquet files with an 8-column schema (event_id, event_type, actor, repo, created_at, etc.). Each file is ~6.6 MB and 3.6× smaller than the raw JSON.gz due to columnar compression.
- **Gold (S3 aggregates + RDS):** Pre-computed event counts grouped by (event_hour, event_type). ~10-16 rows per hour depending on event mix. Served from RDS Postgres for low-latency operational queries.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.5 (self-hosted, Docker Compose) |
| Cloud | AWS (EC2, S3, RDS Postgres, Secrets Manager, IAM, SSM) |
| Storage | S3 (bronze/silver/gold), RDS Postgres 16 (gold serving) |
| Processing | Python 3.12 (pandas, pyarrow, psycopg2, requests) |
| Testing | pytest |
| Infrastructure | Docker 25, Amazon Linux 2023, t3.small |

## Design Decisions

**Self-hosted Airflow on EC2 vs MWAA**
MWAA starts at ~$300/month, which is unjustifiable for a portfolio project. Self-hosting on EC2 also demonstrates infrastructure skills that MWAA abstracts away, Docker Compose setup, executor configuration, container health management, which are real skills, not hidden by a managed service.

**LocalExecutor vs CeleryExecutor**
CeleryExecutor requires Redis and Celery worker containers, which would exhaust the 2 GB RAM on a t3.small alongside the existing Airflow stack. LocalExecutor runs tasks as scheduler subprocesses, no additional services needed. The DAG code is identical between executors, so switching later requires only a config change.

**GH Archive as the data source**
GH Archive publishes one JSON.gz file per hour containing all public GitHub events, no API key, no rate limits, no cost. Each file is ~20-150 MB with ~100k-200k events, providing realistic data volume for batch pipeline work. The hourly cadence aligns naturally with the `@hourly` Airflow schedule.

**Parquet vs CSV for the silver layer**
Parquet stores data column-by-column, so downstream queries that only need `event_type` and `event_count` read only those columns, not the entire row. It also enforces schema and carries type information, eliminating parsing ambiguity. A silver file of ~165k events compresses to 6.6 MB in Parquet vs 23.9 MB as raw JSON.gz.

**RDS Postgres for the gold layer instead of keeping everything in S3**
S3 + Athena answers aggregate queries in 3-5 seconds with per-query cost. RDS Postgres answers the same point-lookup query (`WHERE event_hour = X`) in under 1ms with no per-query cost. The gold layer holds only ~10-16 rows per hour, a row-store database is the right tool for that shape of data and that query pattern.

**Hive-style partitioning on S3 keys**
S3 keys follow the `year=YYYY/month=MM/day=DD/hour=HH/` convention. Tools like Athena, Glue, Spark, and DuckDB auto-discover these as partition columns at query time, no manual schema registration needed. Queries filtering by date or hour skip irrelevant partitions entirely, reducing both scan time and cost.

**Why no Spark, no Kafka, no streaming?**
GH Archive's hourly volume (~20-150 MB, ~165k events) is well within pandas' capability on a single node, adding Spark would be complexity for no performance gain. Streaming with Kafka is not appropriate for a data source that publishes complete hourly batches. Both are covered in later portfolio projects at appropriate data scales.

## Project Structure

```
gh-archive-pipeline/
├── dags/
│   └── gh_archive_pipeline.py    # Airflow DAG, all five pipeline tasks
├── tests/
│   ├── __init__.py
│   └── test_transform.py         # pytest suite, JSON parsing logic
├── docs/
│   └── runbook.md                # Daily startup/shutdown + troubleshooting
├── requirements.txt              # Python dependencies
├── .gitignore                    # Excludes logs, .env, __pycache__
└── README.md
```

## Setup

### Prerequisites
- AWS account with IAM user (`ibrahim-admin` or equivalent) configured via `aws configure`
- EC2 instance (`t3.small`, Amazon Linux 2023) with Docker and Docker Compose installed
- S3 bucket with Hive-partitioned prefix structure
- RDS Postgres 16 instance in the same VPC as the EC2
- AWS Secrets Manager entry at `airflow/rds/credentials` containing Postgres credentials
- SSM Session Manager plugin installed locally for shell access

### Running the pipeline

```bash
aws ssm start-session --target <your-instance-id> --region eu-north-1
sudo su - ec2-user
cd ~/airflow-project
sudo systemctl start docker.socket
sudo systemctl start docker
docker compose up -d
docker compose ps   # wait for all 4 (healthy)
```

### Stopping

```bash
docker compose down
aws ec2 stop-instances --region eu-north-1 --instance-ids <your-instance-id>
aws rds stop-db-instance --region eu-north-1 --db-instance-identifier airflow-rds
```

> **Note:** AWS automatically restarts stopped RDS instances after 7 days.

## Testing

The test suite covers the JSON-parsing and schema-validation logic inside the `transform` task, the function that flattens raw GH Archive events into structured rows. This logic was extracted and tested in isolation because it contains the core data-shaping decisions and has no I/O dependencies.

```bash
cd tests
python3 -m pytest test_transform.py -v
```

Expected output:

```
test_transform.py::test_parses_three_events_to_three_rows PASSED    [ 25%]
test_transform.py::test_event_fields_extracted_correctly PASSED      [ 50%]
test_transform.py::test_missing_actor_field_handled PASSED           [ 75%]
test_transform.py::test_handles_empty_input PASSED                   [100%]

4 passed in 0.03s
```

## Security

- **No credentials in code:** Postgres credentials are stored in AWS Secrets Manager and fetched at runtime via boto3. No passwords, keys, or connection strings appear in code, environment variables, or version control.
- **Private RDS:** RDS has no public IP and is not accessible from the internet. Inbound port 5432 is permitted only from `equities-pipeline-sg`, the EC2's security group, using identity-based rules rather than CIDR ranges.
- **Least-privilege IAM:** EC2 role scoped to specific S3 bucket and specific Secrets Manager ARN, no wildcards.
- **SSM Session Manager:** Access via SSM over HTTPS, no open port 22, no key files on disk.

## What I Learned

- The three-fence security framework (IAM / Network / Application credentials), the mental model that makes AWS debugging systematic
- Idempotency as a design discipline, not just "it works" but "it works safely on retry"
- Parsing streaming JSON-lines (NDJSON) format, each GH Archive file is not a JSON array but one object per line, which requires a different read pattern than standard JSON and fails silently if you use the wrong approach
- XComs carry pointers not data, passing DataFrames through XCom bloats the metadata DB and breaks at scale

## Future Work

- Multi-AZ RDS for production failover (currently single-AZ, cost reason documented)
- CI/CD pipeline, currently deploy is a heredoc on the EC2; production would use GitOps
- Additional aggregates, top repos by hour, unique actors by hour, currently only event_type counts

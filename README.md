# Batch Processing: Customer Quality Anlysis Data Engineering Project

## Summary
A data pipeline that transforms raw data from GCS and load curated data into BQ for downstream analysis.

## Pipeline
![Github](https://github.com/user-attachments/assets/70a8de46-7e85-439d-827a-5616da763738)

The data I used is from the CSV file which is updated everyday in an GCS bucket. The data is transformed using dataproc into bigquery where I ran search queries. To make the application the data is ingested every day which was scheduled with Airflow.

## Challenges
To build a performant, scalable and automated data pipeline. 

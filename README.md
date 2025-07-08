# Batch Processing: Customer Quality Anlysis Data Engineering Project

## Summary
A data pipeline that transforms raw data from hive and load curated data into BQ for downstream analysis.

## Pipeline
/Users/saikiran/Downloads/flowchart.jpg

The data I used is from the hive table which is updated everyday and acting as source for pipeline. Pyspark transformations are done on raw data which runs on dataproc cluster  and loads curated data into bigquery where I ran search queries. To make the application the data is ingested every day which was scheduled with Airflow.

## Challenges
To build a performant, scalable and automated data pipeline. 

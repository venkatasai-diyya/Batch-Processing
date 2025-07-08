# Batch Processing: Quality Experience Score (QES) Data Engineering Project

## Summary
A data pipeline that transforms raw data from hive and load curated data into BQ for downstream analysis.

## Pipeline
![flowchart](https://github.com/user-attachments/assets/1a8d7ec8-7d89-40ff-83a9-815ff3a4a0d2)


The data I used is from the hive table which is updated everyday and acting as source for pipeline. Pyspark transformations are done on raw data which runs on dataproc cluster  and loads curated data into bigquery where I ran search queries. To make the application the data is ingested every day which was scheduled with Airflow.

## Challenges
To build a performant, scalable and automated data pipeline. 

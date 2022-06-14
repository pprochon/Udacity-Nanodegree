# Sparkify ETL with Postgres and Python

In this project, we'll learn data modeling with Postgres and build an ETL pipeline using Python.

We will follow the following steps:
- Data modeling with Postgres
- Creating star schema database
- ETL pipeline using Python

## Project Description

This project is for creating a SQL analytics database for a music streaming startup called Sparkify. Sparkify's analytics team want to understand what songs users are listening on the company's music app. They need an easy way to query their data stored in raw JSON files (logs on user activity on the app and songs metadata).

## Project Dataset

- **Song Dataset**: files are partitioned by the first three letters of each song's track ID e.g. */data/song_data.*.json. 

- **Log Dataset**: files in the dataset you'll be working with are partitioned by year and month e.g. */data/log_data.*json. 

## Data Modeling

We will create a Star Schema: 
one fact table consist of the measures associated with each event *songplays*, 
and  referencing four dimensional tables *songs*, *artists*, *users* and *time*.

In this case we use relational database because:

- the data types are structured (we know before-hand the structure of the  data in jsons files, and we are able to extract and transform it with ease);
- we have a small amount of data that we need to analyze;
- this structure will enable to aggregate the data efficiently;
- we can use SQL for this kind of analysis;
- We need to use JOINS for this scenario.

## Project template

The data files, the project includes seven files:
1. ***create_tables.py*:** drops and creates your tables. This file serves to reset your tables before you run your ETL scripts again.
2. ***etl.ipynb*:** reads and processes a single file from *song_data* and *log_data* and loads the data into your tables. Detailed instructions on the ETL process for each table.
3. ***etl.py*:** reads and processes files from *song_data* and *log_data* and loads them into your tables. Prepared after testing the ETL notebook
4. ***README.md*:** provides info on this project.
5. ***sql_queries.py*:** contains all sql queries used in files above.
6. ***test.ipynb*:** Checks the ETL processing.

## How to Run

1. Run ***create_tables.py*** to create the database and tables.
2. Run ***etl.py*** to process for loading, extracting and inserting the data.
3. Run ***test.ipynb*** to confirm the creation of database and columns.
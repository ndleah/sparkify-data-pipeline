# Project 5: Data Pipelines with Airflow

## Summary
- [Project 5: Data Pipelines with Airflow](#project-5-data-pipelines-with-airflow)
  - [Summary](#summary)
- [Sparkify - Data Pipelines ](#sparkify---data-pipelines-)
  - [Introduction](#introduction)
  - [Dataset](#dataset)
    - [ELT Process](#elt-process)
    - [Sources](#sources)
    - [Destinations](#destinations)
        - [Prerequisite](#prerequisite)
    - [Project Structure](#project-structure)
    - [Data Quality Checks](#data-quality-checks)

--------------------------------------------

![Star Badge](https://img.shields.io/static/v1?label=%F0%9F%8C%9F&message=If%20Useful&style=style=flat&color=BC4E99)
![Open Source Love](https://badges.frapsoft.com/os/v1/open-source.svg?v=103)
# Sparkify - Data Pipelines <img src="https://static.vecteezy.com/system/resources/previews/009/726/165/original/pixel-art-isometric-islands-in-the-sky-with-trees-bridge-lake-and-fence-8bit-game-scenario-vector.jpg" align="right" width="150" />

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

![Airflow DAG](assets/dag.png)

## Dataset
**Note** that the actual data (in *JSON*) used in this project is a subset of original dataset preprocessed by the course. The provided data 
resides in AWS S3 (publically available).
1. Song data from [Million Song Dataset](http://millionsongdataset.com/)
2. User activity data from [Event Simulator](https://github.com/Interana/eventsim) based on [Million Song Dataset](http://millionsongdataset.com/)

### ELT Process

The tool used for scheduling and orchestrationg ELT is Apache Airflow.

'Airflow is a platform to programmatically author, schedule and monitor workflows.'

Source: [Apache Foundation](https://airflow.apache.org/)

The schema of the ELT is this DAG:

![DAG](./images/dag.png)

### Sources

The sources are the same than previous projects:

* `Log data: s3://udacity-dend/log_data`
* `Song data: s3://udacity-dend/song_data`

### Destinations

Data is inserted into Amazon Redshift Cluster. The goal is populate an star schema:

* Fact Table:

    * `songplays` 

* Dimension Tables

    * `users - users in the app`
    * `songs - songs in music database`
    * `artists - artists in music database`
    * `time - timestamps of records in songplays broken down into specific units`

By the way we need two staging tables:

* `Stage_events`
* `Stage_songs`

##### Prerequisite   

Tables must be created in Redshift before executing the DAG workflow. The create tables script can be found in:

`create_tables.sql`



### Project Structure

* /
    * `create_tables.sql` - Contains the DDL for all tables used in this projecs
* dags
    * `udac_example_dag.py` - The DAG configuration file to run in Airflow
* plugins
    * operators
        * `stage_redshift.py` - Operator to read files from S3 and load into Redshift staging tables
        * `load_fact.py` - Operator to load the fact table in Redshift
        * `load_dimension.py` - Operator to read from staging tables and load the dimension tables in Redshift
        * `data_quality.py` - Operator for data quality checking
    * helpers
        * `sql_queries` - Redshift statements used in the DAG

### Data Quality Checks

In order to ensure the tables were loaded, 
a data quality checking is performed to count the total records each table has. 
If a table has no rows then the workflow will fail and throw an error message.
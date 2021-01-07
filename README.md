# Data Lakes
This is a data lake project for the Udacity Data Engineering Nanodegree program.

## Project Overview
In this project we'll manipulate Sparkify data (music streaming startup) using the Amazon S3 storage as a *data lake*. A **data lake** is a repository where data is stored in its raw format along with all necessary transformations used for reporting, visualization, advanced analytics and machine learning. The data lake can ingest all types of data - from *structured data* of relational databases, *semi-structured* data (CSV, logs, XML, JSON) to *unstructured data* (emails, documents, PDFs) and binary data (images, audio, video) (extract from https://en.wikipedia.org/wiki/Data_lake).

We'll use the Apache Spark analytic engine (pyspark for python) which is a mainstream technology for Big Data projects. Although our project dataset is rather small, our *ETL pipeline* will be designed for high scalability which will permit to manipulate much largers datasets.

Our task is:

 1. To import data from JSON files (song and log event data) into spark dataframes.
 2. To transform datasets from two spark dataframes into five tables applying the star schema:
     - 4 dimension tables (*time*, *users*, *songs*, *artists*)
     - fact table (*songplays*)
 3. Load star tables back into S3 in parquet format.

The *transformation* part of the ETL process is the most critical one since it must assure that the data from original JSON structure (as dataframe) is properly transformed into analytic tables designed as star schema which will be later used by our analytics team. That said, we'll have to:

 - *Filter* log event data to extract song-related data only since the event logs also cover non-music-listening events.
 - Pay special attention to *time dimension* with all kinds of time portions that are required,
 - Carry out *data casting* where necessary.
 - *Properly handle duplicates* before inserting data into dimension tables. For example, the user dimension contains the *level* attribute which is a *user-time related* since the subscription level that belongs to a user in every moment *can change in time*. We'll opt for picking the level from the last user event log record. 

In the last phase we'll store transformed data back to S3 using *parquet* format with required partitioning.

The ETL code will be executed on Amazon EML cluster properly configured to support Apache Spark analytic engine.

## Installation
### Clone
```sh
$ git clone https://github.com/amosvoron/udacity-data-lakes.git
```

## Repository Description

```sh  
- etl.py                 # ETL python code
- dl.cfg                 # configuration file
- README.md              # README file
- LICENCE.md             # LICENCE file
```

## License

MIT

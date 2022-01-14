IMDB Movie Data Warehouse
============
This is a python powered application that scrapes the top 50 English movies per genre on IMBD and return a star schema in Postgres for analytical use
---

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)
* [Data](#data)
* [Validation](#validation)
* [Discussion](#discussion)

## General info
This project is for consideration for employment with nwo.ai
	
## Technologies
Project is created with:
* Python version: 3.8.9
* docker-compose version: 3.7
* postgres version: 14.1
	
## Setup
To run this project, in the base directory first create the postgres instance with docker-compose:

```
$ docker-compose up -d
```

After creating a python virtual environment, install the required packages with the pip command

```
$ pip install -r requirements.txt
```


To run this project:

```
$ python main.py
```

## Data

After running main, you can view data with Adminer at http://localhost:8080 or use another software. Login with the following credentials:
```
System : PostgreSQL
Server : postgres (localhost:5438)
Username : root
Password : changeme
Database : moviesdb
```

## Validation

After running the main, you can validate the ingested that with validate.py. Run the following command in the base directory:
```
$ python ./validate/validate.py
```

## Discussion
* Roadmap to operationalize your system in a production environment
  * Moving PostgreSQL Server into a cloud environment (AWS)
    * Create PostgreSQL instance in Amazon RDS
    * Create new tables in instance using the same load table load script
  * Migrate fact and dimension tables to the new PostgreSQL instance
    * Change connection string parameters in code
    * Move files from local folders to Amazon S3 for use as a data lake
        * Create 2 buckets: one for raw zip files that are to be ingested and another for archived zip files that have already been ingested
        * Enable cold storage for archive files because they will not be accessed frequently
  * Convert the local python project into separate Lambda functions (extract, transform, load, validate) (serverless microservice)
    * Create 4 new Lambda functions: extract, transform, load, validate
    * Create an AWS EventBridge instance to handle the triggering of Lambda functions
    * Create an EventBridge rule to run the extract Lambda function daily
    * Add an S3 event trigger to the transform function to run when a new file is added to the S3 bucket
    * Trigger load function directly or use SNS to publish/subscribe to a load data notification
    * The validate function will be run after ETL
  
* Benefits and tradeoffs of your architecture (be specific with your choice of
technologies)
  * The only 3rd party package used for data transformation and schema design was pandas.
    * Benefits
      * Not dependant or effected by package end-of-life or unexpected upgrades beyond pandas.
      * Can pivot and add changes to schema and underlying logic easily and simply
    * Tradeoffs
      * Might be slower to develop and integrate than using existing etl 3rd party packages
  * Leverage postgres sql queries to find deltas between tables
    * Benefits 
      * faster processing time then reading to a variable in python and calculating the result
    * Tradeoffs
      * Harder to maintain sql queries written in python code.
  * Used docker-compose to spin up containers
    * Benefits
      * easier to change, debug, and read than Docker files
    * Tradeoffs
      * docker-compose is used mainly for use on a single computer and would not be supported if this project were to need multiple containers and orchestration

* Your choice of schema design pattern (i.e. STAR, 3NF, Snowflake)and why
you believe it was appropriate for this scenario
  * I chose to use a STAR schema for the purposes of this assignment. The goal and use of the underlying data is for analytics and machine learning models which favor the use of highly performative systems and queries. My system contains only two joins to view the whole data set which leads to simple and fast queries needed for the intended use-case. In order to avoid additional joins, I chose not to  normalize the tables. One potential drawback for the schema I selected is that analytics for the IMDB data is limited to movie performance through time, however, assuming a potential client is in the movie industry, I believe clients would be mostly interested in performance over any other metrics. Another potential drawback is that the size of the database may grow much fast than a snowflake schema. I addressed this concern by verifying storage space was not a limiting constraint on the system.
  * Extra Credit
      * If I were building this system for an analyst workflow there would be a few changes I would make to the schema. Currently there are a few columns (actors, directors, genres) where a single value could represent a group. I did this to maintain a star schema and avoid flattening the value across multiple columns (e.g. actor1, actor2, actor3, etc.). In this case, I would create bridge tables that contained the group (actor_group) and foreign key relationship to the individuals in the group (actor). This would add an additional join per group, however querying for a single actor or director would be more intuitive and human-readable. Beyond this change I would maintain all other aspects of the schema for increased performance over a normalized schema.

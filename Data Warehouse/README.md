## Goals

We are creating a data warehouse where Sparkify analytics team would be able to analyze user activity and
specifically what songs the users listen to. With that in mind the database is designed in a way that
supports flexibility on analytical queries. Queries like "Top 10 favourite artists that users from New
York listened to in the year of 2020".

Also the analytics team can easily run aggregate queries like "How much time has user A spent on listening
songs on the first week of March" or "How many users from San Francisco have listened to songs of Eminem
in the year of 2022".

We make it easy and efficient for the team to write these queries as there is at most one JOIN operation
included (advantage of the denormalized form) to get a piece of information (e.g. join songplays with
artists to find artist's name).

With the assumption that this database is to be used only on past data, the database won't require
future inserts and updates, therefore we don't get the disadvantages that come with denormalized forms
i.e. update or insert values on many places (since there is no need of any inserts or updates).

All the above was copied from what I wrote in the first assignment.
What is new is that we are now using Redshift for our data warehouse in order to take advantage of the parallelization of the queries. Our analytics queries now run faster since each each node on the cluster and more specifically each CPU core on each node performs computation on a slice of the table data when a query is run. We have a huge performance gain comparing to Postgres where a single node does the computation for the whole table.


## Run the scripts - Jupyter notebook

First create the cluster and IAM role using execrcise 2 of the lesson "Implementing Data Warehouses on AWS
" (L3 Exercise 2 - IaC - Solution) and get the ARN of the role and the cluster endpoint in step 2.2 and add them to dwh.cfg.

Then run ETL_and_analytics.ipynb notebook where the first two commands are for running the create tables and etl scripts
and the rest of the commands are for running the analytical queries.
 
## Repository files

***create_tables.py***\
Creates the Sparkify tables.
First it drops the tables if they already exist and then creates them.

***etl.py***\
Runs the ETL from the song_data and log_data to the songplays, artists, songs, users and time tables.
Inserts the song_data and log_data into staging_songs and staging_events tables.
Extracts the required information from the staging tables and then inserts the data into the songplays, 
artists, songs, users and time tables.

***sql_queries.py***\
Contains all the sql statements that create_tables.py and etl.py need to run.\
DROP and CREATE statements for create_tables.py\
INSERT and SELECT staments for etl.py\
COPY FROM statements for bulk insertion for inserting data to the staging tabless.


## ETL justification
We are  loading data from the song data and log data to the staging tables and from the staging tables we are inserting the data to the data warehouse tables. 
For all the tables the ETL is straightforward except for songplays and users which needed extra steps.I will explain these extra steps below.

**Table songplays**

- *Using a LEFT JOIN on song name and artist name fields between the two staging tables*\
We do this in order to get the song_id and artist_id from the staging_songs tables.
I used a LEFT JOIN though we have many records in the staging_events that their song name and artist name
do not match with any record on the staging songs. We still want to keep this information. 

**Table users**

- *WHERE (userId, ts)
   IN\
       (SELECT userId, max(ts) as ts\
        FROM staging_events\
        GROUP BY userId\
       )\
    ;*\
We are using this in order to have the most recent user's level.   



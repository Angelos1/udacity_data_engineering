## Goals

We are creating a database where Sparkify analytics team would be able to analyze user activity and
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
What is new is that we are now using parquet files in S3 for our data warehouse in order to take advantage of their columnar storage for better performance on fetching column data. Also we gain storage efficiency as compressed columnar files take less space.
In addition we keep the raw data in s3 along with the created data warehouse. This serves well the data scientists and machine learning engineers since they have a lot of flexibility on reading the data however they want and they can run ad-hoc queries on the raw data directly (schema-on-read).
Therefore our data lake can now serve Data Scientists and ML Engineers along with business analysts in comparison with our Redshist data warehouse in the previous assigment which could only serve business analysts.
Lastly we run our spark jobs on the EMR cluster in which we take advantage of the mass parallel in-memory computation and gain significant performance. Additionally our initial data and our data warehouse data reside on S3 which is in AWS so loading data in the EMR cluster is also relevantly fast since EMR also resides in AWS EC2 servers.



## ETL justification
We are  loading data from S3 song data and log data to the S3 data warehouse tables. 
For all the tables the ETL is straightforward except for songplays and users which needed extra steps.I will explain these extra steps below.

**Table songplays**

- *Using a LEFT JOIN on song name and artist name fields between the initial dataframes*\
We do this in order to get the song_id and artist_id from the song data .
I used a LEFT JOIN though we have many records in the staging_events that their song name and artist name
do not match with any record on the staging songs. We still want to keep this information. 


**Table users**
In the users table we want to have the most recent level a user has. Therefore we partition the df by user and order the records for each user chronologicaly and in descending order using a window function to assign row_numbers in each partition. Then we keep only the records with row_number = 1 which is the most recent record for the users.
 
         user_window = Window.partitionBy('user_id').orderBy(desc('ts'))
         users_table = users_table.withColumn("row_number", row_number().over(user_window))
         users_table = users_table.filter(users_table.row_number == 1).drop("row_number").drop("ts")
 
\
\
**!!!Please ignore the two Jupiter notebooks as it is casual code for testing different parts of the program!!!**\
**!!!Also I deleted my AWS keys from the cfg file!!!**
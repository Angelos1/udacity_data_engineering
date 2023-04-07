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

## Run the scripts - Jupyter notebook

Create the tables.
```bash
%run create_tables.py
```

Run the ETL python script.
```bash
%run etl.py
```

## Repository files

***create_tables.py***\
Creates the Sparkify database and the tables.
First it drops the database if it already exists and then creates it, then creates the tables.

***etl.py***\
Runs the ETL from the song files and log files to the songplays, artists, songs, users and time tables.
Reads the song files and log files into python dataframes, processes their data to extract the
requred information and then inserts the data into the tables.

***sql_queries.py***\
Contains all the sql statements that create_tables.py and etl.py need to run.\
DROP and CREATE statements for create_tables.py\
INSERT and SELECT staments for etl.py\
COPY statements for bulk insertion for time and users tables.

## Schema design
Describing the main schema design considerations through the lines of the 
CREATE TABLE statements from the sql_queries.py file.

**Table songplays**

- *songplay_id bigserial as PRIMARY KEY*\
Used the bigserial data type so that we have a unique incremental identifier for each record. 
In addition I used bigserial and not just serial because I am expecting a lot of activity 
for users thus a lot of records.

- *start_time timestamp NOT NULL REFERENCES time(start_time)*\
NOT NULL because the start time is expected to always be recorded by the system (otherwise there's a possible bug).
We also have a foreign key to time.start_time because this is the join attribute for both tables and start_time is the primary key of time table.

- *user_id int NOT NULL REFERENCES users(user_id)*\
NOT NULL because the primal goal for the Sparkify database is to analyze user activity so we need every songplay record to have information for user.
We also have a foreign key to users.user_id because this is the join attribute for both tables and user_id is the primary key of users table.

- *song_id varchar REFERENCES songs(song_id)*\
We have a foreign key to songs.song_id because this is the join attribute for both tables and song_id is the primary key of songs table.

- *artist_id varchar REFERENCES artists(artist_id)*\
We have a foreign key to artists.artist_id because this is the join attribute for both tables and artist_id is the primary key of artists table.

- *UNIQUE(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) constraint*\
We have the above unique constraint because the combination of the above attibuttes uniquely identifies a user activity. This constraint protects us from the case where the log files contain duplicate data.

**Table users**

- *user_id int PRIMARY KEY*\
every user is uniquely identified by the user_id

- *gender char(1)*\
Genders are 'M' or 'F' so no I chose char(1) for it's data type to save memory space.
No need to use varchar for this field.

- *level varchar*:\
I chose varchar as the data type for level because I am not sure if there is another level
other than 'free' or 'paid'.
If I was confident that the level values are only 'free' and 'paid' I would choose char(4).

**Table songs**

- *song_id varchar PRIMARY KEY*\
every song is uniquely identified by the song_id.

**Table artists**

- *artist_id varchar PRIMARY KEY*\
every artist is uniquely identified by the artist_id.

- *latitude double precision*\
*longitude double precision*\
I chose double precision data type for both latitude and longitude because they have a lot of
decimal points.

**Table time**

- *start_time varchar PRIMARY KEY*\
every time instance is uniquely identified by the start_time which is a timestamp.


## ETL justification
We are directly loading data from the song files and log files fields so the ETL is pretty straightforward. 
For all the tables the ETL is straightforward except for songplays which has an extra step.
I believe that what explains the ETL pipeline completely are the ideas behind the ON COFLICT part of the insert statements. 
That is the way I will try to explain it.

**Table users**

- *ON CONFLICT (user_id) DO\
    UPDATE SET level = EXCLUDED.level*\
We are loading user data from the log files. The log files contain user activities therefore a user_id might appear many times in the dataset and it may have different levels each time. Assuming that the etl.py file reads the data files records in a chronologicall order, we use the above ON CONFLICT statement to update the user's level to the most recent one evevry time there is a conflict.   

**Table songs**

- *ON CONFLICT (song_id) DO NOTHING*\
We use this in case there are duplicated data in the song files.

**Table artists**

- *ON CONFLICT (artist_id) DO NOTHING*\
There are artists that have more than one song and therefore if artist_id is present in more than one song files we do not load them the second time they appear on our ETL.

**Table time**

- *ON CONFLICT (start_time) DO NOTHING*\
Even though it is almost impossible, two users might perform an activity at the same time, or one user
might click a button that triggers two actions (e.g GET songs and GET artists). 
In this case there will be two records in the log file  with the same timestamp.
Therefore we use ON CONFLICT DO NOTHING so that do not load the same timestamp twice.

**Table songplays**

- *ON CONFLICT (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) DO NOTHING*\
In case that there were duplicate data in the data files we do not load them the second time they appear on our ETL.

- Now for the songplays ETL data we have an extra step because we need to populate it with data from
both song files and log files. The way we perform this is that for each record in the log files
we use the fields song_name, artist_name and duration of the song and with these we query the artists
and songs tables (see song_select query in sql_queries.py) in order to get artist_it and song_id. Then we write the full songplay record in the database.


## Implementation with COPY command for Bulk insertion (incomplete)

I made an attempt to implement this but it is incomplete and not tested because I had no time.
The implementation is in etlCOPYcommand.py file which is like the etl.py file but it has some changes on the implementation of the process_log_file function.
What is done differently that I save the time and user dataframes into csv files and then I use this csv files to perform the COPY statements.
I didnt't have time to test this extensively but for some reason the COPY statement of the time table runs but does not insert any records (while running the same statement in test.ipynb runs successfully).
In addition the COPY statement does not have ON CONFICT handling so currently my implementation will violate primary key constraints because it will try to insert duplicate data (e.g. insert records with the same user_id in the users table).
If I had time I would try to handle this by using the COPY command to insert the data from the csv files to a temporary table (e.g. users_tmp_table and time_tmp_table) and then use the following commands to insert from the temporary tables to the final tables (as discused in this [post](https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update)):\
\
***INSERT INTO users\
SELECT \*\
FROM users_tmp_table\
ON CONFLICT (user_id) DO\
    UPDATE SET level = EXCLUDED.level;***
    
The same way I would handle the time table:

***INSERT INTO time\
SELECT \*\
FROM time_tmp_table\
ON CONFLICT (start_time) DO NOTHING\
;***
    

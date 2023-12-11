# Project: Data Modeling with Postgres

A startup called Sparkify wants to analyze the data that it's been collected on songs and user activity on their new music streaming app. This project requires using a Postgres database and ETL pipeline to optimize queries for analysis.

## Files
These are the files used in the project:

*data* - holds the json "log_data" and "song_data" files.
*etl.ipynb* - notebook used to explore data and use the ETL process.
*test.ipynb* - notebook used to test if the data was loading correctly in etl.py
*create_tables* - is run to create the database and tables.
*etl.py* - reads and processes data, and loads into tables.
*sql_queries.py* - defines the SQL queries used in the project.

## Schema

I first wrote DROP statements to reset the tables, and then wrote CREATE statements to create each table. I then made a star schema using the tables below:

#### Fact Table:
1. songplays
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent  
    
#### Dimension Tables:  
2. users  
   * user_id, first_name, last_name, gender, level  
3. songs 
   * song_id, title, artist_id, year, duration  
4. artists
   * artist_id, name, location, latitude, longitude  
5. time 
   * start_time, hour, day, week, month, year, weekday

After writing INSERT statements, I then ran 'create_tables.py' in the jupyter notebook to create the database and tables.

## Running Scripts

To run the scripts in this project, I used the Jupyter notebook. I first ran create_tables.py, to create the database and tables. I then ran the etl.ipynb notebook, and used it to implement etl.py. I also used the test.ipynb notebook to confirm that the data was loading correctly while I was using etl notebook.  



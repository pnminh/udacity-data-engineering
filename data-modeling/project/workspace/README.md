# Sparkify's song ETL pipeline

## Nature of the ETL pipeline
Sparkify data team finds it extremely important to understand the needs to keep our customers engage with the platform and provide new features based on their need. to the music they love to listen to based on their song preferences. 
Based on the current song list that an user is listening to, we hope to find patterns that allows us to understand our user preferences and recommend new songs to them.   
The data reside on multiple raw data-sources, many of which come from JSON files. 
The ETL pipeline provides an easy way to parse the data from these files and save them into Postgres for further analysis

## Purpose of the pipeline

Our data team at Sparkify has been working extensively on analyzing the habit our customers on music selection in order to provide them with efficient recommendations on the next songs they like to listen to.
The data pipeline provides a facilitating tool to extract data from JSON files, transform the data into fact and dimension tables based on STAR schema, and then load those data into Postgres Database.

![schema.png](schema.png)
## Step-by-step ELT pipeline

## Example queries
1. Specify the list of playing songs for an user. 
    ```sql
    select u.first_name ,u.last_name, s.title from songplays sp
    join users u
    on sp.user_id = u.user_id
    join songs s
    on sp.song_id = s.song_id ;
    ```
2. Look for how popular each song is based on its occurrences in the current song playing list for all users.
    ```sql
    select s.title, count(sp.song_id ) as occurrences  from songplays sp
    join songs s 
    on sp.song_id = s.song_id
    group by s.title 
    order by occurrences;
    ```
## About the company
Sparkify is a company focusing on providing users with the highest quality music collections.
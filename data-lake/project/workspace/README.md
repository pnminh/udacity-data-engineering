# Sparkify's song-play spark data pipeline
The data pipeline uses AWS EMR to extract data from song and log JSON files from S3 bucket
then create several tables using STAR schema.

## The nature of the song-play Spark data pipeline
Sparkify finds it extremely important to keep the customers
engaged with and stay active on our platform by continuously
updating the application with new features tailored to the users' needs
by analyzing their listening activities on Sparkify.

We believe that based on the current song list that each user is listening to,
we will be able to find patterns that allow us to understand
their preferences and recommend them with new and related songs, as well
as provide meaningful data to our other analysis tools.

Our existing process involves the creation of a dataset provided as JSON files and
including our whole song libraries and log data from all of our users' current playing songs
on our Sparkify streaming application.

The project provides a Spark data pipeline to easily parse the data from these files, create tables from these files and load them into an output S3 bucket for further analysis.

## Spark data pipeline designs
As we focus on the current playing song from each user,
we find that STAR schema with fact and dimension tables are
the most effective way for our data needs.

The current playing songs are considered our fact data,
while supporting data including info about our users, the artists who perform those songs,
the songs' details, and the time when each song is played are also important and
used as dimension tables.

Here is the detailed diagram of our fact and dimension tables and their relationship:
![schema.png](schema.png)

## Spark data pipeline Step-by-step instructions
From the workspace directory, submit a spark job using  `etl.py` script
```shell
$ spark-submit --master master_url etl.py
```
The `etl.py` script creates a spark session to load data from source s3 bucket, create tables and then load them into output s3 bucket


## About Sparkify the company
Sparkify is a company focusing on providing our music lovers with our large collection of songs
from all genres and countries around the world. Our streaming application, Sparkify,
is highly reviewed by our loyal customers with its easy-to-use and intelligent features. 
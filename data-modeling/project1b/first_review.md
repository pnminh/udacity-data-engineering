Requires Changes
----------------

### 5 specifications require changes

Keen Learner

Congratulations on making it this far. You have done a really great job. Your work so far shows that you have gotten a good grasp of the concepts in Data modeling with Cassandra. However, there are some changes that need to be made for this project to pass successfully. Please do well to make these adjustments and submit again. I have provided details in this review to help you correct these specifications.

Good Luck in your next submission

Extra Resources
---------------

Below are some additional links to help you deepen your understanding on the related concepts:

-   [Understanding Cassandra's data model and Cassandra Query Language (CQL)](https://www.oreilly.com/library/view/cassandra-the-definitive/9781491933657/ch04.html)
-   [Apache Cassandra: Compound Primary Key](https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCompoundPrimaryKeyConcept.html)
-   [Apache Cassandra Data Modeling and Query Best Practices](https://www.red-gate.com/simple-talk/sql/nosql-databases/apache-cassandra-data-modeling-and-query-best-practices/)
-   [Designing a Cassandra Data Model](https://shermandigital.com/blog/designing-a-cassandra-data-model/)
-   [Cassandra data modeling: The primary key](https://www.datastax.com/dev/blog/the-most-important-thing-to-know-in-cassandra-data-modeling-the-primary-key)
-   [CQL data types](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cql_data_types_c.html)

ETL Pipeline Processing
-----------------------

Student creates `event_data_new.csv` file.

-   Great Job!![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:") the `event_data_new.csv` file was created successfully
-   The file contains 6821 records as required.![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:")

[![Screenshot from 2021-04-07 20-47-33.png](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/177405/1617824883/Screenshot_from_2021-04-07_20-47-33.png)](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/177405/1617824883/Screenshot_from_2021-04-07_20-47-33.png)

Student uses the appropriate datatype within the `CREATE` statement.

Good job! You specified the correct data types like:

-   int for sessionId ![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:")
-   int for intemInSession ![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:")
-   text for artist ![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:")
-   text for song ![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:")
-   decimal for length ![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:")

Data Modeling
-------------

Student creates the correct Apache Cassandra tables for each of the three queries. The `CREATE TABLE` statement should include the appropriate table.

Great job!![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:")![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:")![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:") You followed the one table per query rule of Apache Cassandra. You are not replicating the same table for all three queries, which defies that rule. You have three distinct tables with unique table names and use appropriate CREATE table statements.

### Suggestion

You have used all the columns from th`event_datafile_new.csv` file. This will work just fine. However, To make our queries run more efficiently, it will be wise to use just the necessary columns. For e.g. Using query 1\
the requirement says:

> Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

The columns `sessionId`, `itemInSession`, `artist`, `song`, `length` will answer this question perfectly and will run faster than using all the columns

Student demonstrates good understanding of data modeling by generating correct SELECT statements to generate the result being asked for in the question.

The SELECT statement should NOT use `ALLOW FILTERING` to generate the results.

Good job so far. However, your SELECT statements use `SELECT *` which should be avoided. Make sure the SELECT statements request only the data required.

This is very important because Cassandra usually contains lots of data and also receives many concurrent INSERTs. And since we're talking about a lot of data, selecting all columns every time will put an unnecessary load in the Cassandra cluster. For e.g Query 3 requires just the first name and the lastname

```
query = """
SELECT firstName, lastName
FROM music_history_song_partition
WHERE song='All Hands Against His Own'
"""

```

Student should use table names that reflect the query and the result it will generate. Table names should include alphanumeric characters and underscores, and table names must start with a letter.

Splendid! You have used table names that provide a good general sense of what each query generates. This is important because it describes the data model. ![:star:](https://review.udacity.com/assets/images/emojis/star.png ":star:")

```
music_history_song_partition
music_history_userid_partition
music_history_sessionid_partition

```

![:clap:](https://review.udacity.com/assets/images/emojis/clap.png ":clap:")

The sequence in which columns appear should reflect how the data is partitioned and the order of the data within the partitions.

The sequence of the columns in the CREATE and INSERT statements should follow the order of the COMPOSITE PRIMARY KEY and CLUSTERING columns. The data should be inserted and retrieved in the same order as to how the COMPOSITE PRIMARY KEY is set up.

For example, if you have composite Primary key: PRIMARY KEY (a,c), then your CREATE and INSERT statements should have these columns first in sequence followed by the remaining columns:

-   This is important Apache Cassandra is a partition row store, which means the partition key determines which node a particular row is stored on. With the Primary key (which includes the Partition Key and any clustering columns), the partitions are distributed across the nodes of the cluster
-   It determines how data are chunked for write purposes. Any clustering column(s) would determine the order in which the data is sorted within the partition.

For e.g Using query 1:\
Your `INSERT` statement should start with your PRIMARY KEY/COMPOSITE PRIMARY key followed by the CLUSTERING columns

```
query = """
CREATE TABLE IF NOT EXISTS music_history_sessionid_partition
(
     sessionId int,
    itemInSession int,
    artist text,
    firstName text,
    gender text,
    lastName text,
    length decimal,
    level text,
    location text,
    song text,
    userId int,
    PRIMARY KEY(sessionId,itemInSession)
)
"""

```

The INSERT statement should have the columns in the same order as the CREATE statement

```
  query = "INSERT INTO music_history_sessionid_partition\
        (sessionId, itemInSession, artist, firstName, gender, , lastName,\
        length, level, location, song, userId)"

```

Please do same for the other queries (beginning with the composite key followed by clustering columns for CREATE and INSERT statements)

### Extra Resource

You can check [this link](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html) to know more about this rule

PRIMARY KEYS
------------

The combination of the PARTITION KEY alone or with the addition of CLUSTERING COLUMNS should be used appropriately to uniquely identify each row.

Good Job including clustering columns as part of the COMPOSITE PRIMARY KEY.\
![:white_check_mark:](https://review.udacity.com/assets/images/emojis/white_check_mark.png ":white_check_mark:") Query 1: PRIMARY KEY (sessionId, itemInSession)\
![:x:](https://review.udacity.com/assets/images/emojis/x.png ":x:") Query 2: PRIMARY KEY(userid,sessionId,itemInSession)\
![:x:](https://review.udacity.com/assets/images/emojis/x.png ":x:") Query 3:PRIMARY KEY (song)

### Query 2

For query 2, the specification says:

> Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

To get the values in sorted by itemInSession, we need to make use of clustering columns described in [Lesson-4.17](https://classroom.udacity.com/nanodegrees/nd027-beta/parts/10213a6f-6322-4c3e-b9a6-02b520daebc8/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/73fd6e35-3319-4520-94b5-9651437235d7/concepts/347092ad-2042-4385-90e5-b258f41941f4). You may use [Composite Partition Key](https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCompositePartitionKeyConcept.html#useCompositePartitionKeyConcept) here and assign`itemInSession` as your clustering key to sort the data as required.

Here is an example format of a composite partition key:

```
PRIMARY KEY ((a, b), c): The composite partition key is (a, b), the clustering key is c.

```

So the primary key will be

```
PRIMARY KEY((userid,sessionId),itemInSession)

```

### Query 3

Currently, you have only the song name as your primary key. Which will restrict the table to store only one record for a song listened by multiple users with the same name. This might become very critical when there is a situation when we have users A, B, C as names for a thousand of users. Our table will store only one record. The number of listeners which can be used to find the popularity of a song will need each every single record per user. So consider using user_id as another field and make it a part of COMPOSITE PRIMARY KEYs as well.

```
PRIMARY KEY(song, userId)

```

Presentation
------------

The notebooks should include a description of the query the data is modeled after.

Great work so far. Adding a description for the queries is important as you will be presenting your notebook to others. This helps make it easy to follow your work\
Please include a description of the query the data is modeled after. You may indicate your PARTITION and CLUSTERING columns in each query, and explain how did you based the query and its keys on the table you have created.

Here is an example description for Query :

```
Query Description: In this query, I used `column1` as the partition key and `column2` as my clustering key. Each partition is uniquely identified by `column1` while `column2` was used to uniquely identify the rows within a partition to sort the data by the value of num.

```

Code should be organized well into the different queries. Any in-line comments that were clearly part of the project instructions should be removed so the notebook provides a professional look.

Consider you are presenting this project as this notebook to an employer. I believe you wouldn't like to have all the questions and instructions to be in there. You would better separate them as tasks and mention what you have done and why you have done.\
Please remove all to do instructions in the notebook.

# Backgroud
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
# Objective
Build ETL pipeline to generates tables for analytics in data warehouse: 
# Database Schema
![alt text](https://github.com/limengunique/Postgres-ETL/blob/master/Untitled%20drawing.png?raw=true)
Since the company's user base is growing fast, use key distribution on fact table songplays and dimension table users could speed up queries related to which users at what time under what circumstances listened to which song, so that the team could understand the users behavior better and thus, make optimize product service and monetization strategy.
# ETL Pipelines
* Launch Redshift cluster using boto3 python SDK
* Extract data from S3
* Copy data from S3 to Redshift
* Transform data to the schema above


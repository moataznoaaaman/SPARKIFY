<h1>Sparkify Database</h1>

<h3>Business purpose<h3>

<p>
This database is designed to aid the analytics team in the Sparkify startup to analyze theeir inventory of songs, users and subscriptions to help them better understand their traffic and better focus their efforts on better user statsfaction and profit. this database will enable them to perform ad-hoc functions, categorize their songs and users according to diiferent metrics, understand the rise and drop in the number of useres during the day/month/year, and summerize their overall performance
<p>

<h3>Files in the repository<h3>

<p>
1- data: contains all the song data and the logs data
2- sql_queries.py: contains all the sql syntax used to create the database, create the tables, insert the data to all the tables, and to drop the database and the tables
3- create_tables.py: executes the drop and create sql queries written in the sql_queries.py script to set up the DP
4- etl.ipynb: contains the code used to develope the etl.py script
5- etl.py: contains the code that processes all the data in the data folder to fill all the tables
6- test.ipynb: used to verify the tables names, data types and constraints
<p>


<h3>How to run the project<h3>

<p>
1- run the sql_queries.py to create the syntax necessary to perform the sql queries
2- then run etl.py  which will  create_tables.py to create the database and the tables necessary for storing the data then process the data and to fill the database 
<p>

<h3>Why the star schema and the etl<h3>

<p>
1- to allow for flexiplity in performing all sort of analytics
2- a simple way to join the different data in the different tables
3- to allow for fast aggregations and clustering of data
<p>

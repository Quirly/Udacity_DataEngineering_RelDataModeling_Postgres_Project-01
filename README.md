# Udacity Data Engineering

# Project 1: Data Modeling with Postgres

## (1) Scope

Sparkify, a tech startup company, is hosting a new music streaming app. Management collects data on songs and user activity. The data is stored in JSON files which is not
easy to query.

There is JSON metadata and JSON logs on user activity. 
A data engineer is hired to create a Postgres database with a suitable schema to allow easy querying. Furthermore, using
Python, ETL pipelines shall be built to read the data from
files, load it and store it in the Postgres database "sparkifydb". This database then can be easily queried using
SQL within Python package psycopg2.

## (2) How to Run Notebooks and Python Scripts

Please donwload Anaconda and create an environment for this project. 

- **2.1** Please create an environment using the *.yml file. You can do this by

  *conda env create --file environment_DataEngPostgres.yml*

- **2.2** Please activate this environment

  *conda activate DataEngPostgres*

- To run *.ipynb files type in *"jupyter notebook"* when the environment is activated

- In your Jupyter Notebook please ensure that "DataEngPosgres" is selected as kernel

To run the pipeline and have the database created please 

- **2.3** create a folder where you store all data and files

- **2.4** use bash to navigate to this folder in a shell/Terminal

- **2.5** type in

  *python create_tables.py*

Then the database will be created (empty tables, just the schema).

- **2.6** type in 

  *python etl.py*

Then the files will be read and using the pipeline, data will now be automatically
transferred from the files to the database.

## (3) Explanation of Files in the Repository

**3.1**

**FOLDER data**

Log files and metadata is stored.

**3.2** 

**FOLDER data_quality**

In ETL pipeline processing dataframes are generated from the metadata and log files before
the entities in this dataframe are inserted in the respective tables in Postgres. To be able
to check these dataframes before having been inserted, a folder *"data_quality"* exists. The
dataframes are stored as csv for trouble shooting options and lookup.

**3.3**

**FILE create_tables.py**

Drops all tables from *sparkifydb* database and creates new, empty tables.

**3.4**

**FILE environment_DataEngPostgres.yml**

YAML file for creating a suitable environment for this project.

**3.5**

**FILE etl.ipynb**

Jupyter Notebook for developing the pipelines. Not needed for execution.

**3.6**

**FILE etl_functions.py**

Python library file with all functions that are used in pipeline processing in file *etl.py*

**3.7**

**FILE etl.py**

Pipeline execution file. Uses *etl_functions.py* as functions library.

**3.8**

**FILE README.md**

Documentation.

**3.9**

**FILE sql_queries.py**

SQL Code file. Creates schemas in Postgres, contains SQL code. DDL, DML PostgreSQL codes for creating tables, inserting data and querying is stored here in a central place.

**3.10** 

**FILE test.ipynb**

Jupyter Notebook for testing table creation, insertion and querying.

**3.11** 

**FILE Udacity DE P1 ERD.png**

Entity Relationship Diagram.

## (4) Database Design

Please refer to file *"Udacity DE P1 ERD.png"* to get an overview about the database schema.

Notes:

**4.1**
Constraint UNIQUE is used in table *USERS* as user information is extracted from log files. Thus, one user can be in there multiple times. If this is not considered, duplicates will be inserted and one user will be stored redundantly which would make no sense regarding data integrity. In table songs, where artist and song data is extracted from, we do not face the same issue. Thus, UNIQUE as a constraint does not need to be included.

**4.2**
Every table has its own, data independent primary key to ensure data integrity.

**4.3**
In songplays table only data is inserted for which the artist and song data are available in dimension tables. Logs with unknown/missing artist or song data is ignored. This is ensured by using a JOIN in the SQL query statement which is executed. Only the results found here are then inserted. 

**4.4**
Duration in table SONGS is stored as TEXT as it is used as parameter in string format in the query statement described in 4.3.
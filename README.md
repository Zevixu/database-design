# ECE656

# Preparing environment

## Database

The databases enviroment of this project is MySql version 8.0.27.

```console
mysql --version
```

## Python environment

Python3 (version 3.8) is used for the client application.

```console
python --version
```


## Basic Use

### Server:

1. Folder ./csv contains csv file for data loading
2. ece656_project_datacleaning.ipynb file for data clean up
3. data_import.py for loading the data from local
4. mysql_connector.py for database connection
5. db_config.ini for database configurations
6. ProjectSQL.sql for creating tables in the database
7. ProjectSQL(loaddata).sql for creating tables and loading data

### Client:

After setting up the server side, to run the client application:

```console
python3 ece656_main.py
```

to see the help information.

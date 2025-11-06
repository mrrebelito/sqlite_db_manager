
# Sqlite Database Manager

A Sqlite Database Manager Python script. Created to fulfill the need of converting JSON file(s) to sqlite database with one or more tables. 

Currently includes methods:

- adding a datetime field if a JSON field does not initially include such datetime field, date created or updated.
- insert data into existing table. 
- drop table from database

## How to run 

With `sqlite_db_manager.db`, simply instatiatite database running the following: 

`db = DB('example.db')`

Once the db intatiatited, call methods of your choosing.


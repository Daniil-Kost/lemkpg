**lemkpg - CRUD API for quick and simple connection with PostgreSQL DB**

**Usage**

- import LemkPgApi class from lemkpg module: `>>> from lemkpg import LemkPgApi`
- define with credentials of selected Postgres DB: 
` >>> obj = LemkPgApi(db_name="demo_db", db_password="pass", db_user="postgres", db_host="127.0.0.1")`

After object creation - you can call methods from LemkPgApi and executing queries with it 

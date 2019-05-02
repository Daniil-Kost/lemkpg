**lemkpg - CRUD API for quick and simple connection with PostgreSQL DB via sync or async way**

**Installation**

To install this package run command: `$ pip install lemkpg`

**Usage**

- For sync way: import LemkPgApi class from lemkpg module: `>>> from lemkpg import LemkPgApi`
- For async way: import AsyncLemkPgApi class from lemkpg module: `>>> from lemkpg import AsyncLemkPgApi`
- define db connection object with credentials of selected Postgres DB: 
    
    For sync way:
` >>> db_conn = LemkPgApi(db_name="demo_db", db_password="pass", db_user="postgres", db_host="127.0.0.1")`

    For async way:
` >>> db_conn = AsyncLemkPgApi(db_name="demo_db", db_password="pass", db_user="postgres", db_host="127.0.0.1") `

After object creation - you can call all methods from LemkPgApi or AsyncLemkPgApi and execute queries with it 

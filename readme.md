# Lib Sql Retriever

One library to connect to MongoDB server and create an instance with python.


## Requirements

See requirements.txt


## Development setup

This script uses a file named mongo.ini. This file should look like this:

```[MONGO_CONNECTION]
HOST = host_name
PORT = port

[MONGO_AUTHENTIFICATION]
USER = user_name
PWD = one_secret_password
AUTH_SOURCE = auth_source
```


## Script description

mongo.py contains an MongoDB class that connect to mongoDB.\
More methods to come...
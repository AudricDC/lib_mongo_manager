# Lib Mongo Manager

One library to connect to MongoDB server and create an instance with python.


## Requirements

See requirements.txt


## Development setup

This script uses a file named mongo.ini. This file should look like this:

```
[MONGO_AUTHENTICATION]
USER = user_name
PWD = one_secret_password
AUTH_SOURCE = auth_source
; AUTH_SOURCE is only used for a local connection.
```


## Script description

mongo.py contains an MongoDB class that connect to mongoDB.

Connection can be local i.e the standard one using a MongoDB installation, or remote in a cloud using atlas.\
To get started and for more information about atlas, just follow the steps in the following link : https://docs.atlas.mongodb.com/getting-started/\
Connection type is specified in the MongoDB constructor (see connection attribute).

MongoDB object contains methods like : query, getSortedRecords, getDf, update, insert and dfToJsonList.
getDf allows to get a pandas data frame from a mongo database, and dfToJsonList does the exact opposite action 
by transforming a data frame into a list of json documents. Both function use schema arguments. 
See the example at the end of mongo.py scripts to understand how those schemas work.

See function description for more information about each method.

## Data set

Data set used for the example in mongo.py is one of the dataset provided by atlas,
named "Sample Restaurants Dataset". 

To Load Sample Data into Your Atlas Cluster see : https://docs.atlas.mongodb.com/sample-data/
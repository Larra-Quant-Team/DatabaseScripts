import os

credentials = {
    "user": os.environ.get('MONGO_USER'),# "dbUser"
    "password": os.environ.get('MONGO_PASSWORD'), # "Larra.2020"
    "dbname": os.environ.get('MONGO_DB'),
    "host": os.environ.get('MONGO_HOST'), # "amalgama-fbqlw.mongodb.net"
    "args": "test?retryWrites=true&w=majority",
    "port": int(os.environ.get('MONGO_PORT'))
}

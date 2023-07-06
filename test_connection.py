import mysql.connector
import json

with open('db/db_config.json', 'r') as f:
        db_config = json.load(f)

try:
    db = mysql.connector.connect(
        host = db_config['host'],
        user= db_config['user'],
        password = db_config['password'],
        database = db_config['db_name']
    )

    cursor = db.cursor()
    cursor.execute('show databases')

    for i in cursor:
            print(i)
    print('Connection successful')

    cursor.close()
    db.close()
except:
    print('Cannot connect to database')
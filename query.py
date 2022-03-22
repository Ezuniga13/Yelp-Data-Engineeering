import psycopg2 as ps
import requests
import pandas as pd

def connect_to_db(host, dbname, username, password, port):
    try:
        conn =  ps.connect(host = host, database = dbname,  user = username,  password = password, port = port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    return conn


def query_top_rated(curr):
    query = (""" SELECT * FROM yelp_business WHERE rating > %s and review_count > %s ORDER BY review_count DESC""")
    curr.execute(query, (4.5, 30))
    print(curr.rowcount)
    results = curr.fetchall()
    for idx, row in enumerate(results):
        print(idx, row)
    
def main():
   
    host = 'database-yp.co4mvaosgcjm.us-east-1.rds.amazonaws.com'
    dbname = 'yelpdb'
    port = '5432'
    username = 'dtengineer'
    password = '2EsxlYUZvyCGgV7rmjjU'
    conn = None

    conn = connect_to_db(host, dbname, username, password, port)
    curr = conn.cursor()
    query_top_rated(curr)
    conn.commit()
    curr.close
    conn.close
    



if __name__ == '__main__':
    main()

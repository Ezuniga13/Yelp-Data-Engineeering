import psycopg2 as ps
import requests
import pandas as pd
from dotenv import load_dotenv 
from dotenv import find_dotenv
import os
import boto3

def connect_to_db(host, dbname, username, password, port):
    """
        Parameters: Take in standard credentials necessary to connect to a aws rds using a psycopg2 client.
        Returns: a conn and prints out Connected! or exceptiong error if unable to connect
    """
    try:
        conn =  ps.connect(host = host, database = dbname,  user = username,  password = password, port = port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    return conn

def query_top_rated(curr):
    """
        Parameters: Takes in a cursor to query the database.
        Returns: A sql object of filtered values.
    """
    query = (""" SELECT * FROM yelp_business WHERE rating >= %s and review_count > %s ORDER BY review_count DESC""")
    
    curr.execute(query, (4.5, 30))
    print(curr.rowcount)
    results = curr.fetchall()
    
    return results

def make_dataframe(queryresults):
    """
        Parameters: Takes in the queryresults from the query_to_rated functions.
        Returns: a dataframe then converts it into a cvs.
    """
    yelp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone', 'latitude', 'longitude'])
    for row in queryresults:
        row = pd.DataFrame([[row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone', 'latitude', 'longitude'])
        yelp_df = pd.concat([yelp_df, row], ignore_index=True)
    
    print(yelp_df)
    yelp_csv = yelp_df.to_csv()
    return yelp_csv

def push_csv_to_bucket(csv, ACCESS_ID, ACCESS_KEY):
    """
        Parameters: Takes in a csv file and credentials to interact with the S3 bucket on AWS.
        Returns a 200 response and object type that was pushed to the s3 bucket.
    """
    s3_client = boto3.client('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)

    response =  s3_client.put_object(Body = csv, Bucket = 'yelp-data-engineering', Key = 'query_of_nyc_best')
    print(response)

def main():
    load_dotenv(dotenv_path=find_dotenv(), verbose=True)
    HOST = os.getenv('HOST') 
    DBNAME = os.getenv('DBNAME') 
    PORT = os.getenv('PORT') 
    USERNAME = os.getenv('USERNAME') 
    PASSWORD = os.getenv('PASSWORD') 
    conn = None
    ACCESS_ID = os.getenv('ACCESS_ID')
    ACCESS_KEY = os.getenv('ACCESS_KEY')

    conn = connect_to_db(HOST, DBNAME, USERNAME, PASSWORD, PORT)
    curr = conn.cursor()
    queryresults = query_top_rated(curr)
    conn.commit()
    curr.close()
    conn.close()
    yelp_csv = make_dataframe(queryresults)
    push_csv_to_bucket(yelp_csv, ACCESS_ID, ACCESS_KEY)
    
if __name__ == '__main__':
    main()

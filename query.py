import psycopg2 as ps
import requests
import pandas as pd
from main import connect_to_db



def query_top_rated(curr):
    query = (""" SELECT * FROM yelp_business WHERE rating > %s and review_count > %s ORDER BY review_count DESC""")
    curr.execute(query, (4.5, 30))
    print(curr.rowcount)
    results = curr.fetchall()
    return results


def make_dataframe(queryresults):
    yelp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone'])
    for row in queryresults:
        row = pd.DataFrame([[row[0], row[1], row[2], row[3], row[4], row[5], row[6]]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone'])

        yelp_df = pd.concat([yelp_df, row], ignore_index=True)
    
    print(yelp_df)



def main():
    
    host = 'database-yp.co4mvaosgcjm.us-east-1.rds.amazonaws.com'
    dbname = 'yelpdb'
    port = '5432'
    username = 'dtengineer'
    password = '2EsxlYUZvyCGgV7rmjjU'
    conn = None

    conn = connect_to_db(host, dbname, username, password, port)
    curr = conn.cursor()
    queryresults = query_top_rated(curr)
    conn.commit()
    curr.close
    conn.close
    make_dataframe(queryresults)
    



if __name__ == '__main__':
    main()

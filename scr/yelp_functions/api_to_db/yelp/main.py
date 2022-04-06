import psycopg2 as ps
import requests
import pandas as pd
from dotenv import load_dotenv 
from dotenv import find_dotenv
import os

load_dotenv(dotenv_path=find_dotenv(), verbose=True)
API_KEY = os.getenv('API_KEY')
URL = os.getenv('URL')
headers = {'Authorization':'Bearer {}'.format(API_KEY)}

def make_dataframe(response, dataframe):
    """ 
        Args: Takes in a Json response from an Yelp API call and an empty Pandas Datarame from the call_yelp function.
        Returns: The dataframe with the data from the API call concated row by row
    """
    for biz in response['businesses']:
        alias = biz['alias']
        name = biz['name']
        title = biz['categories'][0]['title']
        review_count = biz['review_count']
        rating = biz['rating']
        location = biz['location']['display_address']
        phone = biz['phone']
        coordinates = biz['coordinates']       
        row = pd.DataFrame([[alias, name, title, review_count, rating, location, phone, coordinates]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone', 'coordinates'])
        dataframe = pd.concat([dataframe, row], ignore_index=True)
    return dataframe



def call_yelp():
    """
        Args: None
        Returns: A get reponse of 1000 restaurants from New York City then parses through the json objects and converts it into a pandas dataframe by calling the make-dataframe function
    
    """
    offset = 0
    count = 0
    yelp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone', 'coordinates'])
    while offset < 1000:
        parameters = {
            'location': 'New York City", "NYC"',
            'term': 'Restaurant',
            'radius': 10000,
            'limit': 50,
            'offset': offset}

        response = requests.get(URL, headers = headers , params = parameters)
        business_data = response.json()
        
        ## here I will put in pandas dataframe instead of printing it out. 
        yelp_df = make_dataframe(business_data, yelp_df)
        
        offset += 50
        count += 50
    
    print('done with the api call',  count, 'Restuarants Called')
    yelp_df = yelp_df.drop_duplicates('alias')
    print(yelp_df.count())
    print(yelp_df['alias'].nunique())
    print(yelp_df.head())
    return yelp_df


 

#connecting to dabase ---------------

def connect_to_db(host, dbname, username, password, port):
    """
        Args: Take in standard credentials necessary to connect to a aws rds using a psycopg2 client.
        Returns: a conn and prints out Connected! or exceptiong error if unable to connect
    
    """
    try:
        conn =  ps.connect(host = host, database = dbname,  user = username,  password = password, port = port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    return conn

# create table --takes in the cursor and needs the conn from connect_to_db
def create_table(curr):
    """
        Args: Takes in a cursor from the connection attribute for posgres
        Returns: A created table in a posgres db with the following fields
    
    """
    create_table_command = ("""CREATE TABLE IF NOT EXISTS yelp_business(
        alias VARCHAR(255) PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        review_count INTEGER NOT NULL,
        rating DECIMAL NOT NULL,
        location TEXT,
        phone TEXT,
        latitude DECIMAL,
        longitude DECIMAL
    )""")
    curr.execute(create_table_command)
    print('created_table')


# update or append values to our table sql

def alias_exist(curr, alias):
    query = (""" SELECT alias FROM yelp_business where alias = %s""")
    curr.execute(query, (alias,))
    return curr.fetchone() is not None


def update_row(curr, alias, name, type, review_count, rating, location, phone, latitude, longitude):
    query = (""" UPDATE yelp_business
                SET name = %s,
                    type = %s,
                    review_count = %s,
                    rating = %s,
                    location = %s,
                    phone = %s,
                    latitude = %s,
                    longitude = %s
                WHERE alias = %s;""")
    vars_to_update = (name, type, review_count, rating, location, phone, latitude, longitude, alias)
    curr.execute(query, vars_to_update)


def update_db(curr, yelp_df):
    temp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone', 'coordinates'])

    for i, row in yelp_df.iterrows():
        if alias_exist(curr, row['alias']): # alias is primary key ``
            update_row(curr, row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone'], row['coordinates']['latitude'], row['coordinates']['longitude'])
        else:
            row_df = pd.DataFrame([[row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone'], row['coordinates']]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone', 'coordinates'])
            temp_df = pd.concat([temp_df, row_df], ignore_index=True)
    print(temp_df.count())
    print(temp_df.head())
    return temp_df



def insert_into_table(curr, alias, name, type, review_count, rating, location, phone, latitude, longitude):

    insert_business_into = (""" INSERT INTO yelp_business(alias, name, type, review_count, rating, location, phone, latitude, longitude)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);""")
    row_to_insert = (alias, name, type, review_count, rating, location, phone, latitude, longitude)
    curr.execute(insert_business_into, row_to_insert)

    
def append_from_df_to_db(curr, dataframe):

    for i, row in dataframe.iterrows():
        insert_into_table(curr, row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone'], row['coordinates']['latitude'], row['coordinates']['longitude'])
    print('done')

def main():
    HOST = os.getenv('HOST') 
    DBNAME = os.getenv('DBNAME') 
    PORT = os.getenv('PORT') 
    USERNAME = os.getenv('USERNAME') 
    PASSWORD = os.getenv('PASSWORD') 
    conn = None
    
    yelp_df = call_yelp()
    conn = connect_to_db(HOST, DBNAME, USERNAME, PASSWORD, PORT)
    curr = conn.cursor()
    create_table(curr)
    new_yelp_df = update_db(curr, yelp_df)
    append_from_df_to_db(curr, new_yelp_df)
    conn.commit()




if __name__ == '__main__':
    main()


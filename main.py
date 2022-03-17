import psycopg2 as ps
import requests
import pandas as pd

url = 'https://api.yelp.com/v3/businesses/search'
key = open('/Users/estebanzuniga/Desktop/Yelp-Engineering/api-info/access.txt').readlines()[0]
headers = {'Authorization':'Bearer {}'.format(key)}

def make_dataframe(response, dataframe):
    for biz in response['businesses']:
        alias = biz['alias']
        name = biz['name']
        title = biz['categories'][0]['title']
        review_count = biz['review_count']
        rating = biz['rating']
        location = biz['location']['display_address']
        phone = biz['phone']
                    
        row = pd.DataFrame([[alias, name, title, review_count, rating, location, phone]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone'])
        dataframe = pd.concat([dataframe, row], ignore_index=True)
    return dataframe



def get_data():
    offset = 0
    count = 0
    yelp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone'])
    while offset < 500:
        parameters = {
            'location': 'New York City", "NYC"',
            'term': 'Restaurant',
            'radius': 10000,
            'limit': 50,
            'offset': offset}

        response = requests.get(url, headers = headers , params = parameters)
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


 

# connecting to dabase ---------------

def connect_to_db(host, dbname, username, password, port):
    try:
        conn =  ps.connect(host = host, database = dbname,  user = username,  password = password, port = port)
    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
    return conn

# create table --takes in the cursor and needs the conn from connect_to_db
def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS yelp_business(
        alias VARCHAR(255) PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        review_count INTEGER NOT NULL,
        rating NUMERICAL NOT NULL,
        location TEXT,
        phone TEXT

    )""")
    curr.execute(create_table_command)
    print('created_table')


# update or append values to our table sql

def alias_exist(curr, alias):
    query = (""" SELECT alias FROM yelp_business where alias = %s""")
    curr.execute(query, (alias,))
    return curr.fetchone() is not None


def update_row(curr, alias, name, type, review_count, rating, location, phone):
    query = (""" UPDATE yelp_business
                SET name = %s,
                    type = %s,
                    review_count = %s,
                    rating = %s,
                    location = %s,
                    phone = %s
                WHERE alias = %s;""")
    vars_to_update = (name, type, review_count, rating, location, phone, alias)
    curr.execute(query, vars_to_update)


def update_db(curr, yelp_df):
    temp_df = pd.DataFrame(columns=['alias', 'name', 'type', 'review_count', 'rating', 'location', 'phone'])

    for i, row in yelp_df.iterrows():
        if alias_exist(curr, row['alias']): # alias is primary key ``
            update_row(curr, row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone'])
        else:
            row_df = pd.DataFrame([[row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone']]], columns= ['alias','name','type','review_count', 'rating', 'location', 'phone'])
            temp_df = pd.concat([temp_df, row_df], ignore_index=True)
    print(temp_df.count())
    print(temp_df.head())
    return temp_df



def insert_into_table(curr, alias, name, type, review_count, rating, location, phone):

    insert_business_into = (""" INSERT INTO yelp_business(alias, name, type, review_count, rating, location, phone)
                            VALUES(%s, %s, %s, %s, %s, %s, %s);""")
    row_to_insert = (alias, name, type, review_count, rating, location, phone)
    curr.execute(insert_business_into, row_to_insert)

    
def append_from_df_to_db(curr, dataframe):

    for i, row in dataframe.iterrows():
        insert_into_table(curr, row['alias'], row['name'], row['type'], row['review_count'], row['rating'],
                        row['location'], row['phone'])






def main():
   
    host = 'database-yp.co4mvaosgcjm.us-east-1.rds.amazonaws.com'
    dbname = 'yelpdb'
    port = '5432'
    username = 'dtengineer'
    password = '2EsxlYUZvyCGgV7rmjjU'
    conn = None

    conn = connect_to_db(host, dbname, username, password, port)
    curr = conn.cursor()
    create_table(curr)
    yelp_df = get_data()
    new_yelp_df = update_db(curr, yelp_df)
    append_from_df_to_db(curr, new_yelp_df)
    conn.commit()



if __name__ == '__main__':
    main()


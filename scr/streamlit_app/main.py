import streamlit as st
import pandas as pd
import psycopg2 as ps
from main import connect_to_db



st.title(''' SIMPLY THE BEST EATS NYC ''')





def query_top_rated(curr):
    query = (""" SELECT * FROM yelp_business WHERE rating > %s and review_count > %s ORDER BY review_count DESC""")
    curr.execute(query, (4.5, 30))
    print(curr.rowcount)
    results = curr.fetchall()
    return results


def make_dataframe(queryresults):
    yelp_df = pd.DataFrame(columns=['name', 'type', 'review_count', 'rating', 'location', 'phone'])
    for row in queryresults:
        row = pd.DataFrame([[row[1], row[2], row[3], row[4], row[5], row[6]]], columns= ['name','type','review_count', 'rating', 'location', 'phone'])

        yelp_df = pd.concat([yelp_df, row], ignore_index=True)
    
    yelp_df.index  = yelp_df.index + 1
    print(yelp_df)
    return yelp_df



def main():
    
    df = make_dataframe(queryresults)
    st.dataframe(df.style.format(subset=['rating'], formatter='{:.1f}'))
    
        

if __name__ == '__main__':
    main()

    
import streamlit as st
import pandas as pd
from dotenv import load_dotenv 
from dotenv import find_dotenv
import os
import boto3
import io



st.title(''' SIMPLY THE BEST EATS NYC ''')


def get_csv_from_bucket(ACCESS_ID, ACCESS_KEY):
    s3_client = boto3.client('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)

    response = s3_client.get_object(Bucket = 'yelp-data-engineering', Key = 'query_of_nyc_best')
    csv = response['Body'].read()
    return csv

def make_dataframe(csv):
    yelp_df = pd.read_csv(io.BytesIO(csv))
    yelp_df.index  = yelp_df.index + 1
    yelp_df = yelp_df.loc[:, ~yelp_df.columns.str.contains('Unnamed')]
    return yelp_df

    

def main():
    load_dotenv(dotenv_path=find_dotenv(), verbose=True)
    ACCESS_ID = os.getenv('ACCESS_ID')
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    
    csv = get_csv_from_bucket(ACCESS_ID, ACCESS_KEY)
    df = make_dataframe(csv)
    
    st.dataframe(df.style.format(subset=['rating'], formatter='{:.1f}'))
    st.map(df)
    



if __name__ == '__main__':
    main()
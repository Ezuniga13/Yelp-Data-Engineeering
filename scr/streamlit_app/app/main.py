from urllib import request
import streamlit as st
import pandas as pd
from dotenv import load_dotenv 
from dotenv import find_dotenv
import os
import boto3
import io
import pydeck as pdk
import numpy as np

st.title(''' SIMPLY THE BEST EATS NYC ''')

@st.cache(persist=True)
def get_csv_from_bucket(ACCESS_ID, ACCESS_KEY):
    """
        Args: Takes in standard credentials to connect to AWS s3 bucket.
        Returns: The body of a response from s3 client get object attriubte.
    """
    s3_client = boto3.client('s3',
         aws_access_key_id=ACCESS_ID,
         aws_secret_access_key= ACCESS_KEY)

    response = s3_client.get_object(Bucket = 'yelp-data-engineering', Key = 'query_of_nyc_best')
    csv = response['Body'].read()
    return csv

def make_dataframe(csv):
    """
        Args: Takes in the body of a get object resonse from the get_csv_from_bucket function.
        Returns: A dataframe of restuarants from New York City.
    """
    yelp_df = pd.read_csv(io.BytesIO(csv))
    yelp_df.index  = yelp_df.index + 1
    yelp_df = yelp_df.loc[:, ~yelp_df.columns.str.contains('Unnamed')]
    yelp_df = yelp_df.loc[:, ~yelp_df.columns.str.contains('phone')]
    
    return yelp_df

load_dotenv(dotenv_path=find_dotenv(), verbose=True)
ACCESS_ID = os.getenv('ACCESS_ID')
ACCESS_KEY = os.getenv('ACCESS_KEY')
csv = get_csv_from_bucket(ACCESS_ID, ACCESS_KEY)
df = make_dataframe(csv)

st.subheader('Top Eats with a 4.5 or higher Yelp rating!') 
if st.checkbox("Click to view List", False): 
    st.dataframe(df.style.format(subset=['rating'], formatter='{:.1f}'))
    
st.header('3D Map')

if st.checkbox('Click to view map'):
    midpoint = [np.average(df['latitude']), np.average(df['longitude'])]

    st.pydeck_chart(pdk.Deck(
                map_style="mapbox://styles/mpbox/light-v10",
                initial_view_state= pdk.ViewState(
                                        latitude=midpoint[0],
                                        longitude=midpoint[1],
                                        zoom=11,
                                        pitch=50,
                ),
                
                layers=[
                    pdk.Layer(
                        'HexagonLayer',
                        data=df,
                        get_position='[longitude, latitude]',
                        radius=200,
                        elevation_scale=4,
                        elevation_range=[0, 1000],
                        pickable=True,
                        extruded=True,
                            )
                        ]
                    ))
    
def main():
    pass
    
if __name__ == '__main__':
    main()

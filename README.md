# Fully Automated Pipeline With Interative Dashboard For New York Foodies

Esteban Zuniga <br>
April 13, 2022 <br>
Data Engineering

## ABSTRACT

There are over 20,000 restaurants, bars and café's in New York City and some estimate it would take a person over 20 years to visit all of them. Of course, some of those would go out of business by the time one could visit them, making it virtually impossible to pull off the task. However, it would be nice if one knew what the best restaurants were according to their peers before trying eateries randonmly.

Foodies like to explore, but would rather not be dissapointed. Because they can only visit so many places it would be more strategic to know in advance the best spots. And the places to avoid! 

Can you imagine searching one by one from 20,000 places you know or do not know exist? This is a situation where the power of analytics can be demonstrated and be utilized by cutting out the manual searching and saving a massive amount of time. This can be accomplished by way of data engineering.

The goal of this project is to construct an ETL pipeline to curate a table and a 3D map of the best eateries in the city that have a rating of 4.5 or higher and at least 30 reviews. The list has all the relevant information of the establishment making it easy for the foodie to select their next culinary adventure. Bon Appétit!

## DATA
I collected data for this pipeline by way of making [Yelp](https://www.yelp.com/developers/documentation/v3/business_search) API calls from the Business Search endpoint to make daily updates of their ratings, phone, location, review counts and to display the type of food the place offers.

I am currently pulling 1000 restaurants a day and pushing the data to an [Amazon Web Services RDS](https://aws.amazon.com) but may not impact the size of the database because of the nature of the limitations of the restaurants that the city can hold. In other words, there are only so many restaurants, even in a city like New York. If the restaurant already exist only the values, such as rating and reviews are updated to reflect the most up-to-date standards.

The data is then queried from the database filtering the restaurants that have the highest ratings and at least 30 reviews to ensure a decent sample size. That query is converted into a CVS file and pushed to an Amazon s3 bucket. From the s3 bucket, you can use an Python web framework to visualize the data but I used Streamlit for this particular project. 

## ALGORITHM/TOOLS
*Libraries*

- requests to call the Yelp API
- psycopg2 to use as client to connect to the database and to make queries
- pandas for EDA and dataframe manipulation
- loadenv and dotenv for environment variables
- boto3 to push data to s3 Buckets

*Databases*

- AWS RDS posgres

*Cloud Processing*

- AWS s3 buckets

*Containers*

- Docker to run mutilple containers and to simplify deployment.

*Web Application*

- Streamlit




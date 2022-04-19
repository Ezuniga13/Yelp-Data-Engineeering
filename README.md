# Fully Automated Pipeline With Interative Dashboard For New York Foodies

Esteban Zuniga <br>
April 18, 2022 <br>
Data Engineering

## ABSTRACT

Ever order from a third party delivery service and needed to cross reference Yelp ratings before ordering or wanted to know what the best rated restuarants, cafes, or bars are in New York City but did not want to manually search for the rating? Then, this project was made for you. 

The goal of this project is to construct an ETL pipeline to provide a list and location of the eateries in the city that have a rating of 4.5 and higher and at least 30 reviews. 

## DATA
I collected data for this pipeline by way of making [Yelp](https://www.yelp.com/developers/documentation/v3/business_search) API calls from the Business Search endpoint.

I am currently pulling 1000 restaurants a day and pushing the data to an [Amazon Web Services RDS](https://aws.amazon.com/ target=_blank) but may not impact the size of the databse because of the nature of the limitations of the restaurants that the city can hold. In other words, there are only so many restaurants, even in a city like New York. If the restaurant already exist only the values, such as rating and reviews are updated to reflect the most up-to-date standards.

The data is then queried from the database filtering the restaurants that have the highest ratings and at least 30 reviews to ensure a decent sample size. That query is converted into a CVS file and pushed to an Amazon s3 bucket.




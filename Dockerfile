FROM python:3.8-buster
WORKDIR /yelp-engineering-app
COPY . .
ENV API_URL=https://api.yelp.com/v3/businesses/search
RUN pip3 install -r requirements.txt

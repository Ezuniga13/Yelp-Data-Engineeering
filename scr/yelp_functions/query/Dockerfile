FROM python:3.8-buster
WORKDIR /yelp-data-engineering
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
ENV API_URL=https://api.yelp.com/v3/businesses/search
RUN groupadd dev && useradd -r -g dev bxhack
USER bxhack
CMD ["python3", "query_to_s3/main.py"] 

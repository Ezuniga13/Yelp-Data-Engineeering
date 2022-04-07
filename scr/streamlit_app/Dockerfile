FROM python:3.8-buster
WORKDIR /yelp-data-engineering
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
ENV API_URL=https://api.yelp.com/v3/businesses/search
RUN groupadd dev && useradd -r -g dev bxhack
USER bxhack
CMD ["streamlit", "run", "app/main.py"] 

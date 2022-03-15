
import requests
import pandas as pd

url = 'https://api.yelp.com/v3/businesses/search'
key = open('/Users/estebanzuniga/Desktop/Yelp-Engineering/api-info/access.txt').readlines()[0]
headers = {'Authorization':'Bearer {}'.format(key)}

# make a que here to increment the offset parameter until you get 500 results per 500
def make_dataframe(response, dataframe):
    for biz in response['businesses']:
        name = biz['name']
        title = biz['categories'][0]['title']
        review_count = biz['review_count']
        rating = biz['rating']
        location = biz['location']['display_address']
        phone = biz['phone']
                    
        row = pd.DataFrame([[name, title, review_count, rating, location, phone]], columns= ['name','type','review_count', 'rating', 'location', 'phone'])
        dataframe = pd.concat([dataframe, row], ignore_index=True)
    return dataframe


def get_data():
    offset = 0
    count = 0
    yelp_df = pd.DataFrame(columns=['name', 'type', 'review_count', 'rating', 'location', 'phone'])
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
    print(yelp_df)

 

        



def main():
    get_data()

if __name__ == '__main__':
    main()


import requests

def fetch_api_data(api_url):

    response = requests.get(api_url)

    if response.status_code != 200:
        raise Exception("API call failed")

    return response.json()
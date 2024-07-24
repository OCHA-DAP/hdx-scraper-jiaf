import json
from urllib import request, error
from hdx.utilities.saver import save_json

def fetch_data(query_url, limit=1000):
    """
    Fetch data from the provided base_url with pagination support.

    Args:
    - base_url (str): The base URL endpoint to fetch data from.
    - limit (int): The number of records to fetch per request.

    Returns:
    - list: A list of fetched results.
    """

    idx = 0
    results = []

    while True:
        offset = idx * limit
        url = f"{query_url}&offset={offset}&limit={limit}"
        print(url)
        with request.urlopen(url) as response:
            print(f"Getting results {offset} to {offset+limit-1}")
            json_response = json.loads(response.read())

            results.extend(json_response['data'])

            # If the returned results are less than the limit,
            # it's the last page
            if len(json_response['data']) < limit:
                break

        idx += 1

    return results


def download_geojson(geojson_url):
    """
    Download GeoJSON data from the provided URL.

    Args:
    - geojson_url (str): The URL to download the GeoJSON data from.

    Returns:
    - dict: The GeoJSON data as a dictionary.
    """
    try:
        with request.urlopen(geojson_url) as response:
            return json.loads(response.read())
    except error.URLError as e:
        print(f"URL error: {e.reason}")
    except error.HTTPError as e:
        print(f"HTTP error occurred: {e.code} - {e.reason}")
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON data: {e.msg}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None

def save_geojson(geojson, filename):
    """
    Save the GeoJSON data to a file.

    Args:
    - geojson (dict): The GeoJSON data.
    - filename (str): The filename to save the data to.
    """
    try:
        save_json(geojson, filename)
    except Exception as e:
        print(f"Error: {e}")

def get_country_code(country_list):
    return [country['code'] for country in country_list]



APP_IDENTIFIER = "aGFwaS1kYXNoYm9hcmQ6ZXJpa2Eud2VpQHVuLm9yZw=="
BASE_URL = "https://hapi.humdata.org/api/v1/"
LIMIT = 1000

if __name__ == "__main__":
    country_query_url = f"{BASE_URL}metadata/location?output_format=json&app_identifier={APP_IDENTIFIER}"
    country_data = fetch_data(country_query_url, LIMIT)
    #country_list = get_country_code(country_data)
    #HAPI locations endpoint returns 249 countries which takes too long for the script to complete
    #just getting geojson for the subset of countries used in the dashboard
    country_list = ['AFG', 'BFA', 'CMR', 'CAF', 'TCD', 'COL', 'COD', 'SLV', 'ETH', 'GTM', 'HTI', 'HND', 'MLI', 'MOZ', 'MMR', 'NER', 'NGA', 'PSE', 'SOM', 'SSD', 'SDN', 'SYR', 'UKR', 'VEN', 'YEM']

    for code in country_list:
        print(f"Getting data for {code}")
        geojson_url = f"https://apps.itos.uga.edu/codv2api/api/v1/themes/cod-ab/locations/{code}/versions/current/geoJSON/1"
        geojson_data = download_geojson(geojson_url)

        if geojson_data:
            save_geojson(geojson_data, f"geojson/itos-{code}.geojson")
            print(f"GeoJSON data saved for country code {code}")
        else:
            print(f"Failed to download GeoJSON data for country code {code}.")



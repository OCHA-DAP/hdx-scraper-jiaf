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
            print(f"Downloading GeoJSON data from {geojson_url}")
            return json.loads(response.read())
    except error.URLError as e:
        print(f"Failed to download data from {geojson_url}. URL error: {e.reason}")
    return None

def save_geojson(geojson, filename):
    """
    Save the GeoJSON data to a file.

    Args:
    - geojson (dict): The GeoJSON data.
    - filename (str): The filename to save the data to.
    """
    # with open(filename, 'w') as file:
    #     json.dump(geojson, file)
    #     print(f"GeoJSON saved to {filename}")
    try:
        save_json(geojson, filename)
        print(f"GeoJSON saved to {filename}")
    except Exception as e:
        print(f"Failed to save GeoJSON data to {filename}. Error: {e}")

def extract_country_code(country_list):
    return [country['code') for country in country_list]



APP_IDENTIFIER = "aGFwaS1kYXNoYm9hcmQ6ZXJpa2Eud2VpQHVuLm9yZw=="
BASE_URL = "https://hapi.humdata.org/api/v1/"
LIMIT = 1000

if __name__ == "__main__":
    country_query_url = f"{BASE_URL}metadata/location?output_format=json&app_identifier={APP_IDENTIFIER}"
    country_data = fetch_data(country_query_url, LIMIT)
    country_list = extract_country_code(country_data)

    for code in country_list:
        geojson_url = f"https://apps.itos.uga.edu/codv2api/api/v1/themes/cod-ab/locations/{code}/versions/current/geoJSON/1"
        geojson_data = download_geojson(geojson_url)

        if geojson_data:
            save_geojson(geojson_data, f"itos-{code}.geojson")
        else:
            print(f"Failed to download GeoJSON data for country code {code}.")



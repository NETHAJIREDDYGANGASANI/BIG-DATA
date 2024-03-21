import requests
import redis
import json
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
import os
# Call load_dotenv to load environment variables from .env file
load_dotenv()

class ImportData:
    """
    A class for importing, storing, and retrieving data from APIs and Redis.

    Methods:
    - load_api_data(url): Fetches data from the specified API URL.
    - load_data_to_redis(data, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None): 
      Stores the provided data in Redis.
    - read_data_from_redis(redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None): 
      Retrieves data from Redis.
    """

    def load_api_data(self, url):
        """
        Fetches data from the specified API URL.

        Parameters:
        - url (str): The URL of the API.

        Returns:
        - dict or None: The fetched data in JSON format or None if there is an error.
        """
        try:
            response = requests.get(url)
            data = response.json() 
            return data
        except Exception as e:
            print("Error fetching data:", e)
            return None

    def load_data_to_redis(self, data, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        """
        Stores the provided data in Redis.

        Parameters:
        - data (dict): The data to be stored in Redis.
        - redis_host (str): The Redis server host.
        - redis_port (int): The Redis server port.
        - redis_username (str): The Redis username (if applicable).
        - redis_password (str): The Redis password.
        - redis_db (str): The Redis database name.
        """
        try:
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            json_data = json.dumps(data)
            r.set('data_key', json_data)
            print("Data loaded into Redis successfully.")
        except Exception as e:
            print("Error loading data into Redis:", e)

    def read_data_from_redis(self, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        """
        Retrieves data from Redis.

        Parameters:
        - redis_host (str): The Redis server host.
        - redis_port (int): The Redis server port.
        - redis_username (str): The Redis username (if applicable).
        - redis_password (str): The Redis password.
        - redis_db (str): The Redis database name.

        Returns:
        - dict or None: The retrieved data from Redis in JSON format or None if there is an error.
        """
        try:
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            json_data = r.get('data_key')
            
            if json_data:
                data = json.loads(json_data)
                return data
            else:
                print("No data found in Redis.")
                return None
        except Exception as e:
            print("Error reading data from Redis:", e)
            return None

class Analytics:
    """
    A class for performing analytics on data stored in a DataFrame.

    Methods:
    - __init__(self, data): Initializes an instance of the class with data.
    - plot_population_growth(self, country_name): Plots population growth for a specified country.
    - search_country_by_name(self, name): Searches for a country by name.
    - aggregate_country_data(self): Computes and returns aggregated data.

    Attributes:
    - data (dict): The input data.
    - df (pd.DataFrame): The DataFrame created from the input data.
    """

    def __init__(self, data):
        """
        Initializes an instance of the class with data.

        Parameters:
        - data (dict): The input data.
        """
        self.data = data
        self.df = pd.DataFrame(data)

    def plot_population_growth(self, country_name):
        """
        Plots population growth for a specified country.

        Parameters:
        - country_name (str): The name of the country.

        Returns:
        - None
        """
        country_data = self.df[self.df["country"].str.lower() == country_name.lower()]

        if country_data.empty:
            print(f"Country '{country_name}' not found.")
            return

        years = ["1980", "2000", "2010", "2022", "2023", "2030", "2050"]
        population = [country_data[f"pop{year}"].values[0] for year in years]

        plt.plot(years, population, marker='o')
        plt.title(f"Population Growth Over the Years - {country_data['country'].values[0]}")
        plt.xlabel("Year")
        plt.ylabel("Population")
        plt.grid(True)
        plt.show()

    def search_country_by_name(self, name):
        """
        Searches for a country by name.

        Parameters:
        - name (str): The name of the country.

        Returns:
        - pd.DataFrame: The DataFrame containing the search result.
        """
        return self.df[self.df["country"].str.lower() == name.lower()]

    def aggregate_country_data(self, density_unit="people_per_km2"):
        """
        Computes and returns aggregated data for population density.

        Parameters:
        - density_unit (str): The unit in which population density should be expressed.
          Possible values: "people_per_km2" (default), "people_per_mi2".

        Returns:
        - dict: The aggregated data, including min, max, and average population density.
        """
        density_column = "density" if density_unit == "people_per_km2" else "densityMi"

        aggregated_data = {
            "min_density": self.df[density_column].min(),
            "max_density": self.df[density_column].max(),
            "avg_density": self.df[density_column].mean(),
            "density_unit": "people per square kilometer" if density_unit == "people_per_km2" else "people per square mile",
        }
        return aggregated_data

if __name__ == "__main__":
    dl = ImportData()
    url = "https://apis-ugha.onrender.com/countries"
    data = dl.load_api_data(url)
    
    print("Extracted data successfully")
    
    redis_host = 'redis-16403.c326.us-east-1-3.ec2.cloud.redislabs.com'
    redis_port = 16403
    redis_password = os.getenv('REDIS_PASSWORD')
    redis_db = 'Bigdata'
    username = 'default'
    dl.load_data_to_redis(data, redis_host=redis_host, redis_port=redis_port, redis_username=username, redis_password=redis_password, redis_db=redis_db)

    redis_data = dl.read_data_from_redis(redis_host=redis_host, redis_port=redis_port, redis_username=username, redis_password=redis_password, redis_db=redis_db)

    if redis_data:
        analytics = Analytics(redis_data)

        analytics.plot_population_growth("India")

        search_result = analytics.search_country_by_name("China")
        if not search_result.empty:
            print("Search Result:")
            print(search_result)

        aggregated_data = analytics.aggregate_country_data()
        print("\nAggregated Data:")
        print(aggregated_data)
    else:
        print("Failed to read data from Redis.")
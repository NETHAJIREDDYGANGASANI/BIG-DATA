import requests
import redis
import json
import matplotlib.pyplot as plt
import pandas as pd



class importData:
    
    def loadApiData(self, url):
        try:
            response = requests.get(url)
            data = response.json() 
            return data
        except Exception as e:
            print("Error fetching data:", e)
            return None
        
    def load_data_to_redis(self,data, redis_host=None, redis_port=None,redis_username = None,redis_password = None, redis_db=None):
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port,
                            username=redis_username,password=redis_password,)
            

            # Convert the data to JSON string
            json_data = json.dumps(data)
            
            # Store the JSON data in Redis
            r.set('data_key', json_data)
            
            print("Data loaded into Redis successfully.")
        except Exception as e:
            print("Error loading data into Redis:", e)
            

    
    def read_data_from_redis(self, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            
            # Retrieve data from Redis
            json_data = r.get('data_key')
            
            # Decode JSON data
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
    def __init__(self, data):
        self.data = data
        self.df = pd.DataFrame(data)


    # Function for plotting a graph
    def plot_population_growth(self,country_name):
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

    # Function for searching a country
    def search_country_by_name(self,name):
        return self.df[self.df["country"].str.lower() == name.lower()]

    # Function for aggregation (min, max, average)
    def aggregate_country_data(self):
        aggregated_data = {
            "min_density": self.df["density"].min(),
            "max_density": self.df["density"].max(),
            "avg_density": self.df["density"].mean(),
            # Add more aggregations as needed
        }
        return aggregated_data




if __name__ == "__main__":
    dl = importData()
    url = "https://apis-ugha.onrender.com/countries"
    data = dl.loadApiData(url)
    
    print("extracted data successfully")
    # Load data into Redis
    redis_host = 'redis-16403.c326.us-east-1-3.ec2.cloud.redislabs.com'
    redis_port = 16403  # Your Redis Cloud port
    redis_password = 'Bigdata007'  # Your Redis Cloud password
    redis_db = 'Bigdata'
    username = 'default'
    dl.load_data_to_redis(data,redis_host=redis_host,
                          redis_port=redis_port,
                          redis_username=username,
                          redis_password=redis_password,
                          redis_db=redis_db)
    

    #print(data)


    # Read data from Redis
    redis_data = dl.read_data_from_redis(redis_host=redis_host,
                                         redis_port=redis_port,
                                         redis_username=username,
                                         redis_password=redis_password,
                                         redis_db=redis_db)

    if redis_data:
        #print("Data read from Redis:", redis_data)
        # Initialize Analytics object with the retrieved data
        analytics = Analytics(redis_data)

        # Example usage
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

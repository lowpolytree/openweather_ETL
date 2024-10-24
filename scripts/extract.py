import os
import asyncio
import aiohttp
from datetime import datetime
from config.settings import CITIES
import json

async def fetch_weather(session, city):
    api_key = os.getenv('API_KEY')
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    #print(url)
    try:
        async with session.get(url, timeout=500) as response:
            if response.status == 429:
                print("Rate limit reached for {city}.")
                return None
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as err:
        print(f"Error fetching data for {city}: {err}")
        return None

async def fetch_all_cities(cities):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather(session, city) for city in cities]
        return await asyncio.gather(*tasks)
  
# Save raw data to CVS
def save_raw_data(data, city):
    # Get the absolute path of the directory containing the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Calculate the path to the data/raw directory relative to the script location
    raw_data_dir =os.path.join(script_dir, '..', 'data', 'raw')
    
    #Ensure the directory exists
    os.makedirs(raw_data_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(raw_data_dir, f"{city}_weather_{timestamp}.csv")
    
    # Save raw data as JSON
    with open(filename, 'w') as file:
        json.dump(data, file)
    print(f"Data for {city} saved to {filename}")
    
async def main():
    cities = CITIES
    weather_data = await fetch_all_cities(cities)
    
    for city, data in zip(cities, weather_data):
        if data:
            save_raw_data(data, city)
        else:
            print(f"Failed to fetch weather data for {city}.")
            return False
    return True


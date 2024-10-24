import os
import pandas as pd
import json

def load_raw_data(city):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_dir = os.path.join(script_dir, '..', 'data', 'raw')

    # Find the most recent raw data file for the city
    files = [f for f in os.listdir(raw_data_dir) if f.startswith(city)]
    if not files:
        print(f"No raw data files found for {city}")
        return None

    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(raw_data_dir, x)))
    with open(os.path.join(raw_data_dir, latest_file), 'r') as file:
        data = json.load(file)

    return data

def convert_weather_to_dataframe(data):
    weather_df = pd.json_normalize(data)
    weather_df['weather_main'] = weather_df['weather'].apply(lambda x: x[0]['main'] if isinstance(x, list) and len(x) > 0 else None)
    weather_df['weather_description'] = weather_df['weather'].apply(lambda x: x[0]['description'] if isinstance(x, list) and len(x) > 0 else None)
    weather_df.drop(columns=['weather'], inplace=True)
    weather_df['dt'] = pd.to_datetime(weather_df['dt'], unit='s')
    weather_df['sys.sunrise'] = pd.to_datetime(weather_df['sys.sunrise'], unit='s')
    weather_df['sys.sunset'] = pd.to_datetime(weather_df['sys.sunset'], unit='s')
    return weather_df

def transform_all_cities(cities):
    combined_df = pd.DataFrame()

    try:
        for city in cities:
            raw_data = load_raw_data(city)
            if not raw_data:
                print(f"Failed to load raw data for {city}. Aborting transformation.")
                return False
            
            city_df = convert_weather_to_dataframe(raw_data)
            if city_df is None or city_df.empty:
                print(f"Failed to convert raw data for {city}. Aborting transformation.")
                return False
            
            city_df['city'] = city  # Add city name to the DataFrame
            combined_df = pd.concat([combined_df, city_df], ignore_index=True)

        # Save the transformed data to CSV (optional)
        transformed_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'transformed')
        os.makedirs(transformed_data_dir, exist_ok=True)
        combined_df.to_csv(os.path.join(transformed_data_dir, 'transformed_weather_data.csv'), index=False)

        print("Transformed data saved successfully!")
        return True
    
    except Exception as e:
        print(f"An error occured during transformation: {e}")
        return False



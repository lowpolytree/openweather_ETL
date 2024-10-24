import scripts.extract as extract
import scripts.transform as transform
import scripts.load as load
import asyncio
import pandas as pd
from config.settings import CITIES
from dotenv import load_dotenv

def run_etl():
    print("Starting ETL process...")
    
    # Step 1: Extract
    print("Extracting raw weather data...")
    extraction_success = asyncio.run(extract.main())
    
    if not extraction_success:
        print("Aborting ETL process due to failed data extraction.")
        return
    
    # Step 2: Transform
    print("Transforming raw weather data...")
    cities = CITIES
    tranform_success = transform.transform_all_cities(cities)
       
    if not tranform_success:
        print("Aborting ETL process due to failed transformation.")
        return
       
    # Step 3: Load
    print("Loading transformed data into Postgres...")
    transformed_data = pd.read_csv('data/transformed/transformed_weather_data.csv')
    
    load_success = load.load_data_to_postgres(transformed_data, 'weather_data')
    
    if not load_success:
        print("Aborting ETL process due to failed data loading.")
        return
    
    print("ETL process completed successfully.")
    
if __name__ == "__main__":
    # Load postgres credentials
    load_dotenv()
    run_etl()

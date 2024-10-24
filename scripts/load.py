import psycopg
import os

def load_data_to_postgres(df, table_name):
    # Fetch credentials from environment variables
    connection_details = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }
    
    # Connect to PostgreSQL
    try:
        with psycopg.connect(**connection_details) as conn:
            with conn.cursor() as cur:
                # Insert data row by row
                for i, row in df.iterrows():
                    insert_query = f"""
                    INSERT INTO {table_name} 
                    (city, weather_main, weather_description, temp, feels_like, temp_min, temp_max, pressure, humidity, wind_speed, wind_deg, dt, sunrise, sunset)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                        row['city'],
                        row['weather_main'],
                        row['weather_description'],
                        row['main.temp'],
                        row['main.feels_like'],
                        row['main.temp_min'],
                        row['main.temp_max'],
                        row['main.pressure'],
                        row['main.humidity'],
                        row['wind.speed'],
                        row['wind.deg'],
                        row['dt'],
                        row['sys.sunrise'],
                        row['sys.sunset']
                    )

                    cur.execute(insert_query, values)
            
            # Commit the transaction
            conn.commit()
            print(f"Data loaded into {table_name} successfully!")
            return True

    except Exception as e:
        print(f"Error occurred: {e}")
        return False

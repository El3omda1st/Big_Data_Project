#!/usr/bin/env python3
# phase2_clean.py - Data cleaning from Bronze to Silver
import pandas as pd
import numpy as np
from minio import Minio
import io
import os

def main():
    print("PHASE 2: DATA CLEANING")
    print("=" * 50)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Create directories
    os.makedirs('cleaned_data', exist_ok=True)
    
    # ===== 1. WEATHER DATA CLEANING =====
    print("\n1. Processing weather data...")
    
    # Download from Bronze
    weather_response = client.get_object("bronze", "weather_data_raw.csv")
    weather_df = pd.read_csv(io.BytesIO(weather_response.read()))
    print(f"   Downloaded: {len(weather_df)} records")
    
    # Remove duplicates
    weather_df = weather_df.drop_duplicates()
    
    # Fix date_time column
    weather_df['date_time'] = pd.to_datetime(weather_df['date_time'], errors='coerce')
    
    # Fix temperature (London range: -10 to 40°C)
    weather_df['temperature_c'] = pd.to_numeric(weather_df['temperature_c'], errors='coerce')
    weather_df['temperature_c'] = weather_df['temperature_c'].clip(-10, 40)
    
    # Fix humidity (0-100%)
    weather_df['humidity'] = pd.to_numeric(weather_df['humidity'], errors='coerce')
    weather_df['humidity'] = weather_df['humidity'].clip(0, 100)
    
    # Fix rainfall (0-100mm)
    weather_df['rain_mm'] = pd.to_numeric(weather_df['rain_mm'], errors='coerce')
    weather_df['rain_mm'] = weather_df['rain_mm'].clip(0, 100)
    
    # Fix wind speed (0-150 km/h)
    weather_df['wind_speed_kmh'] = pd.to_numeric(weather_df['wind_speed_kmh'], errors='coerce')
    weather_df['wind_speed_kmh'] = weather_df['wind_speed_kmh'].clip(0, 150)
    
    # Fix visibility - convert to numeric first
    weather_df['visibility_m'] = pd.to_numeric(weather_df['visibility_m'], errors='coerce')
    weather_df['visibility_m'] = weather_df['visibility_m'].clip(50, 20000)
    
    # Fix air pressure (950-1050 hPa)
    weather_df['air_pressure_hpa'] = pd.to_numeric(weather_df['air_pressure_hpa'], errors='coerce')
    weather_df['air_pressure_hpa'] = weather_df['air_pressure_hpa'].clip(950, 1050)
    
    # Fix weather conditions
    valid_conditions = ['Clear', 'Rain', 'Fog', 'Storm', 'Snow']
    weather_df['weather_condition'] = weather_df['weather_condition'].apply(
        lambda x: x if x in valid_conditions else np.random.choice(valid_conditions)
    )
    
    # Set all cities to London
    weather_df['city'] = 'London'
    
    # Fill missing seasons based on date
    def get_season(date):
        if pd.isna(date):
            return None
        month = date.month
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Autumn'
    
    weather_df['season'] = weather_df.apply(
        lambda row: row['season'] if pd.notna(row['season']) and row['season'] in ['Winter', 'Spring', 'Summer', 'Autumn'] 
        else get_season(row['date_time']), axis=1
    )
    
    # Remove rows with missing critical data
    weather_df = weather_df.dropna(subset=['date_time', 'temperature_c'])
    
    print(f"   Cleaned: {len(weather_df)} records")
    
    # Save as Parquet
    weather_file = 'cleaned_data/weather_cleaned.parquet'
    weather_df.to_parquet(weather_file, index=False)
    print(f"   Saved: {weather_file}")
    
    # ===== 2. TRAFFIC DATA CLEANING =====
    print("\n2. Processing traffic data...")
    
    # Download from Bronze
    traffic_response = client.get_object("bronze", "traffic_data_raw.csv")
    traffic_df = pd.read_csv(io.BytesIO(traffic_response.read()))
    print(f"   Downloaded: {len(traffic_df)} records")
    
    # Remove duplicates
    traffic_df = traffic_df.drop_duplicates()
    
    # Fix date_time column
    traffic_df['date_time'] = pd.to_datetime(traffic_df['date_time'], errors='coerce')
    
    # Fix area/district names
    valid_areas = ['Camden', 'Chelsea', 'Islington', 'Southwark', 'Kensington',
                   'Westminster', 'Greenwich', 'Hackney', 'Lambeth']
    
    traffic_df['area'] = traffic_df['area'].apply(
        lambda x: x if x in valid_areas else np.random.choice(valid_areas)
    )
    
    # Fix vehicle count (0-10,000)
    traffic_df['vehicle_count'] = pd.to_numeric(traffic_df['vehicle_count'], errors='coerce')
    traffic_df['vehicle_count'] = traffic_df['vehicle_count'].clip(0, 10000)
    
    # Fix average speed (0-120 km/h)
    traffic_df['avg_speed_kmh'] = pd.to_numeric(traffic_df['avg_speed_kmh'], errors='coerce')
    traffic_df['avg_speed_kmh'] = traffic_df['avg_speed_kmh'].clip(0, 120)
    
    # Fix accident count (0-20)
    traffic_df['accident_count'] = pd.to_numeric(traffic_df['accident_count'], errors='coerce')
    traffic_df['accident_count'] = traffic_df['accident_count'].clip(0, 20)
    
    # Fix congestion level
    valid_congestion = ['Low', 'Medium', 'High']
    traffic_df['congestion_level'] = traffic_df['congestion_level'].apply(
        lambda x: x if x in valid_congestion else np.random.choice(valid_congestion)
    )
    
    # Fix road condition
    valid_road = ['Dry', 'Wet', 'Snowy', 'Damaged']
    traffic_df['road_condition'] = traffic_df['road_condition'].apply(
        lambda x: x if x in valid_road else np.random.choice(valid_road)
    )
    
    # Fix visibility - convert to numeric first
    traffic_df['visibility_m'] = pd.to_numeric(traffic_df['visibility_m'], errors='coerce')
    traffic_df['visibility_m'] = traffic_df['visibility_m'].clip(50, 10000)
    
    # Set all cities to London
    traffic_df['city'] = 'London'
    
    # Remove rows with missing critical data
    traffic_df = traffic_df.dropna(subset=['date_time', 'area', 'vehicle_count'])
    
    print(f"   Cleaned: {len(traffic_df)} records")
    
    # Save as Parquet
    traffic_file = 'cleaned_data/traffic_cleaned.parquet'
    traffic_df.to_parquet(traffic_file, index=False)
    print(f"   Saved: {traffic_file}")
    
    # ===== 3. UPLOAD TO SILVER LAYER =====
    print("\n3. Uploading to Silver layer...")
    
    # Create Silver bucket if needed
    if not client.bucket_exists("silver"):
        client.make_bucket("silver")
        print("   Created silver bucket")
    
    # Upload weather data
    client.fput_object(
        bucket_name="silver",
        object_name="weather_cleaned.parquet",
        file_path=weather_file
    )
    print("   Uploaded: weather_cleaned.parquet")
    
    # Upload traffic data
    client.fput_object(
        bucket_name="silver",
        object_name="traffic_cleaned.parquet",
        file_path=traffic_file
    )
    print("   Uploaded: traffic_cleaned.parquet")
    
    # ===== 4. SUMMARY =====
    print("\n" + "=" * 50)
    print("CLEANING COMPLETE")
    print("=" * 50)
    print(f"Weather data: {len(weather_df)} records")
    print(f"Traffic data: {len(traffic_df)} records")
    print("\nFiles in Silver bucket:")
    print("  - weather_cleaned.parquet")
    print("  - traffic_cleaned.parquet")
    print("\nCheck Silver bucket: sudo docker exec datalake_minio_setup mc ls local/silver/")
    print("=" * 50)

if __name__ == "__main__":
    # Check required packages
    try:
        import pandas
        import numpy
        from minio import Minio
        import pyarrow
    except ImportError as e:
        print(f"Error: Missing package - {e}")
        print("Install with: pip install pandas numpy pyarrow minio")
        exit(1)
    
    main()
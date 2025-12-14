# generate_all_data.py
import subprocess
import pandas as pd
import os

def generate_all_data():
    """
    Generate both weather and traffic datasets
    """
    print("=" * 60)
    print("Generating Synthetic Datasets for Big Data Project")
    print("=" * 60)
    
    # Generate weather data
    print("\n1. Generating Weather Dataset...")
    from weather_data_generator import generate_weather_data, save_weather_data
    weather_df = generate_weather_data(5000)
    save_weather_data(weather_df, 'weather_data_raw.csv')
    
    print("\n2. Generating Traffic Dataset...")
    from traffic_data_generator import generate_traffic_data, save_traffic_data
    traffic_df = generate_traffic_data(5000)
    save_traffic_data(traffic_df, 'traffic_data_raw.csv')
    
    # Create a summary report
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    
    print(f"\nWeather Dataset:")
    print(f"  Total records: {len(weather_df):,}")
    print(f"  Columns: {list(weather_df.columns)}")
    print(f"  Missing values per column:")
    for col in weather_df.columns:
        null_count = weather_df[col].isna().sum()
        if null_count > 0:
            print(f"    - {col}: {null_count} ({null_count/len(weather_df)*100:.1f}%)")
    
    print(f"\nTraffic Dataset:")
    print(f"  Total records: {len(traffic_df):,}")
    print(f"  Columns: {list(traffic_df.columns)}")
    print(f"  Missing values per column:")
    for col in traffic_df.columns:
        null_count = traffic_df[col].isna().sum()
        if null_count > 0:
            print(f"    - {col}: {null_count} ({null_count/len(traffic_df)*100:.1f}%)")
    
    # Check for duplicates
    weather_duplicates = weather_df.duplicated().sum()
    traffic_duplicates = traffic_df.duplicated().sum()
    
    print(f"\nDuplicate Records:")
    print(f"  Weather: {weather_duplicates} duplicates")
    print(f"  Traffic: {traffic_duplicates} duplicates")
    
    # Save summary to file
    with open('dataset_summary.txt', 'w') as f:
        f.write("Dataset Generation Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Weather Dataset: {len(weather_df)} records\n")
        f.write(f"Traffic Dataset: {len(traffic_df)} records\n\n")
        f.write("Files created:\n")
        f.write("1. weather_data_raw.csv\n")
        f.write("2. traffic_data_raw.csv\n")
        f.write("3. dataset_summary.txt\n")
    
    print(f"\nSummary saved to 'dataset_summary.txt'")
    print("\n" + "=" * 60)
    print("Data generation complete! Ready for Phase 1 (Bronze Layer)")
    print("=" * 60)
    
    return weather_df, traffic_df

if __name__ == "__main__":
    weather_df, traffic_df = generate_all_data()
    
    # Display sample of both datasets
    print("\nSample from Weather Dataset:")
    print(weather_df[['weather_id', 'date_time', 'temperature_c', 'weather_condition']].head())
    
    print("\nSample from Traffic Dataset:")
    print(traffic_df[['traffic_id', 'date_time', 'area', 'vehicle_count', 'congestion_level']].head())
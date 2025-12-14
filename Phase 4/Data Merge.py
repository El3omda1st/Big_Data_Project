#!/usr/bin/env python3
# phase4_merge_fixed.py - Merge datasets and handle duplicate columns
import pandas as pd
from minio import Minio
import io
import os

def main():
    print("PHASE 4: DATASET MERGING")
    print("=" * 50)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Create directory
    os.makedirs('phase4_merged', exist_ok=True)
    
    # 1. Load cleaned datasets
    print("\n1. Loading cleaned datasets...")
    
    # Load weather data
    weather_data = client.get_object("silver", "weather_cleaned.parquet")
    weather_df = pd.read_parquet(io.BytesIO(weather_data.read()))
    print(f"   Weather: {len(weather_df)} records, {len(weather_df.columns)} columns")
    print(f"   Weather columns: {list(weather_df.columns)}")
    
    # Load traffic data
    traffic_data = client.get_object("silver", "traffic_cleaned.parquet")
    traffic_df = pd.read_parquet(io.BytesIO(traffic_data.read()))
    print(f"   Traffic: {len(traffic_df)} records, {len(traffic_df.columns)} columns")
    print(f"   Traffic columns: {list(traffic_df.columns)}")
    
    # 2. Identify overlapping columns
    print("\n2. Identifying column overlaps...")
    
    weather_cols = set(weather_df.columns)
    traffic_cols = set(traffic_df.columns)
    common_cols = weather_cols.intersection(traffic_cols)
    
    print(f"   Common columns: {list(common_cols)}")
    
    # 3. Prepare datasets for merge
    print("\n3. Preparing datasets for merge...")
    
    # Standardize date_time
    weather_df['date_time'] = pd.to_datetime(weather_df['date_time'])
    traffic_df['date_time'] = pd.to_datetime(traffic_df['date_time'])
    
    # Create hourly grouping for better matching
    weather_df['hour'] = weather_df['date_time'].dt.floor('H')
    traffic_df['hour'] = traffic_df['date_time'].dt.floor('H')
    
    # 4. Merge datasets with proper suffix handling
    print("\n4. Merging datasets...")
    
    # Determine which columns get which suffix
    # Columns that are in both datasets get suffixes
    # Columns that are unique keep their names
    
    # First, let's see what happens with standard merge
    merged_df = pd.merge(
        weather_df,
        traffic_df,
        left_on=['hour', 'city'],
        right_on=['hour', 'city'],
        how='inner',
        suffixes=('_weather', '_traffic')
    )
    
    print(f"   Initial merge: {len(merged_df)} records, {len(merged_df.columns)} columns")
    
    # 5. Clean up merged dataset
    print("\n5. Cleaning merged dataset...")
    
    # Remove the helper hour column
    if 'hour' in merged_df.columns:
        merged_df = merged_df.drop(columns=['hour'])
    
    # Handle duplicate city columns
    if 'city_traffic' in merged_df.columns and 'city_weather' in merged_df.columns:
        # They should both be 'London', drop one
        merged_df = merged_df.drop(columns=['city_traffic'])
        merged_df = merged_df.rename(columns={'city_weather': 'city'})
    
    # Handle duplicate date_time columns
    if 'date_time_traffic' in merged_df.columns and 'date_time_weather' in merged_df.columns:
        # Use weather date_time (should be same), drop traffic
        merged_df = merged_df.drop(columns=['date_time_traffic'])
        merged_df = merged_df.rename(columns={'date_time_weather': 'date_time'})
    
    # Handle duplicate visibility columns
    if 'visibility_m_weather' in merged_df.columns and 'visibility_m_traffic' in merged_df.columns:
        # Use weather visibility, rename it
        merged_df['visibility_m'] = merged_df['visibility_m_weather']
        merged_df = merged_df.drop(columns=['visibility_m_weather', 'visibility_m_traffic'])
    
    # Check for any other duplicates
    duplicate_suffixes = ['_weather', '_traffic']
    for suffix in duplicate_suffixes:
        cols_with_suffix = [col for col in merged_df.columns if col.endswith(suffix)]
        for col in cols_with_suffix:
            base_col = col.replace(suffix, '')
            other_suffix = '_traffic' if suffix == '_weather' else '_weather'
            other_col = base_col + other_suffix
            
            if other_col in merged_df.columns:
                print(f"   Found duplicate: {col} and {other_col}")
                # Keep the _weather version for weather data, _traffic for traffic data
                if 'weather' in col:
                    merged_df[base_col] = merged_df[col]
                merged_df = merged_df.drop(columns=[col, other_col])
    
    # 6. Select final columns - only keep unique ones
    print("\n6. Selecting final columns...")
    
    # Define which columns we want in final dataset
    final_columns = []
    
    # Basic info
    if 'date_time' in merged_df.columns:
        final_columns.append('date_time')
    if 'city' in merged_df.columns:
        final_columns.append('city')
    
    # Weather columns (unique to weather or properly renamed)
    weather_unique = ['season', 'temperature_c', 'humidity', 'rain_mm', 
                     'wind_speed_kmh', 'weather_condition', 'air_pressure_hpa']
    for col in weather_unique:
        if col in merged_df.columns:
            final_columns.append(col)
    
    # Traffic columns (unique to traffic)
    traffic_unique = ['area', 'vehicle_count', 'avg_speed_kmh', 
                     'accident_count', 'congestion_level', 'road_condition']
    for col in traffic_unique:
        if col in merged_df.columns:
            final_columns.append(col)
    
    # Visibility (now unified)
    if 'visibility_m' in merged_df.columns:
        final_columns.append('visibility_m')
    
    # Create final dataset
    final_df = merged_df[final_columns].copy()
    
    # Remove any remaining duplicates
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    
    print(f"   Final dataset: {len(final_df)} records, {len(final_df.columns)} columns")
    print(f"   Final columns: {list(final_df.columns)}")
    
    # 7. Save merged dataset
    print("\n7. Saving merged dataset...")
    
    # Save locally
    local_file = './merged_dataset.parquet'
    final_df.to_parquet(local_file, index=False)
    print(f"   Saved locally: {local_file}")
    
    # 8. Upload to MinIO Gold
    print("\n8. Uploading to MinIO...")
    
    # Create Gold bucket if needed
    if not client.bucket_exists("gold"):
        client.make_bucket("gold")
        print("   Created gold bucket")
    
    # Upload Parquet file
    client.fput_object(
        bucket_name="gold",
        object_name="merged_dataset.parquet",
        file_path=local_file
    )
    print("   Uploaded to gold: merged_dataset.parquet")
    
    # 9. Summary
    print("\n" + "=" * 50)
    print("MERGE COMPLETE")
    print("=" * 50)
    print(f"Merged records: {len(final_df)}")
    print(f"Unique columns: {len(final_df.columns)}")
    
    print("\nDataset structure:")
    for i, col in enumerate(final_df.columns, 1):
        non_null = final_df[col].count()
        print(f"  {i:2d}. {col:25s} ({non_null} non-null values)")
    
    print("\nSample data:")
    print(final_df.head(3))
    
    print("\nTo verify in MinIO:")
    print("  sudo docker exec datalake_minio_setup mc ls local/gold/")
    print("=" * 50)

if __name__ == "__main__":
    main()
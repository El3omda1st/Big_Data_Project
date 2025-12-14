# weather_data_generator.py
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import uuid

def generate_weather_data(num_records=5000):
    """
    Generate synthetic weather data with messy scenarios
    """
    
    # Base data structures
    data = {
        'weather_id': [],
        'date_time': [],
        'city': [],
        'season': [],
        'temperature_c': [],
        'humidity': [],
        'rain_mm': [],
        'wind_speed_kmh': [],
        'visibility_m': [],
        'weather_condition': [],
        'air_pressure_hpa': []
    }
    
    # Season definitions
    seasons = {
        1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer', 9: 'Autumn', 10: 'Autumn',
        11: 'Autumn', 12: 'Winter'
    }
    
    # Weather conditions with probabilities
    weather_conditions = ['Clear', 'Rain', 'Fog', 'Storm', 'Snow']
    weather_probs = [0.5, 0.25, 0.1, 0.1, 0.05]
    
    # Start date
    start_date = datetime(2024, 1, 1, 0, 0)
    
    # Generate base records
    for i in range(num_records):
        # Generate ID (5001, 5002, ...)
        base_id = 5000 + i + 1
        
        # Decide if this record will have issues
        has_issues = random.random() < 0.3  # 30% of records have issues
        
        # ----- weather_id -----
        if has_issues and random.random() < 0.1:  # 10% of problematic records
            data['weather_id'].append(None)  # NULL ID
        else:
            data['weather_id'].append(base_id)
        
        # ----- date_time -----
        current_date = start_date + timedelta(hours=i)
        
        if has_issues:
            # Introduce date format issues
            rand_choice = random.random()
            if rand_choice < 0.05:  # 5%: NULL timestamp
                data['date_time'].append(None)
            elif rand_choice < 0.1:  # 5%: Invalid format
                invalid_formats = ['2099-13-40 25:61', 'Unknown', 'Invalid Date']
                data['date_time'].append(random.choice(invalid_formats))
            elif rand_choice < 0.2:  # 10%: Different format
                formats = [
                    current_date.strftime('%d/%m/%Y %I:%M%p'),  # 15/01/2024 2PM
                    current_date.strftime('%Y-%m-%dT%H:%MZ'),   # 2024-01-15T14:00Z
                    current_date.strftime('%Y/%m/%d %H:%M:%S')   # 2024/01/15 14:00:00
                ]
                data['date_time'].append(random.choice(formats))
            else:  # Standard format
                data['date_time'].append(current_date.strftime('%Y-%m-%d %H:%M'))
        else:
            data['date_time'].append(current_date.strftime('%Y-%m-%d %H:%M'))
        
        # ----- city -----
        if has_issues and random.random() < 0.05:  # 5% NULL city
            data['city'].append(None)
        else:
            data['city'].append('London')
        
        # ----- season -----
        month = current_date.month
        if has_issues and random.random() < 0.1:  # 10% NULL or wrong season
            if random.random() < 0.5:
                data['season'].append(None)
            else:
                wrong_seasons = ['Monsoon', 'Fall', 'Rainy', 'Dry']
                data['season'].append(random.choice(wrong_seasons))
        else:
            data['season'].append(seasons[month])
        
        # ----- temperature_c -----
        season = seasons[month]
        if season == 'Winter':
            base_temp = random.uniform(-5, 15)
        elif season == 'Summer':
            base_temp = random.uniform(10, 35)
        elif season == 'Spring':
            base_temp = random.uniform(5, 25)
        else:  # Autumn
            base_temp = random.uniform(5, 20)
        
        if has_issues and random.random() < 0.05:  # 5% outliers
            data['temperature_c'].append(random.choice([-30, 60, 100]))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['temperature_c'].append(None)
        else:
            data['temperature_c'].append(round(base_temp, 1))
        
        # ----- humidity -----
        if has_issues and random.random() < 0.05:  # 5% outliers
            data['humidity'].append(random.choice([-10, 150, 200]))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['humidity'].append(None)
        else:
            # Higher humidity in winter, lower in summer
            if season in ['Winter', 'Autumn']:
                data['humidity'].append(random.randint(60, 100))
            else:
                data['humidity'].append(random.randint(20, 80))
        
        # ----- rain_mm -----
        if has_issues and random.random() < 0.05:  # 5% extreme values
            data['rain_mm'].append(random.uniform(120, 300))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['rain_mm'].append(None)
        else:
            # More rain in winter and autumn
            if season in ['Winter', 'Autumn']:
                data['rain_mm'].append(round(random.uniform(0, 30), 1))
            else:
                data['rain_mm'].append(round(random.uniform(0, 10), 1))
        
        # ----- wind_speed_kmh -----
        if has_issues and random.random() < 0.05:  # 5% outliers
            data['wind_speed_kmh'].append(random.uniform(200, 500))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['wind_speed_kmh'].append(None)
        else:
            data['wind_speed_kmh'].append(round(random.uniform(0, 80), 1))
        
        # ----- visibility_m -----
        if has_issues and random.random() < 0.05:  # 5% extreme values
            data['visibility_m'].append(50000)
        elif has_issues and random.random() < 0.05:  # 5% non-numeric strings
            non_numeric = ['Low', 'Very Low', 'High', 'Unknown']
            data['visibility_m'].append(random.choice(non_numeric))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['visibility_m'].append(None)
        else:
            data['visibility_m'].append(random.randint(50, 10000))
        
        # ----- weather_condition -----
        if has_issues and random.random() < 0.1:  # 10% NULL
            data['weather_condition'].append(None)
        else:
            # Adjust probabilities based on season
            if season == 'Winter':
                adj_probs = [0.4, 0.2, 0.1, 0.1, 0.2]  # More snow
            elif season == 'Summer':
                adj_probs = [0.7, 0.1, 0.05, 0.1, 0.05]  # More clear
            else:
                adj_probs = weather_probs
            
            data['weather_condition'].append(np.random.choice(weather_conditions, p=adj_probs))
        
        # ----- air_pressure_hpa -----
        if has_issues and random.random() < 0.1:  # 10% NULL
            data['air_pressure_hpa'].append(None)
        else:
            data['air_pressure_hpa'].append(round(random.uniform(950, 1050), 1))
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add duplicates (5% of records)
    num_duplicates = int(num_records * 0.05)
    duplicate_indices = random.sample(range(num_records), num_duplicates)
    duplicates = df.iloc[duplicate_indices].copy()
    
    # Modify some duplicates slightly
    for idx in range(len(duplicates)):
        if random.random() < 0.5:
            # Change the ID to create exact duplicate IDs
            duplicates.iloc[idx, 0] = random.choice(data['weather_id'][:100])
    
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # Shuffle the DataFrame
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    return df

def save_weather_data(df, filename='synthetic_weather_data.csv'):
    """Save the generated weather data to CSV"""
    df.to_csv(filename, index=False)
    print(f"Weather data saved to {filename}")
    print(f"Total records: {len(df)}")
    print(f"Records with NULL weather_id: {df['weather_id'].isna().sum()}")
    print(f"Records with NULL date_time: {df['date_time'].isna().sum()}")
    print(f"Records with extreme temperature: {len(df[(df['temperature_c'] > 50) | (df['temperature_c'] < -10)])}")
    
    return df

if __name__ == "__main__":
    # Generate weather data
    weather_df = generate_weather_data(5000)
    save_weather_data(weather_df, 'weather_data_raw.csv')
    
    # Display sample
    print("\nSample of generated weather data (first 10 rows):")
    print(weather_df.head(10))
    print("\nData types:")
    print(weather_df.dtypes)
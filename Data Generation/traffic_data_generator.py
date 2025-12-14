# traffic_data_generator.py
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_traffic_data(num_records=5000):
    """
    Generate synthetic traffic data with messy scenarios
    """
    # London areas/districts
    london_areas = ['Camden', 'Chelsea', 'Islington', 'Southwark', 'Kensington', 
                    'Westminster', 'Greenwich', 'Hackney', 'Lambeth', 'Tower Hamlets',
                    'Wandsworth', 'Hammersmith', 'Brent', 'Ealing', 'Hounslow']
    # Base data structures
    data = {
        'traffic_id': [],
        'date_time': [],
        'city': [],
        'area': [],
        'vehicle_count': [],
        'avg_speed_kmh': [],
        'accident_count': [],
        'congestion_level': [],
        'road_condition': [],
        'visibility_m': []
    }
    
    # Congestion levels with probabilities
    congestion_levels = ['Low', 'Medium', 'High']
    congestion_probs = [0.6, 0.3, 0.1]
    
    # Road conditions
    road_conditions = ['Dry', 'Wet', 'Snowy', 'Damaged']
    
    # Start date (same as weather for merging later)
    start_date = datetime(2024, 1, 1, 0, 0)
    
    # Generate base records
    for i in range(num_records):
        # Generate ID (9001, 9002, ...)
        base_id = 9000 + i + 1
        
        # Decide if this record will have issues
        has_issues = random.random() < 0.3  # 30% of records have issues
        
        # ----- traffic_id -----
        if has_issues and random.random() < 0.1:  # 10% of problematic records
            data['traffic_id'].append(None)  # NULL ID
        else:
            data['traffic_id'].append(base_id)
        
        # ----- date_time -----
        # Generate time with more traffic during rush hours
        hour_offset = i % 24
        current_date = start_date + timedelta(hours=i, minutes=random.randint(0, 59))
        
        if has_issues:
            # Introduce date format issues
            rand_choice = random.random()
            if rand_choice < 0.05:  # 5%: NULL timestamp
                data['date_time'].append(None)
            elif rand_choice < 0.1:  # 5%: Invalid format
                invalid_formats = ['TBD', '2099-00-00 99:99', 'Unknown', 'N/A']
                data['date_time'].append(random.choice(invalid_formats))
            elif rand_choice < 0.2:  # 10%: Different format
                formats = [
                    current_date.strftime('%d/%m/%Y %I:%M%p'),  # 15/01/2024 8AM
                    current_date.strftime('%Y-%m-%dT%H:%MZ'),   # 2024-01-15T08:00Z
                    current_date.strftime('%d-%b-%Y %H:%M')     # 15-Jan-2024 08:00
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
        
        # ----- area -----
        if has_issues and random.random() < 0.1:  # 10% NULL area
            data['area'].append(None)
        else:
            data['area'].append(random.choice(london_areas))
        
        # ----- vehicle_count -----
        # More vehicles during rush hours (7-9 AM, 5-7 PM)
        hour = current_date.hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            base_vehicles = random.randint(1000, 5000)
        else:
            base_vehicles = random.randint(100, 2000)
        
        if has_issues and random.random() < 0.05:  # 5% extreme outliers
            data['vehicle_count'].append(random.randint(20000, 50000))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['vehicle_count'].append(None)
        else:
            data['vehicle_count'].append(base_vehicles)
        
        # ----- avg_speed_kmh -----
        # Lower speed during high traffic
        if base_vehicles > 3000:
            base_speed = random.uniform(10, 40)
        else:
            base_speed = random.uniform(40, 120)
        
        if has_issues and random.random() < 0.05:  # 5% negative speeds
            data['avg_speed_kmh'].append(random.uniform(-50, -1))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['avg_speed_kmh'].append(None)
        else:
            data['avg_speed_kmh'].append(round(base_speed, 1))
        
        # ----- accident_count -----
        # More accidents during bad weather (simulated by random here)
        if has_issues and random.random() < 0.02:  # 2% extreme values
            data['accident_count'].append(random.randint(50, 100))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['accident_count'].append(None)
        else:
            # Higher probability during rush hours
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                data['accident_count'].append(np.random.choice([0, 1, 2, 3, 4, 5], 
                                                               p=[0.8, 0.1, 0.05, 0.03, 0.01, 0.01]))
            else:
                data['accident_count'].append(np.random.choice([0, 1, 2, 3], 
                                                               p=[0.95, 0.03, 0.015, 0.005]))
        
        # ----- congestion_level -----
        # Determine congestion based on vehicle count and speed
        if has_issues and random.random() < 0.1:  # 10% NULL
            data['congestion_level'].append(None)
        elif has_issues and random.random() < 0.05:  # 5% incorrect categories
            wrong_categories = ['Very High', 'Low-Medium', 'Heavy', 'Light', 'Severe']
            data['congestion_level'].append(random.choice(wrong_categories))
        else:
            # Base on vehicle count and speed
            if base_vehicles > 4000 and base_speed < 30:
                data['congestion_level'].append('High')
            elif base_vehicles > 2000 and base_speed < 50:
                data['congestion_level'].append('Medium')
            else:
                data['congestion_level'].append('Low')
        
        # ----- road_condition -----
        if has_issues and random.random() < 0.1:  # 10% NULL
            data['road_condition'].append(None)
        else:
            data['road_condition'].append(random.choice(road_conditions))
        
        # ----- visibility_m -----
        if has_issues and random.random() < 0.05:  # 5% extreme values
            data['visibility_m'].append(random.choice([10, 100000]))
        elif has_issues and random.random() < 0.1:  # 10% NULL
            data['visibility_m'].append(None)
        else:
            data['visibility_m'].append(random.randint(50, 10000))
    
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
            duplicates.iloc[idx, 0] = random.choice(data['traffic_id'][:100])
    
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # Shuffle the DataFrame
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    return df

def save_traffic_data(df, filename='synthetic_traffic_data.csv'):
    """Save the generated traffic data to CSV"""
    df.to_csv(filename, index=False)
    print(f"Traffic data saved to {filename}")
    print(f"Total records: {len(df)}")
    print(f"Records with negative speed: {len(df[df['avg_speed_kmh'] < 0])}")
    print(f"Records with NULL area: {df['area'].isna().sum()}")
    print(f"Records with extreme vehicle count (>20000): {len(df[df['vehicle_count'] > 20000])}")
    
    return df

if __name__ == "__main__":
    # Generate traffic data
    traffic_df = generate_traffic_data(5000)
    save_traffic_data(traffic_df, 'traffic_data_raw.csv')
    
    # Display sample
    print("\nSample of generated traffic data (first 10 rows):")
    print(traffic_df.head(10))
    print("\nData types:")
    print(traffic_df.dtypes)
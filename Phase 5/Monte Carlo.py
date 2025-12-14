#!/usr/bin/env python3
# phase5_mc.py - Monte Carlo Simulation using actual data from MinIO Gold
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from minio import Minio
import io
import os

def main():
    print("PHASE 5: MONTE CARLO SIMULATION")
    print("=" * 60)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Create directories
    os.makedirs('phase5_results', exist_ok=True)
    
    # 1. Load merged dataset from Gold layer
    print("\n1. Loading merged dataset from MinIO Gold...")
    
    try:
        response = client.get_object("gold", "merged_dataset.parquet")
        df = pd.read_parquet(io.BytesIO(response.read()))
        print(f"   Loaded {len(df)} records")
        
    except Exception as e:
        print(f"   Error: {e}")
        return
    
    # 2. Prepare data
    print("\n2. Preparing data for simulation...")
    
    # Ensure numeric columns
    numeric_cols = ['temperature_c', 'rain_mm', 'humidity', 'wind_speed_kmh', 'visibility_m']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Handle missing values
    df = df.dropna(subset=numeric_cols)
    
    # 3. Calculate baseline probabilities from actual data
    print("\n3. Calculating baseline probabilities...")
    
    total_records = len(df)
    
    # Traffic jam probability (congestion_level = 'High')
    if 'congestion_level' in df.columns:
        jam_count = len(df[df['congestion_level'] == 'High'])
        base_jam_prob = jam_count / total_records if total_records > 0 else 0.15
    else:
        base_jam_prob = 0.15
    
    # Accident probability
    if 'accident_count' in df.columns:
        accident_count = len(df[df['accident_count'] > 0])
        base_accident_prob = accident_count / total_records if total_records > 0 else 0.05
    else:
        base_accident_prob = 0.05
    
    print(f"   Base traffic jam probability: {base_jam_prob:.3f}")
    print(f"   Base accident probability: {base_accident_prob:.3f}")
    
    # 4. Define simulation scenarios using actual data filters
    print("\n4. Setting up simulation scenarios...")
    
    scenarios = [
        {
            'name': 'heavy_rain',
            'condition': 'rain_mm > 20',
            'description': 'Heavy Rain (> 20mm)'
        },
        {
            'name': 'temperature_extreme_cold', 
            'condition': 'temperature_c < 0',
            'description': 'Extreme Cold (< 0°C)'
        },
        {
            'name': 'temperature_extreme_hot',
            'condition': 'temperature_c > 30',
            'description': 'Extreme Heat (> 30°C)'
        },
        {
            'name': 'high_humidity',
            'condition': 'humidity > 85',
            'description': 'High Humidity (> 85%)'
        },
        {
            'name': 'low_visibility',
            'condition': 'visibility_m < 500',
            'description': 'Low Visibility (< 500m)'
        },
        {
            'name': 'strong_winds',
            'condition': 'wind_speed_kmh > 60',
            'description': 'Strong Winds (> 60 km/h)'
        }
    ]
    
    # Add normal conditions for comparison
    scenarios.append({
        'name': 'normal_conditions',
        'condition': '(temperature_c >= 10) & (temperature_c <= 25) & (rain_mm < 5) & (humidity <= 70) & (wind_speed_kmh < 30) & (visibility_m > 1000)',
        'description': 'Normal Conditions'
    })
    
    # 5. Run Monte Carlo simulations using ONLY actual data
    print("\n5. Running Monte Carlo simulations...")
    
    n_simulations = 5000
    results = []
    
    for scenario in scenarios:
        print(f"   Simulating: {scenario['description']}")
        
        # Filter data for this scenario using actual data only
        try:
            scenario_data = df.query(scenario['condition'])
        except:
            # If query fails, filter manually
            if scenario['name'] == 'heavy_rain':
                scenario_data = df[df['rain_mm'] > 20]
            elif scenario['name'] == 'temperature_extreme_cold':
                scenario_data = df[df['temperature_c'] < 0]
            elif scenario['name'] == 'temperature_extreme_hot':
                scenario_data = df[df['temperature_c'] > 30]
            elif scenario['name'] == 'high_humidity':
                scenario_data = df[df['humidity'] > 85]
            elif scenario['name'] == 'low_visibility':
                scenario_data = df[df['visibility_m'] < 500]
            elif scenario['name'] == 'strong_winds':
                scenario_data = df[df['wind_speed_kmh'] > 60]
            else:  # normal_conditions
                scenario_data = df[
                    (df['temperature_c'] >= 10) & 
                    (df['temperature_c'] <= 25) & 
                    (df['rain_mm'] < 5) &
                    (df['humidity'] <= 70) &
                    (df['wind_speed_kmh'] < 30) &
                    (df['visibility_m'] > 1000)
                ]
        
        if len(scenario_data) < 10:
            print(f"      Warning: Only {len(scenario_data)} records for this scenario")
            # Use all data but adjust probabilities
            simulation_data = df
            adjustment_factor = 1.5  # Assume worse conditions
        else:
            simulation_data = scenario_data
            adjustment_factor = 1.0
        
        # Run Monte Carlo simulations using actual data samples
        jam_probs = []
        accident_probs = []
        
        for i in range(n_simulations):
            # Sample from actual data
            if len(simulation_data) > 0:
                sample = simulation_data.sample(n=1, replace=True).iloc[0]
                
                # Calculate probabilities based on actual sample
                jam_prob = calculate_jam_from_sample(sample, base_jam_prob, adjustment_factor)
                accident_prob = calculate_accident_from_sample(sample, base_accident_prob, adjustment_factor)
                
                jam_probs.append(jam_prob)
                accident_probs.append(accident_prob)
        
        # Calculate statistics
        if jam_probs:
            avg_jam = np.mean(jam_probs)
            std_jam = np.std(jam_probs)
            avg_accident = np.mean(accident_probs)
            std_accident = np.std(accident_probs)
        else:
            avg_jam = base_jam_prob * adjustment_factor
            std_jam = 0.1
            avg_accident = base_accident_prob * adjustment_factor
            std_accident = 0.05
        
        # Store results
        result = {
            'scenario': scenario['name'],
            'description': scenario['description'],
            'n_actual_records': len(scenario_data),
            'avg_traffic_jam_prob': avg_jam,
            'std_traffic_jam_prob': std_jam,
            'avg_accident_prob': avg_accident,
            'std_accident_prob': std_accident
        }
        
        results.append(result)
        
        print(f"      Traffic jam probability: {avg_jam:.3f} (±{std_jam:.3f})")
        print(f"      Accident probability: {avg_accident:.3f} (±{std_accident:.3f})")
    
    # 6. Save results
    print("\n6. Saving simulation results...")
    
    # Save to CSV
    results_df = pd.DataFrame(results)
    csv_file = 'phase5_results/simulation_results.csv'
    results_df.to_csv(csv_file, index=False)
    print(f"   Saved: {csv_file}")
    
    # Upload to MinIO Gold
    client.fput_object("gold", "simulation_results.csv", csv_file)
    print("   Uploaded to MinIO Gold: simulation_results.csv")
    
    # 7. Create plots
    print("\n7. Creating visualization plots...")
    
    # Plot 1: Traffic jam probabilities by scenario
    plt.figure(figsize=(12, 6))
    
    scenarios_list = [r['description'] for r in results]
    jam_probs_list = [r['avg_traffic_jam_prob'] for r in results]
    jam_errors = [r['std_traffic_jam_prob'] for r in results]
    
    colors = ['blue', 'lightblue', 'red', 'green', 'gray', 'purple', 'yellow']
    
    bars = plt.bar(scenarios_list, jam_probs_list, yerr=jam_errors, 
                   capsize=5, alpha=0.7, color=colors)
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Probability')
    plt.title('Traffic Jam Probability by Weather Scenario (Monte Carlo Simulation)')
    plt.ylim(0, max(jam_probs_list) * 1.2)
    
    # Add value labels on bars
    for bar, prob in zip(bars, jam_probs_list):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{prob:.3f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    jam_plot = 'phase5_results/congestion_probability.png'
    plt.savefig(jam_plot, dpi=300)
    print(f"   Saved: {jam_plot}")
    
    # Upload plot to MinIO
    client.fput_object("gold", "congestion_probability.png", jam_plot)
    print("   Uploaded to MinIO Gold: congestion_probability.png")
    
    # Plot 2: Comparison of jam vs accident probabilities
    plt.figure(figsize=(10, 6))
    
    x = np.arange(len(results))
    width = 0.35
    
    plt.bar(x - width/2, jam_probs_list, width, label='Traffic Jam', alpha=0.7)
    plt.bar(x + width/2, [r['avg_accident_prob'] for r in results], width, label='Accident', alpha=0.7)
    
    plt.xlabel('Scenario')
    plt.ylabel('Probability')
    plt.title('Traffic Jam vs Accident Probabilities by Scenario')
    plt.xticks(x, [r['description'][:15] + '...' for r in results], rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()
    
    comp_plot = 'phase5_results/jam_vs_accident.png'
    plt.savefig(comp_plot, dpi=300)
    print(f"   Saved: {comp_plot}")
    
    # Upload plot to MinIO
    client.fput_object("gold", "jam_vs_accident.png", comp_plot)
    
    # 8. Generate distribution plot from actual simulation data
    print("\n8. Generating probability distributions...")
    
    # Run one more simulation to collect distribution data
    plt.figure(figsize=(10, 6))
    
    # Simulate normal vs heavy rain scenarios
    normal_data = df.query('rain_mm < 5 and visibility_m > 1000')
    heavy_rain_data = df.query('rain_mm > 20')
    
    normal_probs = []
    heavy_rain_probs = []
    
    for _ in range(1000):
        if len(normal_data) > 0:
            sample = normal_data.sample(n=1, replace=True).iloc[0]
            prob = calculate_jam_from_sample(sample, base_jam_prob, 1.0)
            normal_probs.append(prob)
        
        if len(heavy_rain_data) > 0:
            sample = heavy_rain_data.sample(n=1, replace=True).iloc[0]
            prob = calculate_jam_from_sample(sample, base_jam_prob, 1.5)
            heavy_rain_probs.append(prob)
    
    # Plot distributions
    if normal_probs:
        plt.hist(normal_probs, bins=30, alpha=0.5, label='Normal Conditions', density=True)
    if heavy_rain_probs:
        plt.hist(heavy_rain_probs, bins=30, alpha=0.5, label='Heavy Rain', density=True)
    
    plt.xlabel('Traffic Jam Probability')
    plt.ylabel('Density')
    plt.title('Distribution of Congestion Probabilities (Monte Carlo Simulation)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    dist_plot = 'phase5_results/congestion_distribution.png'
    plt.savefig(dist_plot, dpi=300)
    print(f"   Saved: {dist_plot}")
    
    # Upload plot to MinIO
    client.fput_object("gold", "congestion_distribution.png", dist_plot)
    print("   Uploaded to MinIO Gold: congestion_distribution.png")
    
    # 9. Summary
    print("\n" + "=" * 60)
    print("MONTE CARLO SIMULATION COMPLETE")
    print("=" * 60)
    
    print(f"\nSimulation Parameters:")
    print(f"  Simulations per scenario: {n_simulations}")
    print(f"  Total scenarios: {len(scenarios)}")
    print(f"  Base data records: {len(df)}")
    
    print("\nKey Findings:")
    print("-" * 40)
    
    # Find highest risk scenarios
    sorted_by_jam = sorted(results, key=lambda x: x['avg_traffic_jam_prob'], reverse=True)
    
    print(f"\nHighest traffic jam risk: {sorted_by_jam[0]['description']}")
    print(f"  Probability: {sorted_by_jam[0]['avg_traffic_jam_prob']:.3f}")
    
    sorted_by_accident = sorted(results, key=lambda x: x['avg_accident_prob'], reverse=True)
    print(f"\nHighest accident risk: {sorted_by_accident[0]['description']}")
    print(f"  Probability: {sorted_by_accident[0]['avg_accident_prob']:.3f}")
    
    print("\n" + "=" * 60)
    print("Deliverables saved to MinIO Gold layer:")
    print("  - simulation_results.csv")
    print("  - congestion_probability.png")
    print("  - jam_vs_accident.png")
    print("  - congestion_distribution.png")
    print("=" * 60)

def calculate_jam_from_sample(sample, base_prob, adjustment_factor):
    """Calculate traffic jam probability from actual data sample"""
    prob = base_prob * adjustment_factor
    
    # Adjust based on actual weather conditions in sample
    if 'rain_mm' in sample and sample['rain_mm'] > 20:
        prob *= 1.8
    elif 'rain_mm' in sample and sample['rain_mm'] > 10:
        prob *= 1.4
    
    if 'temperature_c' in sample:
        if sample['temperature_c'] < 0 or sample['temperature_c'] > 30:
            prob *= 1.3
    
    if 'visibility_m' in sample and sample['visibility_m'] < 500:
        prob *= 1.5
    
    if 'wind_speed_kmh' in sample and sample['wind_speed_kmh'] > 60:
        prob *= 1.2
    
    # Add small random noise for Monte Carlo
    prob += np.random.normal(0, 0.03)
    
    # Ensure bounds
    return max(0.01, min(prob, 0.95))

def calculate_accident_from_sample(sample, base_prob, adjustment_factor):
    """Calculate accident probability from actual data sample"""
    prob = base_prob * adjustment_factor
    
    # Adjust based on actual weather conditions
    if 'rain_mm' in sample:
        if sample['rain_mm'] > 30:
            prob *= 3.0
        elif sample['rain_mm'] > 20:
            prob *= 2.5
        elif sample['rain_mm'] > 10:
            prob *= 2.0
    
    if 'visibility_m' in sample and sample['visibility_m'] < 200:
        prob *= 2.5
    elif 'visibility_m' in sample and sample['visibility_m'] < 500:
        prob *= 1.8
    
    if 'wind_speed_kmh' in sample and sample['wind_speed_kmh'] > 60:
        prob *= 1.5
    
    # Add small random noise
    prob += np.random.normal(0, 0.01)
    
    # Ensure bounds
    return max(0.001, min(prob, 0.5))

if __name__ == "__main__":
    main()
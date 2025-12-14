#!/usr/bin/env python3
# phase6_factor_analysis.py - Factor Analysis for weather impact on traffic
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from minio import Minio
import io
import os
from sklearn.preprocessing import StandardScaler
from factor_analyzer import FactorAnalyzer
from factor_analyzer.factor_analyzer import calculate_bartlett_sphericity, calculate_kmo

def main():
    print("PHASE 6: FACTOR ANALYSIS")
    print("=" * 60)
    print("Identifying weather variables with strongest effect on traffic patterns")
    print("=" * 60)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Create directories
    os.makedirs('phase6_results', exist_ok=True)
    
    # 1. Load merged dataset from Gold layer
    print("\n1. Loading merged dataset from MinIO Gold...")
    
    try:
        response = client.get_object("gold", "merged_dataset.parquet")
        df = pd.read_parquet(io.BytesIO(response.read()))
        print(f"   Loaded {len(df)} records")
        print(f"   Columns: {list(df.columns)}")
        
    except Exception as e:
        print(f"   Error loading data: {e}")
        return
    
    # 2. Select features for factor analysis
    print("\n2. Selecting features for factor analysis...")
    
    # Define feature mapping based on available columns
    feature_mapping = {
        'temperature': ['temperature_c', 'temp', 'temperature'],
        'humidity': ['humidity'],
        'rain': ['rain_mm', 'rain', 'rainfall'],
        'wind_speed': ['wind_speed_kmh', 'wind_speed', 'wind'],
        'visibility': ['visibility_m', 'visibility'],
        'air_pressure': ['air_pressure_hpa', 'air_pressure', 'pressure'],
        'vehicle_count': ['vehicle_count', 'vehicles'],
        'average_speed': ['avg_speed_kmh', 'avg_speed', 'speed'],
        'accident_count': ['accident_count', 'accidents']
    }
    
    # Find actual column names
    selected_features = {}
    selected_data = pd.DataFrame()
    
    for feature_name, possible_cols in feature_mapping.items():
        found = False
        for col in possible_cols:
            if col in df.columns:
                selected_data[feature_name] = df[col]
                selected_features[feature_name] = col
                found = True
                print(f"   ✓ {feature_name}: using '{col}'")
                break
        
        if not found:
            print(f"   ✗ {feature_name}: column not found")
    
    print(f"\n   Selected {len(selected_data.columns)} features for analysis")
    
    # 3. Prepare data for factor analysis
    print("\n3. Preparing data for factor analysis...")
    
    # Remove rows with missing values
    data_clean = selected_data.dropna()
    print(f"   Records after removing missing values: {len(data_clean)}")
    
    if len(data_clean) < 50:
        print("   Warning: Insufficient data for factor analysis")
        return
    
    # Standardize the data
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data_clean)
    data_scaled_df = pd.DataFrame(data_scaled, columns=data_clean.columns)
    
    # 4. Check suitability for factor analysis
    print("\n4. Checking data suitability...")
    
    # Bartlett's test of sphericity
    chi_square_value, p_value = calculate_bartlett_sphericity(data_scaled_df)
    print(f"   Bartlett's Test:")
    print(f"     Chi-square: {chi_square_value:.2f}")
    print(f"     p-value: {p_value:.4f}")
    
    if p_value > 0.05:
        print("     Warning: Data may not be suitable for factor analysis (p > 0.05)")
    else:
        print("     ✓ Data is suitable for factor analysis (p < 0.05)")
    
    # KMO test
    kmo_all, kmo_model = calculate_kmo(data_scaled_df)
    print(f"\n   KMO Test:")
    print(f"     Overall KMO: {kmo_model:.3f}")
    
    if kmo_model < 0.6:
        print("     Warning: KMO value suggests data may not be suitable")
    elif kmo_model < 0.7:
        print("     ✓ Mediocre suitability for factor analysis")
    elif kmo_model < 0.8:
        print("     ✓ Good suitability for factor analysis")
    elif kmo_model < 0.9:
        print("     ✓ Great suitability for factor analysis")
    else:
        print("     ✓ Excellent suitability for factor analysis")
    
    # 5. Determine number of factors
    print("\n5. Determining number of factors...")
    
    # Method 1: Eigenvalues > 1 (Kaiser criterion)
    fa = FactorAnalyzer(rotation=None, method='minres')
    fa.fit(data_scaled_df)
    ev, v = fa.get_eigenvalues()
    
    # Plot scree plot
    plt.figure(figsize=(10, 6))
    plt.plot(range(1, len(ev) + 1), ev, marker='o', linestyle='--')
    plt.axhline(y=1, color='r', linestyle='-')
    plt.title('Scree Plot for Factor Analysis')
    plt.xlabel('Factor Number')
    plt.ylabel('Eigenvalue')
    plt.grid(True, alpha=0.3)
    
    scree_plot = 'phase6_results/scree_plot.png'
    plt.savefig(scree_plot, dpi=300)
    plt.close()
    
    # Count eigenvalues > 1
    n_factors_kaiser = sum(ev > 1)
    print(f"   Kaiser criterion (eigenvalues > 1): {n_factors_kaiser} factors")
    
    # Method 2: Parallel analysis (simplified)
    print(f"   Eigenvalues: {ev[:5].round(3)}...")
    
    # Based on project requirements and eigenvalues, choose 2-3 factors
    if n_factors_kaiser >= 2 and n_factors_kaiser <= 3:
        n_factors = n_factors_kaiser
    else:
        n_factors = 3  # Default to 3 as per project requirements
    
    print(f"   Selected number of factors: {n_factors}")
    
    # 6. Perform factor analysis
    print("\n6. Performing factor analysis...")
    
    # Use Varimax rotation for better interpretability
    fa = FactorAnalyzer(n_factors=n_factors, rotation='varimax', method='minres')
    fa.fit(data_scaled_df)
    
    # Get factor loadings
    loadings = fa.loadings_
    loadings_df = pd.DataFrame(
        loadings,
        index=data_clean.columns,
        columns=[f'Factor_{i+1}' for i in range(n_factors)]
    )
    
    # Get communalities
    communalities = fa.get_communalities()
    communalities_df = pd.DataFrame({
        'Variable': data_clean.columns,
        'Communality': communalities
    })
    
    # Get variance explained
    variance = fa.get_factor_variance()
    variance_df = pd.DataFrame({
        'Factor': [f'Factor_{i+1}' for i in range(n_factors)],
        'SS Loadings': variance[0],
        'Proportion Var': variance[1],
        'Cumulative Var': variance[2]
    })
    
    # 7. Interpret factors
    print("\n7. Interpreting factors...")
    
    # Define factor names based on loadings
    factor_names = []
    factor_interpretations = []
    
    for i in range(n_factors):
        factor_loadings = loadings_df[f'Factor_{i+1}']
        
        # Find variables with highest loadings on this factor
        high_loadings = factor_loadings[abs(factor_loadings) > 0.4]
        
        if len(high_loadings) > 0:
            # Determine factor type based on variables
            weather_vars = ['temperature', 'humidity', 'rain', 'wind_speed', 'visibility', 'air_pressure']
            traffic_vars = ['vehicle_count', 'average_speed', 'accident_count']
            
            weather_count = sum(1 for var in high_loadings.index if var in weather_vars)
            traffic_count = sum(1 for var in high_loadings.index if var in traffic_vars)
            
            if weather_count > traffic_count:
                if 'rain' in high_loadings.index or 'wind_speed' in high_loadings.index:
                    factor_name = "Weather Severity Factor"
                    interpretation = "Represents adverse weather conditions affecting traffic"
                elif 'temperature' in high_loadings.index:
                    factor_name = "Temperature Impact Factor"
                    interpretation = "Represents temperature-related effects on traffic"
                else:
                    factor_name = "Weather Conditions Factor"
                    interpretation = "Represents general weather impact on traffic"
            elif 'accident_count' in high_loadings.index:
                factor_name = "Accident Risk Factor"
                interpretation = "Represents factors contributing to accident probability"
            elif 'vehicle_count' in high_loadings.index and 'average_speed' in high_loadings.index:
                factor_name = "Traffic Flow Stress Factor"
                interpretation = "Represents traffic volume and speed conditions"
            else:
                factor_name = f"Latent Factor {i+1}"
                interpretation = "Combined influence of multiple variables"
        else:
            factor_name = f"Latent Factor {i+1}"
            interpretation = "Combined influence of multiple variables"
        
        factor_names.append(factor_name)
        factor_interpretations.append(interpretation)
    
    # Rename factors
    loadings_df.columns = factor_names
    variance_df['Factor_Name'] = factor_names
    
    # 8. Create factor loadings heatmap
    print("\n8. Creating visualizations...")
    
    plt.figure(figsize=(12, 8))
    heatmap_data = loadings_df.copy()
    
    # Create heatmap
    sns.heatmap(heatmap_data, annot=True, cmap='RdBu_r', center=0, 
                fmt='.2f', linewidths=0.5, cbar_kws={'label': 'Factor Loading'})
    plt.title('Factor Loadings Heatmap', fontsize=14, pad=20)
    plt.tight_layout()
    
    heatmap_file = 'phase6_results/factor_loadings_heatmap.png'
    plt.savefig(heatmap_file, dpi=300)
    plt.close()
    
    # Create bar chart of highest loadings per factor
    plt.figure(figsize=(14, 6))
    
    for i, factor in enumerate(factor_names):
        plt.subplot(1, n_factors, i + 1)
        
        # Get top 5 variables by absolute loading
        factor_loadings = loadings_df[factor]
        top_vars = factor_loadings.abs().sort_values(ascending=False).head(5).index
        top_loadings = factor_loadings[top_vars]
        
        colors = ['red' if x < 0 else 'blue' for x in top_loadings]
        plt.barh(range(len(top_vars)), top_loadings, color=colors)
        plt.yticks(range(len(top_vars)), top_vars)
        plt.xlabel('Factor Loading')
        plt.title(f'{factor}\n({variance_df.loc[i, "Proportion Var"]:.1%} variance)')
        plt.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    bar_chart_file = 'phase6_results/factor_loadings_bars.png'
    plt.savefig(bar_chart_file, dpi=300)
    plt.close()
    
    # 9. Generate reports
    print("\n9. Generating reports...")
    
    # Create factor loadings table with interpretation
    loadings_table = loadings_df.copy()
    loadings_table['Variable_Type'] = loadings_table.index.map(
        lambda x: 'Weather' if x in ['temperature', 'humidity', 'rain', 'wind_speed', 'visibility', 'air_pressure'] 
        else 'Traffic'
    )
    
    # Sort by variable type
    loadings_table = loadings_table.sort_values('Variable_Type', ascending=False)
    
    # Save loadings table
    loadings_csv = 'phase6_results/factor_loadings.csv'
    loadings_table.to_csv(loadings_csv)
    print(f"   Saved: {loadings_csv}")
    
    # Create interpretation report
    report_file = 'phase6_results/factor_analysis_report.txt'
    with open(report_file, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("FACTOR ANALYSIS REPORT - Weather Impact on Traffic Patterns\n")
        f.write("=" * 70 + "\n\n")
        
        f.write("1. DATA OVERVIEW\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total records analyzed: {len(data_clean)}\n")
        f.write(f"Variables analyzed: {len(data_clean.columns)}\n")
        f.write(f"Bartlett's test p-value: {p_value:.4f}\n")
        f.write(f"KMO measure: {kmo_model:.3f}\n\n")
        
        f.write("2. FACTOR EXTRACTION\n")
        f.write("-" * 40 + "\n")
        f.write(f"Number of factors extracted: {n_factors}\n")
        f.write("Method: Minimum Residuals with Varimax Rotation\n\n")
        
        f.write("3. VARIANCE EXPLAINED\n")
        f.write("-" * 40 + "\n")
        for _, row in variance_df.iterrows():
            f.write(f"{row['Factor_Name']}:\n")
            f.write(f"  SS Loadings: {row['SS Loadings']:.3f}\n")
            f.write(f"  Proportion of Variance: {row['Proportion Var']:.3f}\n")
            f.write(f"  Cumulative Variance: {row['Cumulative Var']:.3f}\n\n")
        
        f.write(f"Total Variance Explained: {variance_df['Cumulative Var'].iloc[-1]:.1%}\n\n")
        
        f.write("4. FACTOR INTERPRETATIONS\n")
        f.write("-" * 40 + "\n")
        for i, (name, interpretation) in enumerate(zip(factor_names, factor_interpretations)):
            f.write(f"\nFactor {i+1}: {name}\n")
            f.write(f"  Interpretation: {interpretation}\n")
            
            # List variables with significant loadings
            factor_loadings = loadings_df[name]
            significant_vars = factor_loadings[abs(factor_loadings) > 0.4]
            
            if len(significant_vars) > 0:
                f.write("  Key Variables:\n")
                for var, loading in significant_vars.items():
                    direction = "positive" if loading > 0 else "negative"
                    f.write(f"    • {var}: {loading:.3f} ({direction} effect)\n")
        
        f.write("\n5. KEY FINDINGS\n")
        f.write("-" * 40 + "\n")
        
        # Find which weather variables have strongest overall impact
        weather_loadings = loadings_table[loadings_table['Variable_Type'] == 'Weather']
        weather_impact = {}
        
        for factor in factor_names:
            weather_factor_loadings = weather_loadings[factor].abs()
            if len(weather_factor_loadings) > 0:
                strongest_var = weather_factor_loadings.idxmax()
                strongest_loading = weather_factor_loadings.max()
                weather_impact[strongest_var] = weather_impact.get(strongest_var, 0) + strongest_loading
        
        if weather_impact:
            f.write("\nWeather Variables with Strongest Impact on Traffic:\n")
            sorted_impact = sorted(weather_impact.items(), key=lambda x: x[1], reverse=True)
            for var, impact in sorted_impact[:3]:
                f.write(f"  1. {var}: Overall impact score = {impact:.3f}\n")
        
        f.write("\n6. RECOMMENDATIONS\n")
        f.write("-" * 40 + "\n")
        f.write("1. Monitor identified key weather variables for traffic prediction\n")
        f.write("2. Consider factor scores for traffic management decisions\n")
        f.write("3. Use factor analysis results to prioritize weather monitoring\n")
        f.write("4. Incorporate factor insights into traffic forecasting models\n")
        
        f.write("\n" + "=" * 70 + "\n")
        f.write("Report generated by Phase 6 Factor Analysis\n")
        f.write("=" * 70 + "\n")
    
    print(f"   Saved: {report_file}")
    
    # 10. Upload results to MinIO Gold
    print("\n10. Uploading results to MinIO Gold layer...")
    
    files_to_upload = [
        (loadings_csv, "factor_loadings.csv"),
        (report_file, "factor_analysis_report.txt"),
        (scree_plot, "scree_plot.png"),
        (heatmap_file, "factor_loadings_heatmap.png"),
        (bar_chart_file, "factor_loadings_bars.png")
    ]
    
    for local_file, object_name in files_to_upload:
        if os.path.exists(local_file):
            client.fput_object("gold", object_name, local_file)
            print(f"   Uploaded: {object_name}")
    
    # 11. Summary
    print("\n" + "=" * 70)
    print("FACTOR ANALYSIS COMPLETE")
    print("=" * 70)
    
    print(f"\nExtracted {n_factors} latent factors:")
    for i, name in enumerate(factor_names):
        var_explained = variance_df.loc[i, 'Proportion Var']
        print(f"  Factor {i+1}: {name} ({var_explained:.1%} variance)")
    
    print("\nTotal variance explained: " + 
          f"{variance_df['Cumulative Var'].iloc[-1]:.1%}")
    
    print("\nKey Weather Variables Affecting Traffic:")
    print("-" * 40)
    
    # Calculate overall impact for each weather variable
    weather_vars = ['temperature', 'humidity', 'rain', 'wind_speed', 'visibility', 'air_pressure']
    for var in weather_vars:
        if var in loadings_df.index:
            var_loadings = loadings_df.loc[var].abs().sum()
            print(f"  {var}: Impact score = {var_loadings:.3f}")
    
    print("\n" + "=" * 70)
    print("Deliverables saved to MinIO Gold layer:")
    print("  • factor_loadings.csv")
    print("  • factor_analysis_report.txt")
    print("  • scree_plot.png")
    print("  • factor_loadings_heatmap.png")
    print("  • factor_loadings_bars.png")
    print("=" * 70)

if __name__ == "__main__":
    # Check if factor_analyzer is installed
    try:
        from factor_analyzer import FactorAnalyzer
    except ImportError:
        print("Error: factor_analyzer package not installed.")
        print("Install with: pip install factor_analyzer")
        exit(1)
    
    main()
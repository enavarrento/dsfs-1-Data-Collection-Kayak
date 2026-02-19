import pandas as pd
import os
import numpy as np

# Define cities to ensure ID consistency
CITIES = [
    "Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen", "Paris", "Amiens",
    "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg", "Colmar", "Eguisheim",
    "Besancon", "Dijon", "Annecy", "Grenoble", "Lyon", "Gorges du Verdon",
    "Bormes les Mimosas", "Cassis", "Marseille", "Aix en Provence", "Avignon",
    "Uzes", "Nimes", "Aigues Mortes", "Saintes Maries de la mer", "Collioure",
    "Carcassonne", "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne",
    "La Rochelle"
]

def calculate_weather_score(row):
    """
    Calculates a custom weather score (0-100) based on user preferences.
    Target: 25°C. 
    Penalties: Hotter is 1.5x worse than Colder.
    Rain and Humidity reduce the score.
    """
    T = row['temp_day']
    target = 25.0
    
    # 1. Temperature Penalty
    if T > target:
        # Too hot: 1.5 penalty per degree
        temp_penalty = (T - target) * 1.5
    else:
        # Too cold: 1.0 penalty per degree
        temp_penalty = (target - T) * 1.0
        
    # 2. Rain Penalty (Heavy penalty)
    # Deduct 5 points per mm to strictly avoid rain.
    rain_penalty = row['rain'] * 5.0 

    # 3. Humidity Penalty
    # If humidity > 60%, deduct 0.3 points per %
    humidity_penalty = max(0, (row['humidity'] - 60) * 0.3)
    
    # Calculate Final Score
    score = 100 - temp_penalty - rain_penalty - humidity_penalty
    
    return score

def process_data_refined():
    print("🔄 Loading Raw Data...")
    
    try:
        df_weather = pd.read_csv("data/raw/weather_data.csv")
        df_hotels = pd.read_csv("data/raw/booking_data_enriched.csv")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return

    # --- STEP 1: Filter Planning Horizon ---
    # "Exclude the first 2 days of data"
    # day_offset starts at 0. So we keep offsets 2, 3, 4, 5, 6
    print("📅 Filtering for Trip Planning (Days 3-7)...")
    df_planning = df_weather[df_weather['day_offset'] >= 2].copy()

    # --- STEP 2: Calculate Daily Score ---
    # We calculate a score for EACH day, then average it for the city
    df_planning['daily_score'] = df_planning.apply(calculate_weather_score, axis=1)

    # --- STEP 3: Aggregate by City ---
    print("🌤️ Aggregating Scores...")
    weather_summary = df_planning.groupby("city").agg({
        "latitude": "first",    
        "longitude": "first",
        "temp_day": "mean",     # Avg temp over the planning period
        "rain": "sum",          # Total rain during the trip
        "daily_score": "mean"   # The crucial "Weather Score"
    }).reset_index()

    weather_summary.rename(columns={
        "temp_day": "avg_temp",
        "rain": "total_rain_mm",
        "daily_score": "weather_score"
    }, inplace=True)

    # --- STEP 4: Merge & Clean ---
    city_id_map = {city: i+1 for i, city in enumerate(CITIES)}
    
    df_master = pd.merge(df_hotels, weather_summary, on="city", how="left")
    df_master['city_id'] = df_master['city'].map(city_id_map)
    
    # Reorder
    cols = ['city_id', 'city', 'hotel_name', 'url', 'score', 'description', 
            'hotel_lat', 'hotel_lon', 'weather_score', 'avg_temp', 'total_rain_mm', 
            'latitude', 'longitude']
    
    # Handle missing cols if hotel file has different structure
    existing_cols = [c for c in cols if c in df_master.columns]
    df_master = df_master[existing_cols]

    # Sort by Weather Score (Best first)
    df_master.sort_values(by=['weather_score', 'score'], ascending=[False, False], inplace=True)

    # Save
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/kayak_master.csv" # Overwrite the master
    df_master.to_csv(output_path, index=False)
    
    print(f"✅ Refined Master Dataset created: {output_path}")
    print(df_master[['city', 'weather_score', 'avg_temp', 'total_rain_mm']].drop_duplicates().head(10))

if __name__ == "__main__":
    process_data_refined()
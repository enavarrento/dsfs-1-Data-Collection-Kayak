import pandas as pd
import os

# ==========================================
# 🎛️ SCORING PARAMETERS
# ==========================================
TARGET_TEMP = 25.0
HOT_PENALTY_MULT = 2.5
COLD_PENALTY_MULT = 1.8
RAIN_PENALTY_MULT = 5.0
HUMID_PENALTY_MULT = 0.3
# ==========================================

def load_cities():
    """Reads the master list of cities from the text file."""
    with open("data/cities.txt", "r", encoding="utf-8") as file:
        return [line.strip() for line in file if line.strip()]

def calculate_climate_index(row):
    """
    Drives the COLOR on the map. Purely based on Temperature.
    0 = Extreme Cold (Blue), 50 = Perfect (Green), 100 = Extreme Hot (Red).
    """
    T = row['temp_day']
    
    if T > TARGET_TEMP:
        index = 50 + ((T - TARGET_TEMP) * HOT_PENALTY_MULT)
    else:
        index = 50 - ((TARGET_TEMP - T) * COLD_PENALTY_MULT)
        
    return max(0, min(100, index))

def calculate_weather_score(row):
    """
    Drives the SIZE on the map. Applies ALL penalties.
    Uses a 2x multiplier for temperature to normalize against the 100-point scale.
    """
    T = row['temp_day']
    
    # 1. Temperature Penalty (Normalized 2x)
    if T > TARGET_TEMP:
        temp_penalty = (T - TARGET_TEMP) * 2 * HOT_PENALTY_MULT
    else:
        temp_penalty = (TARGET_TEMP - T) * 2 * COLD_PENALTY_MULT
        
    # 2. Rain & Humidity Penalties
    rain_penalty = row['rain'] * RAIN_PENALTY_MULT 
    humidity_penalty = max(0, (row['humidity'] - 60) * HUMID_PENALTY_MULT)
    
    # Final absolute score
    final_score = 100 - temp_penalty - rain_penalty - humidity_penalty
    
    return max(0, min(100, final_score))

# ... [Keep the rest of your process_data_refined() function exactly the same] ...

def process_data_refined():
    print("🔄 Loading Raw Data...")
    print("🚀 Starting Data Processing & Merging...")

    # Load central source of truth for cities
    CITIES = load_cities()
    
    # --- STEP 1: Load Data ---
    try:
        df_weather = pd.read_csv("data/raw/weather_data.csv")
        df_hotels = pd.read_csv("data/processed/booking_data_enriched.csv")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return

    # --- STEP 2: Process Weather Data ---
    # "Exclude the first 2 days of data"
    # day_offset starts at 0. So we keep offsets 2, 3, 4, 5, 6
    print("📅 Filtering for Trip Planning (Days 3-7)...")
    df_planning = df_weather[df_weather['day_offset'] >= 2].copy()

    # Calculate both the Index (Color) and the Score (Size)
    df_planning['climate_index'] = df_planning.apply(calculate_climate_index, axis=1)
    df_planning['daily_weather_score'] = df_planning.apply(calculate_weather_score, axis=1)

    #print("🌤️ Aggregating Scores...")
    weather_summary = df_planning.groupby("city").agg({
        "latitude": "first",    
        "longitude": "first",
        "temp_day": "mean",     
        "rain": "sum",          
        "climate_index": "mean",        # The directional index (0-100, 50 is best)
        "daily_weather_score": "mean"   # The absolute quality score (0-100, 100 is best)
    }).reset_index()

    weather_summary.rename(columns={
        "temp_day": "avg_temp",
        "rain": "total_rain_mm",
        "daily_weather_score": "weather_score"
    }, inplace=True)

    # --- STEP 3: Merge & Clean ---
    print("🔗 Merging Hotel and Weather Data...")
    city_id_map = {city: i+1 for i, city in enumerate(CITIES)}
    
    df_master = pd.merge(df_hotels, weather_summary, on="city", how="left")
    df_master['city_id'] = df_master['city'].map(city_id_map)
    
    # Reorder
    cols = [
        'city_id', 'city', 'hotel_name', 'url', 'score', 'description', 
        'hotel_lat', 'hotel_lon', 'weather_score', 'climate_index', 
        'avg_temp', 'total_rain_mm', 'latitude', 'longitude'
    ]
    
    # Handle missing cols if hotel file has different structure
    existing_cols = [c for c in cols if c in df_master.columns]
    df_master = df_master[existing_cols]

    # --- Sort the final dataset ---
    print("🧹 Sorting the final dataset by best weather and best hotels...")
    # 1. Best weather first (Descending)
    # 2. Alphabetical by city (Ascending)
    # 3. Best hotel score first (Descending)
    df_master = df_master.sort_values(
        by=['weather_score', 'city', 'score'], 
        ascending=[False, True, False]
    )

    # --- STEP 4: Save Output ---
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/kayak_master.csv" # Overwrite the master
    df_master.to_csv(output_path, index=False)
    
    print(f"✅ Refined Master Dataset created: {output_path}")
    print(df_master[['city', 'weather_score', 'avg_temp', 'total_rain_mm']].drop_duplicates().head(10))

if __name__ == "__main__":
    process_data_refined()
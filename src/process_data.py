import pandas as pd
import os

# Define cities list in the exact same order to ensure consistent IDs
CITIES = [
    "Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen", "Paris", "Amiens",
    "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg", "Colmar", "Eguisheim",
    "Besancon", "Dijon", "Annecy", "Grenoble", "Lyon", "Gorges du Verdon",
    "Bormes les Mimosas", "Cassis", "Marseille", "Aix en Provence", "Avignon",
    "Uzes", "Nimes", "Aigues Mortes", "Saintes Maries de la mer", "Collioure",
    "Carcassonne", "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne",
    "La Rochelle"
]

def process_data():
    print("🔄 Loading Raw Data...")
    
    # 1. Load Data
    try:
        df_weather = pd.read_csv("data/raw/weather_data.csv")
        df_hotels = pd.read_csv("data/raw/booking_data.csv")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return

    # 2. Create ID Mapping
    # Create a dictionary: {'Mont Saint Michel': 1, 'St Malo': 2, ...}
    city_id_map = {city: i+1 for i, city in enumerate(CITIES)}
    
    # 3. Aggregate Weather (One row per city)
    print("🌤️ Aggregating Weather Data...")
    
    # We group by city to get the averages for the next 7 days
    weather_summary = df_weather.groupby("city").agg({
        "latitude": "first",    
        "longitude": "first",
        "temp_day": "mean",     # Avg temp over 7 days
        "pop": "mean",          # Avg rain probability
        "rain": "sum"           # Total rain volume
    }).reset_index()

    # Rename columns for clarity
    weather_summary.rename(columns={
        "temp_day": "avg_temp",
        "pop": "avg_rain_prob",
        "rain": "total_rain_mm"
    }, inplace=True)

    # 4. Merge with Hotels
    print("🏨 Merging with Hotel Data...")
    
    # Left join: Hotels + Weather info
    df_master = pd.merge(df_hotels, weather_summary, on="city", how="left")
    
    # 5. Add the 'city_id' column
    # Map the city name to its ID
    df_master['city_id'] = df_master['city'].map(city_id_map)
    
    # 6. Reorder Columns
    # It's nice to have IDs first
    cols = ['city_id', 'city', 'hotel_name', 'url', 'score', 'description', 
            'latitude', 'longitude', 'avg_temp', 'avg_rain_prob', 'total_rain_mm']
    
    df_master = df_master[cols]

    # 7. Save
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/kayak_master.csv"
    
    df_master.to_csv(output_path, index=False)
    
    print(f"✅ Master Dataset created with IDs at: {output_path}")
    print(f"📊 Shape: {df_master.shape}")
    print(df_master[['city_id', 'city', 'hotel_name']].head())

if __name__ == "__main__":
    process_data()
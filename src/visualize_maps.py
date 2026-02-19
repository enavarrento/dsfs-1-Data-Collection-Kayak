import pandas as pd
import plotly.express as px
import os
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load Config
load_dotenv()
USER = os.getenv("AWS_RDS_USER")
PASSWORD = os.getenv("AWS_RDS_PASSWORD")
HOST = os.getenv("AWS_RDS_HOST")
PORT = os.getenv("AWS_RDS_PORT")
DB_NAME = os.getenv("AWS_RDS_DB_NAME")

def visualize_maps():
    print("🔌 Connecting to AWS RDS...")
    conn_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
    engine = create_engine(conn_string)

    # Fetch all data
    df = pd.read_sql("SELECT * FROM destinations", engine)
    
    # --- MAP 1: ALL 35 CITIES RANKED ---
    print("🌤️ Generating Map 1: All 35 Cities Ranked...")
    
    city_stats = df.groupby('city').agg({
        'latitude': 'first',
        'longitude': 'first',
        'avg_temp': 'first',
        'total_rain_mm': 'first',
        'weather_score': 'first'
    }).reset_index()

    city_stats = city_stats.sort_values(by='weather_score', ascending=False)

    # Using the modern scatter_map instead of scatter_mapbox
    fig1 = px.scatter_map(
        city_stats, 
        lat="latitude", 
        lon="longitude",
        color="weather_score",
        size="weather_score",
        hover_name="city",
        hover_data={"weather_score": ':.1f', "avg_temp": ':.1f', "total_rain_mm": ':.1f'},
        zoom=5,
        map_style="carto-positron", # Updated argument name for scatter_map
        color_continuous_scale="RdYlGn", 
        title="All 35 Destinations Ranked by Custom Weather Score"
    )
    fig1.show()

    # --- MAP 2: TOP HOTELS IN TOP 5 CITIES ---
    print("\n🏨 Generating Map 2: Top 20 Hotels across the Top 5 Cities...")
    
    top_5_cities = city_stats.head(5)['city'].tolist()
    
    df_top_5 = df[df['city'].isin(top_5_cities)].copy()
    
    # FIX: Convert to numeric, and fill missing scores (NaN) with a baseline of 5.0
    # This prevents Plotly from crashing when sizing the dots!
    df_top_5['score'] = pd.to_numeric(df_top_5['score'], errors='coerce').fillna(5.0)
    
    df_top_5 = df_top_5.sort_values(by=['city', 'score'], ascending=[True, False])
    top_100_hotels = df_top_5.groupby('city').head(20).copy()

    final_lats = []
    final_lons = []

    for index, row in top_100_hotels.iterrows():
        if pd.notna(row.get('hotel_lat')) and pd.notna(row.get('hotel_lon')):
            final_lats.append(row['hotel_lat'])
            final_lons.append(row['hotel_lon'])
        else:
            jitter_lat = row['latitude'] + np.random.uniform(-0.015, 0.015)
            jitter_lon = row['longitude'] + np.random.uniform(-0.015, 0.015)
            final_lats.append(jitter_lat)
            final_lons.append(jitter_lon)

    top_100_hotels['map_lat'] = final_lats
    top_100_hotels['map_lon'] = final_lons

    # Using the modern scatter_map
    fig2 = px.scatter_map(
        top_100_hotels,
        lat="map_lat",
        lon="map_lon",
        hover_name="hotel_name",
        hover_data={"city": True, "score": True, "description": True},
        color="score",
        size="score", 
        zoom=6, 
        map_style="open-street-map", # Updated argument name
        color_continuous_scale="Viridis",
        title="Top Hotels in the Best Destinations (Exact Coordinates)"
    )
    fig2.show()

if __name__ == "__main__":
    visualize_maps()
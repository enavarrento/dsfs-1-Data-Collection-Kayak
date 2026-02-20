import pandas as pd
import plotly.express as px
import os
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import textwrap

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
    print("🌤️ Generating Map 1: All Destinations Ranked...")
    
    city_stats = df.groupby('city').agg({
        'latitude': 'first',
        'longitude': 'first',
        'avg_temp': 'first',
        'total_rain_mm': 'first',
        'climate_index': 'first', 
        'weather_score': 'first'  
    }).reset_index()

    city_stats = city_stats.dropna(subset=['weather_score'])
    
    # Sort strictly by weather score (best at the top)
    city_stats = city_stats.sort_values(by='weather_score', ascending=False)

    # Escala no lineal, pero más contenida
    w = city_stats['weather_score']
    w_norm = (w - w.min()) / (w.max() - w.min() + 1e-6)  # 0–1
    # Non linear scalin, min ~5, máx ~25, highlights more the highest values.
    city_stats['plot_size'] = (5 + 20 * (w_norm ** 1.7)).clip(lower=5)

    # THE HIGHLIGHT LOGIC: Create labels ONLY for the first 5 rows
    city_stats['highlight_label'] = "" 

    top_5_indices = city_stats.head(5).index
    
    for idx in top_5_indices:
        # Add the trophy to the top 5!
        city_stats.loc[idx, 'highlight_label'] = f"🏆 {city_stats.loc[idx, 'city']}"

    fig1 = px.scatter_map(
        city_stats, 
        lat="latitude", 
        lon="longitude",
        color="climate_index",      
        size="plot_size",           
        text="highlight_label",     
        hover_name="city",
        hover_data={
            "climate_index": False, 
            "plot_size": False,
            "highlight_label": False, 
            "weather_score": ':.1f', 
            "avg_temp": ':.1f', 
            "total_rain_mm": ':.1f'
        },
        zoom=5,
        map_style="carto-positron", 
        color_continuous_scale="Turbo", 
        range_color=[0, 100],           
        title="Destinations Ranked by Weather (size)<br>Color Scale: Blue=Cold, Green=Ideal (25°C=index 50), Red=Hot"
    )

    # Apply a single, valid position to the map
    fig1.update_traces(
        textposition='top center', # MUST be a single string!
        textfont=dict(size=14, color='black', weight='bold')
    )
    
    # BUILD THE LEADERBOARD BOX
    top_5_df = city_stats.head(5)
    
    # Create the HTML-formatted text for our custom legend
    leaderboard_text = "<b>🏆 Top 5 Destinations</b><br>"
    for i, (_, row) in enumerate(top_5_df.iterrows(), 1):
        leaderboard_text += f"{i}. {row['city']} (Score: {row['weather_score']:.1f})<br>"
        
    # Add the text box to the bottom right of the map
    fig1.add_annotation(
        text=leaderboard_text,
        align='left',
        showarrow=False,
        xref='paper', yref='paper',  # Position relative to the whole canvas
        x=0.98, y=0.02,              # Bottom right corner (98% right, 2% up)
        xanchor='right', yanchor='bottom',
        bgcolor='rgba(255, 255, 255, 0.85)', # Semi-transparent white background
        bordercolor='black',
        borderwidth=1,
        borderpad=10,
        font=dict(size=13, color='black')
    )
    
    # Show the final map
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

# Truncate to 150 chars, THEN wrap every 50 chars onto a new line
    def format_description(text):
        if not isinstance(text, str):
            return str(text)
        
        # 1. Truncate
        truncated = text[:150] + "..." if len(text) > 150 else text
        
        # 2. Wrap at 50 characters (creates \n newlines)
        wrapped = textwrap.fill(truncated, width=50)
        
        # 3. Replace standard \n with HTML <br> for Plotly
        return wrapped.replace('\n', '<br>')

    top_100_hotels['short_desc'] = top_100_hotels['description'].apply(format_description)

    # Using the modern scatter_map
    fig2 = px.scatter_map(
        top_100_hotels,
        lat="map_lat",
        lon="map_lon",
        hover_name="hotel_name",
        # Update hover_data to use 'short_desc' instead of 'description'
        hover_data={"city": True, "score": True, "short_desc": True, "description": False},
        # Rename 'short_desc' so it looks professional in the tooltip
        labels={"short_desc": "Description"}, 
        color="score",
        size="score", 
        zoom=6, 
        map_style="open-street-map",
        color_continuous_scale="Bluyl",
        title="Top Hotels in the Best Destinations (Exact Coordinates)"
    )

    # BUILD THE CONTEXT BOX (Top 5 Cities)
    # Extract the unique Top 5 cities based on their weather score
    top_5_cities_df = top_100_hotels[['city', 'weather_score']].drop_duplicates().sort_values(by='weather_score', ascending=False).head(5)
    
    context_text = "<b>📍 Showing Hotels For:</b><br><b>Top 5 Destinations</b><br>"
    for i, (_, row) in enumerate(top_5_cities_df.iterrows(), 1):
        context_text += f"{i}. {row['city']} (Score: {row['weather_score']:.1f})<br>"
        
    # Add the text box to the bottom right of Map 2
    fig2.add_annotation(
        text=context_text,
        align='left',
        showarrow=False,
        xref='paper', yref='paper',  
        x=0.98, y=0.02,              
        xanchor='right', yanchor='bottom',
        bgcolor='rgba(255, 255, 255, 0.85)', 
        bordercolor='black',
        borderwidth=1,
        borderpad=10,
        font=dict(size=13, color='black')
    )
    
    # Show the final Map 2
    fig2.show()
if __name__ == "__main__":
    visualize_maps()
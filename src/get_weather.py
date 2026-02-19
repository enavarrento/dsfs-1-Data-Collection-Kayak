import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

# 1. Load environment variables
load_dotenv() 

API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("❌ Error: OPENWEATHER_API_KEY not found. Check your .env file.")

# 2. Define the cities
# Read cities from the master text file
with open("data/cities.txt", "r", encoding="utf-8") as file:
    cities = [line.strip() for line in file if line.strip()]

# 3. Helper: Geocoding
def get_coords(city):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{city}, France",   # <--- adding France to avoid getting a city in another country
        "format": "json", 
        "limit": 1
    }
    headers = {'User-Agent': 'Jedha_Student_Project_Kayak'}
    
    try:
        r = requests.get(url, params=params, headers=headers)
        data = r.json()
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
        return None, None
    except Exception as e:
        print(f"⚠️ Error getting coords for {city}: {e}")
        return None, None

# 4. Helper: Weather (One Call API)
def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,alerts&units=metric&appid={API_KEY}"
    
    try:
        r = requests.get(url)
        data = r.json()
        
        # Check for API errors
        if r.status_code != 200:
            print(f"❌ API Error: {data.get('message', 'Unknown error')}")
            return None

        # Return the list of daily forecasts
        return data.get('daily', [])
    except Exception as e:
        print(f"⚠️ Error getting weather: {e}")
        return None

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("🚀 Starting Data Collection...")
    
    weather_data_list = []

    for city in cities:
        print(f"Processing: {city}...")
        
        # A. Get Coords
        lat, lon = get_coords(city)
        if not lat:
            print(f"   ❌ Could not find coordinates for {city}")
            continue
            
        # Respect Nominatim Rate Limit
        time.sleep(1)

        # B. Get Weather
        daily_forecasts = get_weather(lat, lon)
        
        if daily_forecasts:
            # We want the next 7 days (index 0 to 6)
            for day_offset, day_data in enumerate(daily_forecasts[:7]):
                weather_data_list.append({
                    "city": city,
                    "latitude": lat,
                    "longitude": lon,
                    "day_offset": day_offset, 
                    "date": pd.to_datetime(day_data['dt'], unit='s'),
                    "temp_day": day_data['temp']['day'],
                    "temp_min": day_data['temp']['min'],
                    "temp_max": day_data['temp']['max'],
                    "weather_main": day_data['weather'][0]['main'],
                    "weather_description": day_data['weather'][0]['description'],
                    "pop": day_data.get('pop', 0),    
                    "rain": day_data.get('rain', 0),  
                    "humidity": day_data.get('humidity', 0)
                })
        else:
            print(f"   ❌ No weather data for {city}")

    # 5. Save to CSV
    if weather_data_list:
        df = pd.DataFrame(weather_data_list)
        
        # Ensure directory exists
        os.makedirs("data/raw", exist_ok=True)
        
        output_path = "data/raw/weather_data.csv"
        df.to_csv(output_path, index=False)
        print(f"\n✅ Success! Weather data saved to: {output_path}")
        print(f"📊 Total Rows: {len(df)}")
    else:
        print("\n❌ Failed to collect any data.")
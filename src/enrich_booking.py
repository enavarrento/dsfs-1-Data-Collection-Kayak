import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import InvalidSessionIdException
import time
import re
import os

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def enrich_coordinates_resume():
    file_path = "data/raw/booking_data_enriched.csv"
    
    # 1. Load Data (Resume if exists, otherwise start fresh)
    if os.path.exists(file_path):
        print("🔄 Found existing enriched file. Resuming where we left off...")
        df = pd.read_csv(file_path)
    else:
        print("🔄 No enriched file found. Starting fresh...")
        df = pd.read_csv("data/raw/booking_data.csv")
        df['hotel_lat'] = None
        df['hotel_lon'] = None

    driver = init_driver()
    total = len(df)
    
    print(f"📍 Checking {total} hotels for missing coordinates...\n")

    try:
        for i in range(total):
            # Skip if we already successfully scraped this one
            if pd.notna(df.loc[i, 'hotel_lat']):
                continue
                
            url = df.loc[i, 'url']
            if pd.isna(url):
                continue
                
            try:
                driver.get(url)
                time.sleep(2) # Let page load
                html = driver.page_source
                
                # Search for coordinates
                latlng_match = re.search(r'data-atlas-latlng="([0-9.-]+),([0-9.-]+)"', html)
                lat_match = re.search(r'"latitude"[:\s]+"?([0-9.-]+)"?', html)
                lon_match = re.search(r'"longitude"[:\s]+"?([0-9.-]+)"?', html)

                if latlng_match:
                    df.at[i, 'hotel_lat'] = float(latlng_match.group(1))
                    df.at[i, 'hotel_lon'] = float(latlng_match.group(2))
                    print(f"[{i+1}/{total}] ✅ Found: {df.loc[i, 'hotel_name']}")
                elif lat_match and lon_match:
                    df.at[i, 'hotel_lat'] = float(lat_match.group(1))
                    df.at[i, 'hotel_lon'] = float(lon_match.group(1))
                    print(f"[{i+1}/{total}] ✅ Found: {df.loc[i, 'hotel_name']}")
                else:
                    print(f"[{i+1}/{total}] ⚠️ Not found: {df.loc[i, 'hotel_name']}")
                
                # 2. AUTO-SAVE every 10 hotels
                if i % 10 == 0:
                    df.to_csv(file_path, index=False)
                    
            except InvalidSessionIdException:
                print(f"[{i+1}/{total}] 🔄 Browser crashed! Restarting Chrome...")
                try: driver.quit() 
                except: pass
                driver = init_driver()
                
            except Exception as e:
                print(f"[{i+1}/{total}] ❌ Error: {e}")
                
    finally:
        try: driver.quit() 
        except: pass
        # Final save
        df.to_csv(file_path, index=False)
        print(f"\n🎉 Scraping finished! File safely saved to {file_path}")

if __name__ == "__main__":
    enrich_coordinates_resume()
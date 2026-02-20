import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import InvalidSessionIdException
from selenium.webdriver.common.by import By 
import time
import re
import os

# 🛠️ TEST MODE: Set to a number (e.g., 5) to test only a few lines. Set to None for production.
TEST_LIMIT = None

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def enrich_coordinates_resume():
    os.makedirs("data/processed", exist_ok=True)
    file_path = "data/processed/booking_data_enriched.csv"
    
    # 1. Load Data
    if os.path.exists(file_path):
        print("🔄 Found existing enriched file. Resuming where we left off...")
        df = pd.read_csv(file_path)
    else:
        print("🔄 No enriched file found. Starting fresh...")
        df = pd.read_csv("data/raw/booking_data.csv")
        df['hotel_lat'] = None
        df['hotel_lon'] = None
        
    if 'description' not in df.columns:
        df['description'] = None
        
    df['description'] = df['description'].astype('object')

    driver = init_driver()
    total = TEST_LIMIT if TEST_LIMIT and TEST_LIMIT < len(df) else len(df)
    
    print(f"📍 Checking {total} hotels for missing data...\n")

    try:
        for i in range(total):
            # Skip ONLY if we already successfully scraped BOTH coordinates AND description
            has_coords = pd.notna(df.loc[i, 'hotel_lat'])
            has_desc = pd.notna(df.loc[i, 'description']) and df.loc[i, 'description'] != "Description not available"
            
            if has_coords and has_desc:
                continue
                
            url = df.loc[i, 'url']
            if pd.isna(url):
                continue
                
            try:
                driver.get(url)
                time.sleep(2) # Let page load
                html = driver.page_source
                
                # --- A. Grab Coordinates ---
                latlng_match = re.search(r'data-atlas-latlng="([0-9.-]+),([0-9.-]+)"', html)
                lat_match = re.search(r'"latitude"[:\s]+"?([0-9.-]+)"?', html)
                lon_match = re.search(r'"longitude"[:\s]+"?([0-9.-]+)"?', html)

                if latlng_match:
                    df.at[i, 'hotel_lat'] = float(latlng_match.group(1))
                    df.at[i, 'hotel_lon'] = float(latlng_match.group(2))
                elif lat_match and lon_match:
                    df.at[i, 'hotel_lat'] = float(lat_match.group(1))
                    df.at[i, 'hotel_lon'] = float(lon_match.group(1))
                
                # --- B. 🌟 NEW: Grab Description with Robust Fallbacks ---
                clean_desc = "Description not available"
                
                # Booking.com changes its DOM frequently. We try the 3 most common modern selectors.
                selectors = [
                    (By.CSS_SELECTOR, '[data-testid="property-description"]'),
                    (By.ID, "property_description_content"),
                    (By.CSS_SELECTOR, ".hotel_description_wrapper_exp")
                ]
                
                for by_type, selector_string in selectors:
                    desc_elements = driver.find_elements(by_type, selector_string)
                    if desc_elements and desc_elements[0].text.strip():
                        # Extract, strip, and remove newlines
                        clean_desc = desc_elements[0].text.strip().replace('\n', ' ')
                        break # Found it! Stop looking through the fallback list.
                        
                df.at[i, 'description'] = clean_desc

                # --- C. 🌟 NEW: Enhanced Print Statement ---
                lat = df.loc[i, 'hotel_lat']
                lon = df.loc[i, 'hotel_lon']
                # Create a 40-character snippet of the description for the terminal
                desc_snippet = clean_desc[:40] + "..." if clean_desc != "Description not available" else "❌ Not Found"
                
                print(f"[{i+1}/{total}] ✅ {df.loc[i, 'hotel_name']} | Coords: ({lat}, {lon}) | Desc: {desc_snippet}")
                
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
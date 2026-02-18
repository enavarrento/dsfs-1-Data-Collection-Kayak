import time
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
CITIES = [
    "Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen", "Paris", "Amiens",
    "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg", "Colmar", "Eguisheim",
    "Besancon", "Dijon", "Annecy", "Grenoble", "Lyon", "Gorges du Verdon",
    "Bormes les Mimosas", "Cassis", "Marseille", "Aix en Provence", "Avignon",
    "Uzes", "Nimes", "Aigues Mortes", "Saintes Maries de la mer", "Collioure",
    "Carcassonne", "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne",
    "La Rochelle"
]

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def clean_score(text):
    """Extracts the first number pattern X.X from text"""
    if not text:
        return "N/A"
    # Search for pattern like 8.5 or 9.0
    match = re.search(r'\d+[.,]\d+', text)
    if match:
        return match.group().replace(',', '.')
    return text  # Return original if no number found

def scrape_booking():
    driver = init_driver()
    all_hotels = []

    try:
        for city in CITIES:
            print(f"🔎 Searching for hotels in: {city}...")
            
            # Filter for hotels, 2 adults
            url = f"https://www.booking.com/searchresults.html?ss={city}&group_adults=2"
            driver.get(url)
            
            # Handle Cookies
            try:
                cookie_btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                cookie_btn.click()
            except:
                pass 

            # Wait for hotel cards to definitely load
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@data-testid='property-card']"))
                )
            except:
                print("   ⚠️ Timeout waiting for cards. Retrying...")
                time.sleep(2)

            # Parse
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            cards = soup.find_all("div", {"data-testid": "property-card"})
            
            print(f"   🏠 Found {len(cards)} hotels. Scraping top 20...")

            count = 0
            for card in cards:
                if count >= 20: break
                
                try:
                    # 1. Name
                    name_el = card.find("div", {"data-testid": "title"})
                    name = name_el.get_text(strip=True) if name_el else "Unknown"
                    
                    # 2. URL
                    link_el = card.find("a", {"data-testid": "title-link"})
                    link = link_el['href'] if link_el else None
                    
                    # 3. Score (Try multiple selectors)
                    score_el = card.find("div", {"data-testid": "review-score"})
                    if score_el:
                        raw_score = score_el.get_text(strip=True)
                        score = clean_score(raw_score)
                    else:
                        score = "N/A"

                    # 4. Description (Construct from available info)
                    # Booking list view doesn't have a full "description", so we combine Address + Location Info
                    address_el = card.find("span", {"data-testid": "address"})
                    distance_el = card.find("span", {"data-testid": "distance"})
                    
                    parts = []
                    if address_el: parts.append(address_el.get_text(strip=True))
                    if distance_el: parts.append(distance_el.get_text(strip=True))
                    
                    description = " - ".join(parts) if parts else "No description available"

                    all_hotels.append({
                        "city": city,
                        "hotel_name": name,
                        "url": link,
                        "score": score,
                        "description": description
                    })
                    count += 1
                except Exception as e:
                    print(f"   ⚠️ Error parsing card: {e}")
                    continue
            
            # Polite wait
            time.sleep(1)

    except Exception as e:
        print(f"❌ Critical Error: {e}")
    finally:
        driver.quit()

    # Save
    if all_hotels:
        df = pd.DataFrame(all_hotels)
        output_path = "data/raw/booking_data.csv"
        df.to_csv(output_path, index=False)
        print(f"\n✅ Scraping Complete! Saved {len(df)} hotels to {output_path}")
        print("Sample Data:")
        print(df[['hotel_name', 'score', 'description']].head())
    else:
        print("❌ No data scraped.")

if __name__ == "__main__":
    scrape_booking()
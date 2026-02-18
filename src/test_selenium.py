from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def test_browser():
    print("🚀 Launching Chrome...")
    
    # This automatically downloads the correct driver for your Chrome version
    service = Service(ChromeDriverManager().install())
    
    # Options to make it less detectable
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Uncomment this later to run without seeing the window
    
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        # Go to Booking.com
        url = "https://www.booking.com"
        print(f"🔗 Going to {url}...")
        driver.get(url)
        
        # Wait a bit for page to load
        time.sleep(3)
        
        print(f"✅ Page Title: {driver.title}")
        
        # If we see "Booking.com" in the title, it worked
        if "Booking.com" in driver.title:
            print("🎉 Success! Selenium is working.")
        else:
            print("⚠️ Title looks weird. Might be a captcha.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        
    finally:
        # Always close the browser
        print("👋 Closing browser...")
        driver.quit()

if __name__ == "__main__":
    test_browser()
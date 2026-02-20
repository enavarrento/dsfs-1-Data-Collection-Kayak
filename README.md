# 🗺️ Kayak Travel Dashboard: Best Destinations in France

## 📖 Project Overview
The goal of this project is to recommend the best destinations in France for the upcoming week based on weather conditions, and to provide the top hotel recommendations in those areas. 

This project goes beyond simple data analysis; it is a full **Data Engineering Pipeline**. It automatically scrapes live hotel data, fetches live weather APIs, processes and scores the metrics, stores the raw and processed data in an AWS S3 Data Lake, loads it into an AWS RDS PostgreSQL Data Warehouse, and visualizes it using Plotly.

---

## 🎯 Deliverables Checklist (Jedha Bootcamp)
- [x] **S3 Bucket Storage:** Raw and enriched data (weather + hotels) are pushed to an AWS S3 Data Lake.
- [x] **SQL Database:** The final cleaned dataset is loaded into a live AWS RDS (PostgreSQL) database.
- [x] **Interactive Maps:** Two Plotly maps generated directly from the SQL database:<br><br>
  1. Top Destinations ranked by a custom Weather Score.
  👉 **[Click here to view the Interactive Plotly Maps](https://enavarrento.github.io/dsfs-1-Data-Collection-Kayak/assets/map1/)**
  ![Top Destinations Map](assets/map1/map1_destinations.png)<br><br>
  2. Top 20 Hotels across the Top 5 best weather destinations.<br>
  👉 **[Click here to view the Interactive Plotly Maps](https://enavarrento.github.io/dsfs-1-Data-Collection-Kayak/assets/map2/)**
  ![Top Hotels Map](assets/map2/map2_hotels.png)
  
---

## 🏗️ Architecture & Technologies
1. **Data Collection (Web Scraping & APIs):**
   * **Selenium & BeautifulSoup:** Built a resilient scraper with anti-bot bypasses to extract hotel data from Booking.com.
   * **OpenWeatherMap API:** Fetched 7-day weather forecasts.
   * **Nominatim (OpenStreetMap) API:** Geocoded cities to exact latitude/longitude coordinates.
2. **Data Processing (Pandas):** Handled missing data, normalized text, and engineered a dual-metric scoring system: a bidirectional `climate_index` for temperature visualization (Turbo scale), and an absolute `weather_score` for ranking.
3. **Data Lake (AWS S3):** Stored raw `.json`/`.csv` files and the final enriched `kayak_master.csv`.
4. **Data Warehouse (AWS RDS PostgreSQL):** Loaded the structured data into a relational database using `SQLAlchemy`.
5. **Data Visualization (Plotly):** Queried the RDS database to build interactive maps with collision-proof leaderboards.

---

## 📂 Project Structure

```text
dsfs-1-Data-Collection-Kayak/
│
├── assets/
│   ├── map1/                      # Images,Screenshots of Top Destinations map
│   └── map2/                      # Images, Screenshots of Top 20 Hotels map 
│
├── data/
│   ├── cities.txt                 # Single Source of Truth for destination list
│   ├── raw/                       # Immutable scraped/API data
│   └── processed/                 # Cleaned, enriched, and merged master datasets
│
├── notebooks/
│   ├── Final_Report.ipynb         # 📊 Executive summary & visualizations
│   └── QA_weather.ipynb           # Sandbox for hyperparameter tuning & data QA
│
├── src/                           # ⚙️ Data Pipeline Scripts
│   ├── get_weather.py             # Calls OpenWeatherMap API
│   ├── scrape_booking.py          # Initial scraper for base hotel list & URLs
│   ├── enrich_booking.py          # Selenium scraper for coordinates & descriptions
│   ├── process_data.py            # Merges data & calculates weather scores
│   ├── upload_s3.py               # Pushes processed files to AWS S3 Data Lake
│   ├── etl_sql.py                 # Pushes master dataset to AWS RDS PostgreSQL
│   └── visualize_maps.py          # Generates Plotly maps from the SQL database
│
├── .env.example                   # Template for required API keys and AWS credentials
├── requirements.txt               # Python dependencies
├── environment.yml                # Python and non-Python dependencies
└── README.md                      # Project documentation
```

---

## 🚀 How to Run the Pipeline

### 1. Setup
Clone the repository and install the required packages:

```bash
conda env create -f environment.yml
```
or
```bash
conda create -n myproject python=3.12.3
pip install -r requirements.txt
```

Create a `.env` file based on `.env.example` and add your OpenWeather, AWS, and Database credentials.

### 2. Execute the ETL Pipeline
You can run the pipeline sequentially from the terminal:

```bash
python src/get_weather.py        # 1. Fetch live weather
python src/scrape_booking.py     # 2. Scrape base hotel list and URLs
python src/enrich_booking.py     # 3. Scrape hotel coordinates & descriptions
python src/process_data.py       # 4. Clean, merge, and score
python src/upload_s3.py          # 5. Upload to Data Lake (S3)
python src/etl_sql.py            # 6. Load to Data Warehouse (RDS)
python src/visualize_maps.py     # 7. View the final maps
```

---

## 🧠 Engineering Highlights
* **Single Source of Truth (SSOT):** Eliminated hardcoded arrays by reading targets from `data/cities.txt`, allowing the entire pipeline to scale across Europe seamlessly.
* **Resilient Scraping:** Implemented auto-saving logic and crash recovery in Selenium to prevent data loss during long scraping sessions.
* **Decoupled Metric Normalization:** Separated the visual climate scale from the ranking score to accurately penalize rain/humidity without skewing the hot/cold color mapping.
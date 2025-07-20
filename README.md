# 📈 Cryptocurrency ETL Pipeline

This project implements an **ETL (Extract, Transform, Load) pipeline** to collect, process, and store cryptocurrency data using the public [CoinGecko API](https://www.coingecko.com/en/api). It leverages tools widely used in the data engineering ecosystem, including **Python, Apache Airflow, Pandas, SQLAlchemy**, and **SQLite**.

---

## ⚙️ Technologies & Tools

- **Python 3.13+**
- **Apache Airflow**
- **Pandas**
- **SQLAlchemy**
- **SQLite**
- **CoinGecko API**

---

## 🧠 Key Features

- Fetches cryptocurrency data every 4 hours
- Saves raw API responses as `.json` files with a timestamp-based naming convention
- Converts and stores transformed data in `.parquet` format
- Computes key metrics:
  - `price_delta_4h`: percentage price change since the last timestamp
  - `market_cap_delta_4h`: percentage market cap change since the last timestamp
  - `volume_ratio`: current volume / previous volume
- Loads processed data into a SQLite database
- Primary key: composite of `timestamp` and `name`
- Fully modular and schedulable via Airflow

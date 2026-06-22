# 🛩️ Flight Traffic Lakehouse

Real-time flight traffic data pipeline built with medallion architecture (Bronze-Silver-Gold) on Databricks.

## 📋 Overview

This project implements a complete lakehouse for analyzing real-time flight data from the OpenSky Network API. It uses Delta Lake and follows data engineering best practices.

## 🏗️ Medallion Architecture

### 🥉 Bronze Layer - Raw Ingestion
- **Notebook:** `01_bronze_ingesta_vuelos.py`
- **Purpose:** Ingests raw data from OpenSky API
- **Output:** `workspace.flights_project.bronze_flights` (Delta table with no transformations)

### 🥈 Silver Layer - Cleansing and Quality
- **Notebook:** `02_silver_limpieza_vuelos.py`
- **Purpose:** Cleanses, validates, and enriches data
- **Output:** `workspace.flights_project.silver_flights` (clean and normalized data)
- **Validations:**
  - Removes nulls and duplicates
  - Validates coordinate ranges
  - Converts data types

### 🥇 Gold Layer - Aggregations and Metrics
- **Notebook:** `03_gold_metricas_vuelos.py`
- **Purpose:** Generates business metrics and aggregations
- **Outputs:** 
  - `workspace.flights_project.gold_country_traffic` (country-level metrics)
  - `workspace.flights_project.gold_altitude_bands` (altitude distribution)
- **Metrics:**
  - Total flights by country
  - Average altitude by region
  - Average velocity by airline

## 🚀 Getting Started

### 1. Initial Setup
```python
# Ensure you have access to a Databricks cluster
# Notebooks use serverless compute by default
```

### 2. Run the Pipeline
Execute notebooks in order:

```bash
# 1. Ingest raw data
01_bronze_ingesta_vuelos.py

# 2. Cleanse and validate
02_silver_limpieza_vuelos.py

# 3. Generate metrics
03_gold_metricas_vuelos.py
```

### 3. Query the Tables

```sql
-- View raw data
SELECT * FROM workspace.flights_project.bronze_flights LIMIT 10;

-- View clean data
SELECT * FROM workspace.flights_project.silver_flights LIMIT 10;

-- View country metrics
SELECT * FROM workspace.flights_project.gold_country_traffic;

-- View altitude distribution
SELECT * FROM workspace.flights_project.gold_altitude_bands;
```

## 📊 Dashboard

**[Live Flight Traffic — Lakehouse Pipeline](#dashboard-4188303263603619)**

The project includes an interactive dashboard with visualizations of:
- **Symbol Map**: Real-time flight positions (latitude/longitude) for flights in the air
- **Top Countries**: Bar chart showing top 15 countries by number of flights
- **Altitude Distribution**: Bar chart showing flight count across altitude bands

### Dashboard Data Sources
- `workspace.flights_project.silver_flights` - Cleaned flight data
- `workspace.flights_project.gold_country_traffic` - Country-level metrics
- `workspace.flights_project.gold_altitude_bands` - Altitude band aggregations

## 🛠️ Tech Stack

- **Databricks** - Lakehouse platform
- **Delta Lake** - Transactional storage
- **Apache Spark** - Processing engine
- **Python** - Primary language
- **OpenSky API** - Data source

## 📦 Project Structure

```
flight-traffic-lakehouse/
├── 01_bronze_ingesta_vuelos.py    # Raw ingestion
├── 02_silver_limpieza_vuelos.py   # Cleansing and quality
├── 03_gold_metricas_vuelos.py     # Aggregations
└── README.md                       # Documentation
```

## 🔄 Next Steps

- [ ] Automate pipeline with Databricks Jobs
- [ ] Add data quality alerts
- [ ] Implement real-time streaming
- [ ] Add more data sources (weather, airports)
- [ ] Create ML models for delay prediction

## 📝 Notes

- Data sourced from [OpenSky Network](https://opensky-network.org/)
- Pipeline can be updated manually or via scheduling
- All tables use Delta Lake format

## 👤 Author

Project created as a demonstration of lakehouse architecture on Databricks.

---
**Last updated:** June 2026

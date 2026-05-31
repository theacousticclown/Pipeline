
import os
import logging
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

API_URL = "https://api.open-meteo.com/v1/forecast"
LATITUDE = 13.0827
LONGITUDE = 80.2707

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-sandbox-project-id")
DATASET_ID = "marketing_tech_pipeline"
TABLE_ID = "weather_metrics_hourly"

def fetch_raw_data(lat: float, lon: float) -> dict:
    """Fetches hourly temperature and humidity data from Open-Meteo API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m",
        "forecast_days": 1
    }
    
    logging.info(f"Initiating API request to Open-Meteo for Lat: {lat}, Lon: {lon}")
    try:
        response = requests.get(API_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API Connection Failed: {e}")
        raise

def transform_data(raw_data: dict) -> pd.DataFrame:
    """Flattens nested structures, cleans nulls, and derives analytical values."""
    logging.info("Starting data transformation and cleaning.")
    
    try:
        hourly_data = raw_data.get("hourly", {})
        timestamps = hourly_data.get("time", [])
        temps = hourly_data.get("temperature_2m", [])
        humidity = hourly_data.get("relative_humidity_2m", [])
        

        df = pd.DataFrame({
            "timestamp_raw": timestamps,
            "temperature_celsius": temps,
            "relative_humidity_percentage": humidity
        })
        
        
        df = df.dropna(subset=["timestamp_raw"])
        df["temperature_celsius"] = pd.to_numeric(df["temperature_celsius"]).fillna(method="ffill")
        df["relative_humidity_percentage"] = pd.to_numeric(df["relative_humidity_percentage"]).fillna(method="ffill")
        
        
        df["hourly_timestamp"] = pd.to_datetime(df["timestamp_raw"])
        df["extracted_at"] = datetime.utcnow()
        
        
        df["heat_stress_index"] = df["temperature_celsius"] + (0.1 * df["relative_humidity_percentage"])
        
        
        df = df.drop(columns=["timestamp_raw"])
        
        logging.info(f"Successfully transformed {len(df)} hourly records.")
        return df
        
    except Exception as e:
        logging.error(f"Transformation Pipeline Broken: {e}")
        raise

def load_to_bigquery(df: pd.DataFrame):
    """Loads dataframe into BigQuery Sandbox safely handling schema initialization."""
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        logging.info(f"Dataset {DATASET_ID} not found. Creating it now.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("temperature_celsius", "FLOAT"),
            bigquery.SchemaField("relative_humidity_percentage", "FLOAT"),
            bigquery.SchemaField("hourly_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
            bigquery.SchemaField("heat_stress_index", "FLOAT"),
        ],
        write_disposition="WRITE_APPEND" 
    )
    
    logging.info(f"Loading data into BigQuery table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result() 

    logging.info("Data load job finished successfully.")

if __name__ == "__main__":
    try:
        raw_json = fetch_raw_data(LATITUDE, LONGITUDE)
        processed_df = transform_data(raw_json)
        load_to_bigquery(processed_df)
        logging.info("Pipeline execution completed seamlessly.")
    except Exception as main_err:
        logging.critical(f"Pipeline crashed during execution execution: {main_err}")
        

import os
import sqlite3
import pandas as pd

DB_FILE = "iot_devices.db"
DATA_DIR = "data"

# Define schemas per device type
TABLE_SCHEMAS = {
    "HydroRanger": """
        CREATE TABLE IF NOT EXISTS hydroranger (
            timestamp TEXT,
            device_eui TEXT,
            device_name TEXT,
            device_type TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            payload TEXT,
            metadata TEXT,
            sensors INTEGER,
            water_level_avg REAL,
            water_level_min REAL,
            water_level_max REAL,
            air_temp REAL,
            air_humidity REAL
        )
    """,
    "Droplet": """
        CREATE TABLE IF NOT EXISTS droplet (
            timestamp TEXT,
            device_eui TEXT,
            device_name TEXT,
            device_type TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            payload TEXT,
            air_temp REAL,
            air_pressure REAL,
            air_humidity REAL,
            battery_volt REAL,
            rtc_temp REAL,
            rainfall REAL,
            status INTEGER
        )
    """,
    "Hygro": """
        CREATE TABLE IF NOT EXISTS hygro (
            timestamp TEXT,
            device_eui TEXT,
            device_name TEXT,
            device_type TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            payload TEXT,
            soil_moisture REAL,
            soil_temp REAL,
            soil_conductivity REAL,
            air_temp REAL,
            air_humidity REAL,
            battery_volt REAL,
            status INTEGER
        )
    """,
    "Theta": """
        CREATE TABLE IF NOT EXISTS theta (
            timestamp TEXT,
            device_eui TEXT,
            device_name TEXT,
            device_type TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            payload TEXT,
            metadata TEXT,
            soil_moisture REAL,
            soil_temp REAL,
            soil_conductivity REAL
        )
    """,
    "Echo": """
        CREATE TABLE IF NOT EXISTS echo (
            timestamp TEXT,
            device_eui TEXT,
            device_name TEXT,
            device_type TEXT,
            site_name TEXT,
            latitude REAL,
            longitude REAL,
            payload TEXT,
            water_level REAL,
            air_temp REAL,
            battery_volt REAL,
            water_temp REAL,
            status INTEGER
        )
    """
}

def create_tables(conn):
    """Create tables for all device types."""
    cur = conn.cursor()
    for schema in TABLE_SCHEMAS.values():
        cur.execute(schema)
    conn.commit()

def load_csv_to_db(conn, csv_file):
    """Load a single CSV file into the correct table."""
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"❌ Could not read {csv_file}: {e}")
        return

    if df.empty or "device_type" not in df.columns:
        print(f"⚠️ Skipping {csv_file}, no device_type column.")
        return

    device_type = df["device_type"].iloc[0]
    table = device_type.lower()

    if device_type not in TABLE_SCHEMAS:
        print(f"⚠️ Unknown device type '{device_type}' in {csv_file}, skipping.")
        return

    try:
        df.to_sql(table, conn, if_exists="append", index=False)
        print(f"✅ Loaded {len(df)} rows from {csv_file} into {table}")
    except Exception as e:
        print(f"❌ Failed to load {csv_file} into {table}: {e}")

def main():
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)

    for fname in os.listdir(DATA_DIR):
        if fname.endswith(".csv"):
            csv_path = os.path.join(DATA_DIR, fname)
            load_csv_to_db(conn, csv_path)

    conn.close()
    print("🎉 All CSV files loaded into SQLite.")

if __name__ == "__main__":
    main()

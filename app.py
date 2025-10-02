from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os
import uvicorn
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SEPA IoT Database API", version="1.0.0")

# Add CORS middleware to allow browser requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database path - adjust this to your database location
DATABASE_PATH = "iot_devices.db"

# Device type to table mapping
TABLE_MAPPING = {
    "HydroRanger": "hydroranger",
    "Echo": "echo", 
    "Droplet": "droplet",
    "Hygro": "hygro",
    "Theta": "theta"
}

# Column mappings for each device type
COLUMN_MAPPINGS = {
    "HydroRanger": {
        "sensors": "sensors",
        "levelAvg": "water_level_avg", 
        "levelMin": "water_level_min",
        "levelMax": "water_level_max",
        "airTemp": "air_temp",
        "airHumid": "air_humidity"
    },
    "Echo": {
        "waterLevel": "water_level",
        "airTemp": "air_temp", 
        "battVolt": "battery_volt",
        "waterTemp": "water_temp",
        "status": "status"
    },
    "Droplet": {
        "airTemp": "air_temp",
        "airPress": "air_pressure",
        "airHumid": "air_humidity", 
        "battVolt": "battery_volt",
        "rtcTemp": "rtc_temp",
        "rainfall": "rainfall",
        "status": "status"
    },
    "Hygro": {
        "soilMoisture": "soil_moisture",
        "soilTemp": "soil_temp",
        "soilEC": "soil_conductivity",
        "airTemp": "air_temp",
        "airHumid": "air_humidity",
        "battVolt": "battery_volt", 
        "status": "status"
    },
    "Theta": {
        "soilMoisture": "soil_moisture",
        "soilTemp": "soil_temp", 
        "soilEC": "soil_conductivity"
    }
}

def get_db_connection():
    """Get database connection with row factory"""
    if not os.path.exists(DATABASE_PATH):
        raise HTTPException(status_code=404, detail="Database file not found")
    
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def row_to_dict(row):
    """Convert sqlite3.Row to dictionary"""
    return {key: row[key] for key in row.keys()}

def safe_get_column(row, column_name, default=None):
    """Safely get column value from sqlite3.Row"""
    try:
        return row[column_name] if column_name in row.keys() else default
    except (IndexError, KeyError):
        return default

@app.get("/")
async def root():
    return {"message": "SEPA IoT Database API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/devices/{device_type}")
async def get_devices(device_type: str):
    """Get list of devices for a specific type"""
    if device_type not in TABLE_MAPPING:
        raise HTTPException(status_code=400, detail=f"Invalid device type: {device_type}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        table_name = TABLE_MAPPING[device_type]
        query = f"""
        SELECT DISTINCT device_eui, device_name, site_name, latitude, longitude
        FROM {table_name}
        ORDER BY device_name
        """
        
        cursor.execute(query)
        devices = []
        
        for row in cursor.fetchall():
            devices.append({
                "DeviceEUI": row["device_eui"],
                "DevName": row["device_name"], 
                "SiteName": row["site_name"],
                "Lat": str(row["latitude"]),
                "Lon": str(row["longitude"]),
                "type": device_type
            })
        
        conn.close()
        return devices
        
    except Exception as e:
        logger.error(f"Error getting devices: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/data-bounds/{device_type}/{device_eui}")
async def get_data_bounds(device_type: str, device_eui: str):
    """Get date bounds for a specific device"""
    if device_type not in TABLE_MAPPING:
        raise HTTPException(status_code=400, detail=f"Invalid device type: {device_type}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        table_name = TABLE_MAPPING[device_type]
        query = f"""
        SELECT MIN(timestamp) as min_time, MAX(timestamp) as max_time, COUNT(*) as record_count
        FROM {table_name}
        WHERE device_eui = ?
        """
        
        cursor.execute(query, (device_eui,))
        result = cursor.fetchone()
        
        if result["record_count"] == 0:
            raise HTTPException(status_code=404, detail="No data found for this device")
        
        conn.close()
        
        return {
            "startTS": result["min_time"],
            "endTS": result["max_time"], 
            "recordCount": result["record_count"]
        }
        
    except Exception as e:
        logger.error(f"Error getting data bounds: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/data/{device_type}/{device_eui}")
async def get_device_data(
    device_type: str,
    device_eui: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(50000, description="Maximum number of records (default: 50000)")
):
    """Get data for a specific device with improved error handling and large dataset support"""
    if device_type not in TABLE_MAPPING:
        raise HTTPException(status_code=400, detail=f"Invalid device type: {device_type}")
    
    # Limit maximum records to prevent memory issues
    max_limit = 100000  # Adjust based on your server capacity
    if limit > max_limit:
        limit = max_limit
        logger.warning(f"Limit reduced to maximum allowed: {max_limit}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        table_name = TABLE_MAPPING[device_type]
        
        # Build query with optional date filters
        where_conditions = ["device_eui = ?"]
        params = [device_eui]
        
        if start_date:
            where_conditions.append("timestamp >= ?")
            params.append(f"{start_date} 00:00:00")
            
        if end_date:
            where_conditions.append("timestamp <= ?")
            params.append(f"{end_date} 23:59:59")
        
        query = f"""
        SELECT * FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY timestamp ASC
        LIMIT ?
        """
        params.append(limit)
        
        logger.info(f"Executing query: {query}")
        logger.info(f"With params: {params}")
        
        cursor.execute(query, params)
        
        # Process rows in chunks to handle large datasets
        chunk_size = 1000
        data = []
        column_mapping = COLUMN_MAPPINGS[device_type]
        
        rows_processed = 0
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
                
            for row in rows:
                try:
                    # Convert row to dict first for easier handling
                    row_dict = row_to_dict(row)
                    
                    record = {
                        "timestamp": row_dict.get("timestamp"),
                        "deviceEUI": row_dict.get("device_eui"),
                        "payload": row_dict.get("payload", "")
                    }
                    
                    # Map device-specific columns safely
                    for standard_name, db_column in column_mapping.items():
                        if db_column in row_dict:
                            record[standard_name] = row_dict[db_column]
                        else:
                            logger.debug(f"Column {db_column} not found in row for device {device_eui}")
                    
                    data.append(record)
                    rows_processed += 1
                    
                except Exception as row_error:
                    logger.error(f"Error processing row {rows_processed}: {str(row_error)}")
                    continue  # Skip problematic rows and continue
        
        conn.close()
        
        logger.info(f"Successfully processed {len(data)} records for device {device_eui}")
        
        return {
            "deviceType": device_type,
            "deviceEUI": device_eui,
            "recordCount": len(data),
            "totalProcessed": rows_processed,
            "limitApplied": limit,
            "data": data
        }
        
    except Exception as e:
        logger.error(f"ERROR in get_device_data: {str(e)}")
        logger.error(f"Full traceback:", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/data-chunked/{device_type}/{device_eui}")
async def get_device_data_chunked(
    device_type: str,
    device_eui: str,
    offset: int = Query(0, description="Number of records to skip"),
    limit: int = Query(1000, description="Number of records to return"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get device data in chunks for very large datasets"""
    if device_type not in TABLE_MAPPING:
        raise HTTPException(status_code=400, detail=f"Invalid device type: {device_type}")
    
    # Reasonable chunk size limits
    if limit > 10000:
        limit = 10000
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        table_name = TABLE_MAPPING[device_type]
        
        # Build query with optional date filters
        where_conditions = ["device_eui = ?"]
        params = [device_eui]
        
        if start_date:
            where_conditions.append("timestamp >= ?")
            params.append(f"{start_date} 00:00:00")
            
        if end_date:
            where_conditions.append("timestamp <= ?")
            params.append(f"{end_date} 23:59:59")
        
        # Count total records first
        count_query = f"""
        SELECT COUNT(*) as total FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        """
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()["total"]
        
        # Get the chunk
        query = f"""
        SELECT * FROM {table_name}
        WHERE {' AND '.join(where_conditions)}
        ORDER BY timestamp ASC
        LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Process rows
        column_mapping = COLUMN_MAPPINGS[device_type]
        data = []
        
        for row in rows:
            row_dict = row_to_dict(row)
            
            record = {
                "timestamp": row_dict.get("timestamp"),
                "deviceEUI": row_dict.get("device_eui"),
                "payload": row_dict.get("payload", "")
            }
            
            # Map device-specific columns
            for standard_name, db_column in column_mapping.items():
                if db_column in row_dict:
                    record[standard_name] = row_dict[db_column]
            
            data.append(record)
        
        conn.close()
        
        return {
            "deviceType": device_type,
            "deviceEUI": device_eui,
            "totalRecords": total_records,
            "offset": offset,
            "limit": limit,
            "recordCount": len(data),
            "hasMore": (offset + len(data)) < total_records,
            "data": data
        }
        
    except Exception as e:
        logger.error(f"Error in chunked data fetch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/tables")
async def list_tables():
    """List all available tables in the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]
        
        conn.close()
        return {"tables": tables}
        
    except Exception as e:
        logger.error(f"Error listing tables: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/table-info/{table_name}")
async def get_table_info(table_name: str):
    """Get column information for a specific table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = []
        
        for row in cursor.fetchall():
            columns.append({
                "name": row["name"],
                "type": row["type"],
                "notNull": bool(row["notnull"]),
                "primaryKey": bool(row["pk"])
            })
        
        conn.close()
        return {"table": table_name, "columns": columns}
        
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# Debug endpoint for troubleshooting
@app.get("/debug/{device_type}/{device_eui}")
async def debug_device_data(device_type: str, device_eui: str):
    """Debug endpoint to check data availability and structure"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        table_name = TABLE_MAPPING.get(device_type)
        if not table_name:
            return {"error": f"Invalid device type: {device_type}"}
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone()
        
        if not table_exists:
            return {"error": f"Table {table_name} does not exist"}
        
        # Check table structure
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Check if device exists
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name} WHERE device_eui = ?", (device_eui,))
        device_count = cursor.fetchone()
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} WHERE device_eui = ? LIMIT 3", (device_eui,))
        sample_rows = cursor.fetchall()
        sample_data = [row_to_dict(row) for row in sample_rows]
        
        conn.close()
        
        return {
            "device_type": device_type,
            "device_eui": device_eui,
            "table_exists": bool(table_exists),
            "table_name": table_name,
            "table_columns": [col["name"] for col in columns],
            "device_record_count": device_count["count"] if device_count else 0,
            "sample_data": sample_data,
            "column_mapping": COLUMN_MAPPINGS.get(device_type, {})
        }
        
    except Exception as e:
        logger.error(f"Debug endpoint error: {str(e)}")
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
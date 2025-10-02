import requests
import json
import ast
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import re
from Data_Parser_Examples import (
    parseHydroRangerPayload,
    parseThetaPayload,
    parseECHOdata,
    parseDROPLETdata,
    parseHYGROdata,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Endpoints
BASE_BOUNDS = "https://a8p8m605b5.execute-api.eu-west-2.amazonaws.com/sepa_iot_device_date_bounds"
BASE_FETCH = "https://oujshf1m2h.execute-api.eu-west-2.amazonaws.com/tekh_dataFetch"

# Load device config
DEVICES_CONFIG_FILE = "tekh_devices.json"

def load_devices():
    """Load devices from config file"""
    try:
        with open(DEVICES_CONFIG_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file {DEVICES_CONFIG_FILE} not found!")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing {DEVICES_CONFIG_FILE}: {e}")
        return []

devices = load_devices()

def get_devices_by_type(device_type=None):
    """Get list of device EUIs, optionally filtered by type"""
    if device_type:
        filtered = [d for d in devices if d.get("type") == device_type]
        return [d["DeviceEUI"] for d in filtered]
    return [d["DeviceEUI"] for d in devices]

def get_all_device_types():
    """Get unique device types from config"""
    return list(set(d.get("type") for d in devices if d.get("type")))

def parse_timestamp_robust(timestamp_str):
    """Robust timestamp parsing for SEPA's varying formats"""
    if not timestamp_str:
        return datetime.now()
    
    try:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        pass
    
    try:
        pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2}|Z)'
        match = re.match(pattern, timestamp_str)
        
        if match:
            base_time, microseconds, timezone = match.groups()
            truncated_microseconds = microseconds[:6].ljust(6, '0')
            clean_timestamp = f"{base_time}.{truncated_microseconds}{timezone}"
            return datetime.fromisoformat(clean_timestamp.replace("Z", "+00:00"))
    except ValueError:
        pass
    
    try:
        base_timestamp = timestamp_str.split('.')[0]
        if timestamp_str.endswith('Z'):
            return datetime.fromisoformat(base_timestamp + 'Z').replace(tzinfo=None)
        elif '+' in timestamp_str:
            timezone_part = '+' + timestamp_str.split('+')[1]
            return datetime.fromisoformat(base_timestamp + timezone_part)
    except ValueError:
        pass
    
    try:
        clean_str = re.sub(r'\.?\d*[+-]\d{2}:\d{2}$|Z$', '', timestamp_str)
        return datetime.strptime(clean_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        logger.warning(f"Could not parse timestamp: {timestamp_str}, using current time")
        return datetime.now()

def get_device_info(device_eui):
    """Lookup a device by EUI from loaded devices"""
    for d in devices:
        if d["DeviceEUI"] == device_eui:
            return d
    raise ValueError(f"DeviceEUI {device_eui} not found in {DEVICES_CONFIG_FILE}")

def parse_payload(device_type, payload, empty_distance=None):
    """Route to correct parser with safety checks"""
    try:
        empty_dist_int = None
        if empty_distance is not None:
            if isinstance(empty_distance, str):
                empty_dist_int = int(empty_distance) if empty_distance.strip() else None
            else:
                empty_dist_int = int(empty_distance)

        if device_type == "HydroRanger":
            if len(bytes.fromhex(payload)) == 13:
                return parseHydroRangerPayload(payload, emptyDist=empty_dist_int)
        elif device_type == "Theta":
            return parseThetaPayload(payload)
        elif device_type == "Echo":
            return parseECHOdata(payload, emptyDist=empty_dist_int)
        elif device_type == "Droplet":
            return parseDROPLETdata(payload)
        elif device_type == "Hygro":
            return parseHYGROdata(payload)
    except Exception as e:
        logger.warning(f"Parse error for {device_type}: {e}")
        return {"error": str(e)}

    return {"note": "unparsed/short payload"}

def get_device_bounds_safe(device_eui, device_type):
    """Get device bounds with robust timestamp parsing"""
    try:
        bounds_params = {"device": device_eui}
        if device_type in ["HydroRanger", "Theta"]:
            bounds_params["type"] = device_type
            
        response = requests.get(BASE_BOUNDS, params=bounds_params, timeout=10)
        response.raise_for_status()
        bounds = response.json()
        
        start_ts = parse_timestamp_robust(bounds.get("startTS", ""))
        end_ts = parse_timestamp_robust(bounds.get("endTS", ""))
        
        return start_ts, end_ts
        
    except Exception as e:
        logger.error(f"Error getting bounds for {device_eui}: {e}")
        end_time = datetime.now()
        start_time = end_time - timedelta(days=365)
        return start_time, end_time

def fetch_full_history(device_eui, max_days=None):
    """Retrieve all available history for a given device"""
    info = get_device_info(device_eui)
    device_type = info["type"]
    empty_distance = info.get("EmptyDistance")
    
    logger.info(f"Starting collection for {info['DevName']} ({device_type})")
    logger.info(f"Location: {info['SiteName']}")
    
    start, end = get_device_bounds_safe(device_eui, device_type)
    
    if max_days:
        collection_start = max(start, end - timedelta(days=max_days))
    else:
        collection_start = start
    
    total_days = (end - collection_start).days
    logger.info(f"Collecting data from {collection_start} to {end} ({total_days} days)")

    if total_days <= 0:
        logger.warning(f"No valid date range for {device_eui}")
        return pd.DataFrame()

    all_records = []
    batch_count = 0
    successful_batches = 0

    ts = collection_start
    while ts < end and batch_count < 100:
        batch_count += 1
        
        try:
            fetch_params = {
                "device": device_eui, 
                "timestamp": ts.isoformat().replace("+00:00", "Z")
            }
            if device_type in ["HydroRanger", "Theta"]:
                fetch_params["type"] = device_type
            
            logger.info(f"Batch {batch_count}: Fetching from {ts.strftime('%Y-%m-%d %H:%M:%S')}")
            
            resp = requests.get(BASE_FETCH, params=fetch_params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                logger.info(f"No more data available after {ts}")
                break

            batch_records = 0
            for rec in data:
                try:
                    parsed = parse_payload(device_type, rec["Payload"], empty_distance)
                    
                    rec_out = {
                        "timestamp": rec["TimeStamp"],
                        "device_eui": rec["DevEUI"],
                        "device_name": info["DevName"],
                        "device_type": device_type,
                        "site_name": info["SiteName"],
                        "latitude": float(info["Lat"]),
                        "longitude": float(info["Lon"]),
                        "payload": rec["Payload"],
                    }
                    
                    try:
                        if rec.get("Metadata"):
                            rec_out["metadata"] = ast.literal_eval(rec["Metadata"])
                    except:
                        rec_out["metadata"] = rec.get("Metadata", "")
                    
                    if parsed and not isinstance(parsed, dict):
                        if device_type == "HydroRanger":
                            rec_out.update({
                                "sensors": parsed[0],
                                "water_level_avg": parsed[1],
                                "water_level_min": parsed[2],
                                "water_level_max": parsed[3],
                                "air_temp": parsed[4],
                                "air_humidity": parsed[5]
                            })
                        elif device_type == "Echo":
                            rec_out.update({
                                "water_level": parsed[0],
                                "air_temp": parsed[1],
                                "battery_volt": parsed[2],
                                "water_temp": parsed[3],
                                "status": parsed[4]
                            })
                        elif device_type == "Droplet":
                            rec_out.update({
                                "air_temp": parsed[0],
                                "air_pressure": parsed[1],
                                "air_humidity": parsed[2],
                                "battery_volt": parsed[3],
                                "rtc_temp": parsed[4],
                                "rainfall": parsed[5],
                                "status": parsed[6]
                            })
                        elif device_type == "Hygro":
                            rec_out.update({
                                "soil_moisture": parsed[0],
                                "soil_temp": parsed[1],
                                "soil_conductivity": parsed[2],
                                "air_temp": parsed[3],
                                "air_humidity": parsed[4],
                                "battery_volt": parsed[5],
                                "status": parsed[6]
                            })
                        elif device_type == "Theta":
                            rec_out.update({
                                "soil_moisture": parsed[0],
                                "soil_temp": parsed[1],
                                "soil_conductivity": parsed[2]
                            })
                    
                    all_records.append(rec_out)
                    batch_records += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing record: {e}")
                    continue

            successful_batches += 1
            logger.info(f"Batch {batch_count}: Collected {batch_records} records")
            
            if len(data) > 0:
                try:
                    last_ts = parse_timestamp_robust(data[-1]["TimeStamp"])
                    ts = last_ts + timedelta(seconds=1)
                except:
                    ts += timedelta(days=14)
            else:
                break
            
            time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in batch {batch_count}: {e}")
            ts += timedelta(days=14)
            continue

    logger.info(f"Collection complete: {len(all_records)} total records from {successful_batches} successful batches")
    
    df = pd.DataFrame(all_records)
    if not df.empty:
        df['timestamp'] = df['timestamp'].apply(parse_timestamp_robust)
        df = df.sort_values('timestamp').reset_index(drop=True)
    
    return df

def collect_multiple_devices(device_list, max_days=365):
    """Collect data from multiple devices"""
    logger.info(f"Starting collection for {len(device_list)} devices")
    successful_collections = 0
    
    for i, device_eui in enumerate(device_list, 1):
        try:
            device_info = get_device_info(device_eui)
            logger.info(f"\n[{i}/{len(device_list)}] Processing {device_info['DevName']}")
            
            df = fetch_full_history(device_eui, max_days=max_days)
            
            if df.empty:
                logger.warning(f"No data collected for {device_eui}")
                continue
            
            safe_name = device_info["DevName"].replace(" ", "_").replace("#", "").replace("/", "_")
            filename = f"{safe_name}_{device_eui}_{max_days}days.csv"
            
            df.to_csv(filename, index=False)
            logger.info(f"Saved {len(df)} records to {filename}")
            successful_collections += 1
            
            print(f"\n✅ {device_info['DevName']} Collection Summary:")
            print(f"   📍 Location: {device_info['SiteName']}")
            print(f"   📊 Records: {len(df):,}")
            print(f"   📅 Range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"   💾 File: {filename}")
            
            if device_info["type"] == "HydroRanger" and "water_level_avg" in df.columns:
                water_levels = df["water_level_avg"].dropna()
                if not water_levels.empty:
                    print(f"   🌊 Water Level: Current {water_levels.iloc[-1]:.1f}mm, "
                          f"Avg {water_levels.mean():.1f}mm, Range {water_levels.min():.1f}-{water_levels.max():.1f}mm")
            
        except Exception as e:
            logger.error(f"Failed to collect data for {device_eui}: {e}")
            continue
    
    return successful_collections

def main():
    """Main execution with dynamic device loading"""
    
    print("🌊 SEPA IoT Multi-Device Data Collector")
    print("="*60)
    
    if not devices:
        print(f"❌ No devices loaded from {DEVICES_CONFIG_FILE}")
        print("Please ensure the file exists and contains valid JSON.")
        return
    
    print(f"✅ Loaded {len(devices)} devices from {DEVICES_CONFIG_FILE}")
    
    # Show available device types
    device_types = get_all_device_types()
    print(f"\nAvailable device types: {', '.join(device_types)}")
    
    # Show device counts by type
    for dtype in device_types:
        count = len(get_devices_by_type(dtype))
        print(f"  - {dtype}: {count} devices")
    
    print("\n" + "="*60)
    print("Collection Options:")
    print("1. Collect from ALL devices")
    print("2. Collect from specific device type")
    print("3. Collect from specific device EUI")
    print("="*60)
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    MAX_DAYS = int(input("Enter number of days to collect (default 365): ").strip() or "365")
    
    device_list = []
    
    if choice == "1":
        device_list = [d["DeviceEUI"] for d in devices]
        print(f"\n🔄 Collecting from ALL {len(device_list)} devices...")
        
    elif choice == "2":
        print(f"\nAvailable types: {', '.join(device_types)}")
        selected_type = input("Enter device type: ").strip()
        
        if selected_type not in device_types:
            print(f"❌ Invalid device type: {selected_type}")
            return
        
        device_list = get_devices_by_type(selected_type)
        print(f"\n🔄 Collecting from {len(device_list)} {selected_type} devices...")
        
    elif choice == "3":
        device_eui = input("Enter device EUI: ").strip()
        
        try:
            info = get_device_info(device_eui)
            device_list = [device_eui]
            print(f"\n🔄 Collecting from {info['DevName']}...")
        except ValueError as e:
            print(f"❌ {e}")
            return
    else:
        print("❌ Invalid choice")
        return
    
    if not device_list:
        print("❌ No devices selected")
        return
    
    successful = collect_multiple_devices(device_list, max_days=MAX_DAYS)
    
    print(f"\n🎉 Collection completed!")
    print(f"✅ Successfully collected data from {successful}/{len(device_list)} devices")
    
    if successful > 0:
        print(f"\nGenerated CSV files:")
        import os
        for file in os.listdir('.'):
            if file.endswith('.csv') and any(device in file for device in device_list):
                print(f"  📄 {file}")

if __name__ == "__main__":
    main()
# #!/usr/bin/env python3
# """
# Final Working SEPA IoT Data Collector
# Fixed timestamp parsing throughout the entire pipeline
# """

# import requests
# import json
# import ast
# import pandas as pd
# from datetime import datetime, timedelta
# import time
# import logging
# import re
# from Data_Parser_Examples import (
#     parseHydroRangerPayload,
#     parseThetaPayload,
#     parseECHOdata,
#     parseDROPLETdata,
#     parseHYGROdata,
# )

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Endpoints
# BASE_BOUNDS = "https://a8p8m605b5.execute-api.eu-west-2.amazonaws.com/sepa_iot_device_date_bounds"
# BASE_FETCH = "https://oujshf1m2h.execute-api.eu-west-2.amazonaws.com/tekh_dataFetch"

# # Load device config
# with open("tekh_devices.json") as f:
#     devices = json.load(f)

# def parse_timestamp_robust(timestamp_str):
#     """Robust timestamp parsing for SEPA's varying formats"""
#     if not timestamp_str:
#         return datetime.now()
    
#     try:
#         # First try standard ISO format
#         return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
#     except ValueError:
#         pass
    
#     try:
#         # Handle long microseconds by truncating to 6 digits
#         # Pattern: 2020-05-08T11:44:30.57263208+00:00
#         pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2}|Z)'
#         match = re.match(pattern, timestamp_str)
        
#         if match:
#             base_time, microseconds, timezone = match.groups()
#             # Truncate microseconds to 6 digits
#             truncated_microseconds = microseconds[:6].ljust(6, '0')
#             # Reconstruct timestamp
#             clean_timestamp = f"{base_time}.{truncated_microseconds}{timezone}"
#             return datetime.fromisoformat(clean_timestamp.replace("Z", "+00:00"))
#     except ValueError:
#         pass
    
#     try:
#         # Try without microseconds
#         base_timestamp = timestamp_str.split('.')[0]
#         if timestamp_str.endswith('Z'):
#             return datetime.fromisoformat(base_timestamp + 'Z').replace(tzinfo=None)
#         elif '+' in timestamp_str:
#             timezone_part = '+' + timestamp_str.split('+')[1]
#             return datetime.fromisoformat(base_timestamp + timezone_part)
#     except ValueError:
#         pass
    
#     try:
#         # Last resort: manual parsing without timezone
#         clean_str = re.sub(r'\.?\d*[+-]\d{2}:\d{2}$|Z$', '', timestamp_str)
#         return datetime.strptime(clean_str, '%Y-%m-%dT%H:%M:%S')
#     except ValueError:
#         logger.warning(f"Could not parse timestamp: {timestamp_str}, using current time")
#         return datetime.now()

# def get_device_info(device_eui):
#     """Lookup a device by EUI from tekh_devices.json"""
#     for d in devices:
#         if d["DeviceEUI"] == device_eui:
#             return d
#     raise ValueError(f"DeviceEUI {device_eui} not found in tekh_devices.json")

# def parse_payload(device_type, payload, empty_distance=None):
#     """Route to correct parser with safety checks and proper type conversion"""
#     try:
#         # Convert empty_distance to int if it's a string or exists
#         empty_dist_int = None
#         if empty_distance is not None:
#             if isinstance(empty_distance, str):
#                 empty_dist_int = int(empty_distance) if empty_distance.strip() else None
#             else:
#                 empty_dist_int = int(empty_distance)

#         if device_type == "HydroRanger":
#             if len(bytes.fromhex(payload)) == 13:
#                 return parseHydroRangerPayload(payload, emptyDist=empty_dist_int)
#         elif device_type == "Theta":
#             return parseThetaPayload(payload)
#         elif device_type == "Echo":
#             return parseECHOdata(payload, emptyDist=empty_dist_int)
#         elif device_type == "Droplet":
#             return parseDROPLETdata(payload)
#         elif device_type == "Hygro":
#             return parseHYGROdata(payload)
#     except Exception as e:
#         logger.warning(f"Parse error for {device_type}: {e}")
#         return {"error": str(e)}

#     return {"note": "unparsed/short payload"}

# def get_device_bounds_safe(device_eui, device_type):
#     """Get device bounds with robust timestamp parsing"""
#     try:
#         bounds_params = {"device": device_eui}
#         if device_type in ["HydroRanger", "Theta"]:
#             bounds_params["type"] = device_type
            
#         response = requests.get(BASE_BOUNDS, params=bounds_params, timeout=10)
#         response.raise_for_status()
#         bounds = response.json()
        
#         # Parse timestamps robustly
#         start_ts = parse_timestamp_robust(bounds.get("startTS", ""))
#         end_ts = parse_timestamp_robust(bounds.get("endTS", ""))
        
#         return start_ts, end_ts
        
#     except Exception as e:
#         logger.error(f"Error getting bounds for {device_eui}: {e}")
#         # Return sensible defaults
#         end_time = datetime.now()
#         start_time = end_time - timedelta(days=365)
#         return start_time, end_time

# def fetch_full_history(device_eui, max_days=None):
#     """Retrieve all available history for a given device"""
#     info = get_device_info(device_eui)
#     device_type = info["type"]
#     empty_distance = info.get("EmptyDistance")
    
#     logger.info(f"Starting collection for {info['DevName']} ({device_type})")
#     logger.info(f"Location: {info['SiteName']}")
    
#     # Step 1: Get bounds with safe parsing
#     start, end = get_device_bounds_safe(device_eui, device_type)
    
#     # Limit collection if max_days specified
#     if max_days:
#         collection_start = max(start, end - timedelta(days=max_days))
#     else:
#         collection_start = start
    
#     total_days = (end - collection_start).days
#     logger.info(f"Collecting data from {collection_start} to {end} ({total_days} days)")

#     if total_days <= 0:
#         logger.warning(f"No valid date range for {device_eui}")
#         return pd.DataFrame()

#     all_records = []
#     batch_count = 0
#     successful_batches = 0

#     # Step 2: Iterate in 14-day chunks
#     ts = collection_start
#     while ts < end and batch_count < 100:  # Safety limit
#         batch_count += 1
        
#         try:
#             # Prepare request parameters
#             fetch_params = {
#                 "device": device_eui, 
#                 "timestamp": ts.isoformat().replace("+00:00", "Z")
#             }
#             if device_type in ["HydroRanger", "Theta"]:
#                 fetch_params["type"] = device_type
            
#             logger.info(f"Batch {batch_count}: Fetching from {ts.strftime('%Y-%m-%d %H:%M:%S')}")
            
#             resp = requests.get(BASE_FETCH, params=fetch_params, timeout=30)
#             resp.raise_for_status()
#             data = resp.json()

#             if not data:
#                 logger.info(f"No more data available after {ts}")
#                 break

#             batch_records = 0
#             for rec in data:
#                 try:
#                     parsed = parse_payload(device_type, rec["Payload"], empty_distance)
                    
#                     # Create comprehensive record with standardized fields
#                     rec_out = {
#                         "timestamp": rec["TimeStamp"],
#                         "device_eui": rec["DevEUI"],
#                         "device_name": info["DevName"],
#                         "device_type": device_type,
#                         "site_name": info["SiteName"],
#                         "latitude": float(info["Lat"]),
#                         "longitude": float(info["Lon"]),
#                         "payload": rec["Payload"],
#                     }
                    
#                     # Add metadata safely
#                     try:
#                         if rec.get("Metadata"):
#                             rec_out["metadata"] = ast.literal_eval(rec["Metadata"])
#                     except:
#                         rec_out["metadata"] = rec.get("Metadata", "")
                    
#                     # Add parsed values with device-specific column names
#                     if parsed and not isinstance(parsed, dict):  # Valid parsed data
#                         if device_type == "HydroRanger":
#                             rec_out.update({
#                                 "sensors": parsed[0],
#                                 "water_level_avg": parsed[1],
#                                 "water_level_min": parsed[2],
#                                 "water_level_max": parsed[3],
#                                 "air_temp": parsed[4],
#                                 "air_humidity": parsed[5]
#                             })
#                         elif device_type == "Echo":
#                             rec_out.update({
#                                 "water_level": parsed[0],
#                                 "air_temp": parsed[1],
#                                 "battery_volt": parsed[2],
#                                 "water_temp": parsed[3],
#                                 "status": parsed[4]
#                             })
#                         elif device_type == "Droplet":
#                             rec_out.update({
#                                 "air_temp": parsed[0],
#                                 "air_pressure": parsed[1],
#                                 "air_humidity": parsed[2],
#                                 "battery_volt": parsed[3],
#                                 "rtc_temp": parsed[4],
#                                 "rainfall": parsed[5],
#                                 "status": parsed[6]
#                             })
#                         elif device_type == "Hygro":
#                             rec_out.update({
#                                 "soil_moisture": parsed[0],
#                                 "soil_temp": parsed[1],
#                                 "soil_conductivity": parsed[2],
#                                 "air_temp": parsed[3],
#                                 "air_humidity": parsed[4],
#                                 "battery_volt": parsed[5],
#                                 "status": parsed[6]
#                             })
#                         elif device_type == "Theta":
#                             rec_out.update({
#                                 "soil_moisture": parsed[0],
#                                 "soil_temp": parsed[1],
#                                 "soil_conductivity": parsed[2]
#                             })
                    
#                     all_records.append(rec_out)
#                     batch_records += 1
                    
#                 except Exception as e:
#                     logger.warning(f"Error processing record: {e}")
#                     continue

#             successful_batches += 1
#             logger.info(f"Batch {batch_count}: Collected {batch_records} records")
            
#             if len(data) > 0:
#                 # Advance using last record timestamp + 1 second
#                 try:
#                     last_ts = parse_timestamp_robust(data[-1]["TimeStamp"])
#                     ts = last_ts + timedelta(seconds=1)
#                 except:
#                     # Fallback: advance by 14 days
#                     ts += timedelta(days=14)
#             else:
#                 break
            
#             # Respectful delay
#             time.sleep(0.1)
                
#         except Exception as e:
#             logger.error(f"Error in batch {batch_count}: {e}")
#             # Try advancing by 14 days on error
#             ts += timedelta(days=14)
#             continue

#     logger.info(f"Collection complete: {len(all_records)} total records from {successful_batches} successful batches")
    
#     # Convert to DataFrame and sort by timestamp
#     df = pd.DataFrame(all_records)
#     if not df.empty:
#         # Use robust timestamp parsing for DataFrame conversion
#         df['timestamp'] = df['timestamp'].apply(parse_timestamp_robust)
#         df = df.sort_values('timestamp').reset_index(drop=True)
    
#     return df

# def collect_multiple_devices(device_list, max_days=365):
#     """Collect data from multiple devices"""
#     logger.info(f"Starting collection for {len(device_list)} devices")
#     successful_collections = 0
    
#     for i, device_eui in enumerate(device_list, 1):
#         try:
#             device_info = get_device_info(device_eui)
#             logger.info(f"\n[{i}/{len(device_list)}] Processing {device_info['DevName']}")
            
#             # Collect data
#             df = fetch_full_history(device_eui, max_days=max_days)
            
#             if df.empty:
#                 logger.warning(f"No data collected for {device_eui}")
#                 continue
            
#             # Generate safe filename
#             safe_name = device_info["DevName"].replace(" ", "_").replace("#", "").replace("/", "_")
#             filename = f"{safe_name}_{device_eui}_{max_days}days.csv"
            
#             # Save to CSV
#             df.to_csv(filename, index=False)
#             logger.info(f"Saved {len(df)} records to {filename}")
#             successful_collections += 1
            
#             # Generate summary
#             print(f"\n✅ {device_info['DevName']} Collection Summary:")
#             print(f"   📍 Location: {device_info['SiteName']}")
#             print(f"   📊 Records: {len(df):,}")
#             print(f"   📅 Range: {df['timestamp'].min()} to {df['timestamp'].max()}")
#             print(f"   💾 File: {filename}")
            
#             # Show sample metrics
#             if device_info["type"] == "HydroRanger" and "water_level_avg" in df.columns:
#                 water_levels = df["water_level_avg"].dropna()
#                 if not water_levels.empty:
#                     print(f"   🌊 Water Level: Current {water_levels.iloc[-1]:.1f}mm, "
#                           f"Avg {water_levels.mean():.1f}mm, Range {water_levels.min():.1f}-{water_levels.max():.1f}mm")
            
#         except Exception as e:
#             logger.error(f"Failed to collect data for {device_eui}: {e}")
#             continue
    
#     return successful_collections

# def main():
#     """Main execution with better device selection"""
    
#     print("🌊 SEPA IoT Multi-Device Data Collector (Fixed Version)")
#     print("="*60)
    
#     # Focus on devices most likely to have recent data
#     PRIORITY_DEVICES = [
#         # HydroRanger devices (highest priority - known to have recent data)
#         # "F863663062792909", 
#         # "F863663062798591", 
#         # "F863663062793717", 
#         # "F863663062793469", 
#         # "F863663062793626", 
#         # "F863663062792073", 
#         # "F863663062792156", 
#         # "F863663062797205", 
#         # "F863663062792115", 
#         # "F863663062793691", 
#         # "F863663062797262", 
#         # "F863663062792214", 
#         # "F863663062798732", 
#         # "F863663062779674", 
#         # "F863663062787669", 
#         # "F863663062794509", 
#         # "F863663062793204", 
#         # "F863663062798872", 
#         # "F863663062792198", 
#         # "F863663062793501", 
#         # "F863663062793816", 
#         # "F861275077947444", 
#         # "F861275077961817", 
#         # "F861275077962088", 
#         # "F863663069882554", 
#         # "F863663069837491", 
#         # "70B3D54990566062", 
#         # "70B3D54999389C9B", 
#         # "70B3D54991EB3BA5", 
#         # "70B3D5499B3A9F32", 
#         # "70B3D5499E6F40FA", 
#         # "70B3D549970021CD", 
#         # "70B3D54995431295", 
#         # "70B3D5499C9F22F1", 
#         # "70B3D54995472B60", 
#         # "70B3D5499AFA2DEE", 
#         # "70B3D54995A6F26E", 
#         # "70B3D54994B0C83F", 
#         # "70B3D57ED004558D", 
#         # "70B3D51C200000EB", 
#         # "70B3D51C20000092", 
#         # "70B3D51C20000089", 
#         # "70B3D51C20000094", 
#         # "70B3D51C2000008E", 
#         # "70B3D57ED0045541", 
#         # "70B3D51C20000090",
#         # "70B3D54990566062",
#         # "70B3D54999389C9B",
#         # "70B3D54991EB3BA5",
#         # "70B3D5499B3A9F32",
#     ]
    
#     # Collection settings
#     MAX_DAYS = 365  # Start with last 3 months instead of full year
    
#     print(f"Collecting last {MAX_DAYS} days of data for {len(PRIORITY_DEVICES)} high-priority devices...")
#     print("These devices are most likely to have recent data based on our testing.\n")
    
#     successful = collect_multiple_devices(PRIORITY_DEVICES, max_days=MAX_DAYS)
    
#     print(f"\n🎉 Collection completed!")
#     print(f"✅ Successfully collected data from {successful}/{len(PRIORITY_DEVICES)} devices")
    
#     if successful > 0:
#         print(f"\nGenerated CSV files:")
#         import os
#         for file in os.listdir('.'):
#             if file.endswith('.csv') and any(device in file for device in PRIORITY_DEVICES):
#                 print(f"  📄 {file}")
        
#         print(f"\n💡 Next steps:")
#         print(f"1. Use the multi-device dashboard to view all CSV files together")
#         print(f"2. Upload the CSV files to analyze cross-device patterns")
#         print(f"3. If successful, you can expand to collect from more devices")
#     else:
#         print(f"\n❌ No data was successfully collected")
#         print(f"💡 This might indicate:")
#         print(f"   - API connectivity issues")
#         print(f"   - All devices have old data only")
#         print(f"   - Need to try different device types")

# if __name__ == "__main__":
#     main()
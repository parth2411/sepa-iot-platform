## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     SEPA API (Cloud)                        │
│  https://a8p8m605b5.execute-api.eu-west-2.amazonaws.com    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ 1. Fetch Data
                           │    (data_fetcher.py)
                           │
                  ┌────────▼────────┐
                  │  CSV Files      │
                  │  (data/*.csv)   │
                  │                 │
                  │  - Device1.csv  │
                  │  - Device2.csv  │
                  │  - ...          │
                  └────────┬────────┘
                           │
                           │ 2. Build Database
                           │    (database_builder.py)
                           │
                  ┌────────▼─────────┐
                  │  SQLite DB       │
                  │  iot_devices.db  │
                  │                  │
                  │  Tables:         │
                  │  - hydroranger   │
                  │  - echo          │
                  │  - droplet       │
                  │  - hygro         │
                  │  - theta         │
                  └────────┬─────────┘
                           │
                           │ 3. Serve Data
                           │    (app.py - FastAPI)
                           │
                  ┌────────▼─────────┐
                  │  REST API        │
                  │  localhost:8000  │
                  │                  │
                  │  Endpoints:      │
                  │  - /devices      │
                  │  - /data         │
                  │  - /data-bounds  │
                  └────────┬─────────┘
                           │
                           │ 4. Visualise
                           │    (Dashboard.html)
                           │
                  ┌────────▼─────────┐
                  │  Web Dashboard   │
                  │                  │
                  │  - Charts        │
                  │  - Analytics     │
                  │  - Forecasting   │
                  └──────────────────┘
```

## Detailed Step-by-Step Flow

### Step 1: Data Collection (data_fetcher.py)
```bash
python data_fetcher.py
```

**What happens:**
- Connects to SEPA API
- Fetches raw IoT sensor data
- Parses hex payloads using device-specific parsers
- Saves to CSV files in `data/` directory

**Output:**
```
data/
├── HYDRORANGER_0009_F863663062792909_365days.csv
├── ECHO_1_70B3D54990566062_365days.csv
├── DROPLET_8_70B3D5499E6F40FA_365days.csv
└── ...
```

**CSV Format:**
```csv
timestamp,device_eui,device_name,device_type,site_name,latitude,longitude,water_level_avg,air_temp,...
2025-01-01T00:00:00Z,F863663062792909,HYDRORANGER #0009,HydroRanger,Falkland Burn,56.260429,-3.207358,1234.5,12.3,...
```

---

### Step 2: Database Building (database_builder.py)
```bash
python database_builder.py
```

**What happens:**
- Reads all CSV files from `data/` directory
- Creates SQLite database (`iot_devices.db`)
- Creates device-specific tables
- Inserts data from CSVs into appropriate tables

**Output:**
```
iot_devices.db (SQLite Database)
├── hydroranger table  (HydroRanger devices)
├── echo table         (Echo devices)
├── droplet table      (Droplet devices)
├── hygro table        (Hygro devices)
└── theta table        (Theta devices)
```

---

### Step 3: API Server (app.py)
```bash
python app.py
```

**What happens:**
- FastAPI server starts on `http://127.0.0.1:8000`
- Connects to `iot_devices.db`
- Exposes REST API endpoints
- Handles queries from dashboard

**Available Endpoints:**
```
GET  /                                  # API info
GET  /health                            # Health check
GET  /devices/{device_type}             # List devices
GET  /data-bounds/{device_type}/{eui}   # Date range
GET  /data/{device_type}/{eui}          # Get data
```

---

### Step 4: Web Dashboard (Dashboard.html)
```bash
open Dashboard.html  # Opens in browser
```

**What happens:**
- User opens HTML file in browser
- JavaScript connects to FastAPI server (localhost:8000)
- Fetches device data via API
- Creates interactive charts
- Performs analytics and forecasting

**Dashboard Features:**
- Data source selection (SEPA API or Local Server)
- Device selection
- Date range filtering
- Real-time visualisation
- Analytics and forecasting

---

## Alternative Flow: Direct from SEPA API

The dashboard also supports fetching data directly from SEPA API:

```
┌─────────────────┐
│   SEPA API      │
└────────┬────────┘
         │
         │ Direct fetch using Live sepa api
         │
┌────────▼────────┐
│  Dashboard.html │
│                 │
│  - Select "SEPA │
│    API" source  │
│  - Fetch live   │
│  - Visualise    │
└─────────────────┘
```

**When to use:**
- ✅ Need latest real-time data
- ✅ Quick analysis without setup
- ✅ Testing specific devices

**When NOT to use:**
- ❌ Large historical datasets (slow)
- ❌ Repeated analysis (redundant API calls)
- ❌ Cross-device comparisons

---

## Current Flow Summary

| Step | Tool | Input | Output | Purpose |
|------|------|-------|--------|---------|
| 1 | `data_fetcher.py` | SEPA API | CSV files | Collect historical data |
| 2 | `database_builder.py` | CSV files | SQLite DB | Organise and index data |
| 3 | `app.py` | SQLite DB | REST API | Serve data efficiently |
| 4 | `Dashboard.html` | REST API | Visualisations | Analyse and explore |

---

## Why This Architecture?

### Advantages:
1. **Data Persistence**: CSV and DB store data locally (no repeated API calls)
2. **Performance**: Database queries are fast for large datasets
3. **Offline Analysis**: Can work without internet after initial fetch
4. **Flexibility**: Can re-analyse data without re-fetching
5. **Scalability**: Database handles millions of records efficiently

### When You Need Each Step:

**Skip CSV → DB if:**
- Only doing one-time analysis
- Working with small datasets
- Need real-time data only

**Use Full Pipeline if:**
- ✅ Analysing multiple devices
- ✅ Working with large historical datasets
- ✅ Need repeated analysis
- ✅ Building reports or dashboards
- ✅ Sharing data with the team

---

## Optimisation Options

### Option 1: Direct API → DB (Skip CSV)
Modify `data_fetcher.py` to write directly to database:
```python
# Instead of: df.to_csv(filename)
# Use: df.to_sql('hydroranger', conn, if_exists='append')
```

### Option 2: Stream Processing
Process data as it arrives instead of batch:
```python
for batch in fetch_batches():
    parse_and_store(batch)  # Immediate processing
```

### Option 3: Hybrid Approach
- Use local server for historical analysis
- Use SEPA API for latest real-time data
- Dashboard combines both sources

---

## Complete Workflow Example

```bash
# Day 1: Initial Setup
python data_fetcher.py          # Collect 365 days of data
python database_builder.py      # Build database
python app.py                   # Start server

# Use dashboard for analysis
open Dashboard.html

# Day 30: Update with new data
python data_fetcher.py          # Fetch last 30 days
python database_builder.py      # Update database
# Server automatically sees new data

# Dashboard shows updated information
```

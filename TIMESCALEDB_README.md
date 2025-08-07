# TimescaleDB Integration

This document describes the TimescaleDB integration added to histdatacom-tools, which allows you to download and import forex data directly into a TimescaleDB database.

## Overview

TimescaleDB is a time-series database built on PostgreSQL that's optimized for time-series data. The histdatacom-tools now supports importing forex data directly into TimescaleDB alongside the existing InfluxDB support.

## Features

- **Direct Import**: Download and import forex data directly to TimescaleDB without intermediate files
- **Hypertable Support**: Automatically creates hypertables for optimal time-series performance
- **Batch Processing**: Efficient batch insertion with configurable batch sizes
- **Multiple Timeframes**: Supports both tick data (T) and minute bar data (M1)
- **Automatic Schema**: Creates tables and indexes automatically
- **Concurrent Processing**: Multi-threaded processing for optimal performance

## Prerequisites

1. **TimescaleDB Server**: A running TimescaleDB instance
2. **Database URL**: Connection string in the `DATABASE_URL` environment variable
3. **Python Dependencies**: `psycopg2-binary` (automatically installed)

## Setup

### 1. TimescaleDB Server

Ensure you have a TimescaleDB server running. You can use:
- Local installation
- Docker container
- Cloud service (Timescale Cloud, AWS RDS, etc.)

### 2. Environment Variable

Set the `DATABASE_URL` environment variable with your connection string:

```bash
export DATABASE_URL="postgresql://username:password@hostname:port/database"
```

Example formats:
```bash
# Local database
export DATABASE_URL="postgresql://postgres:password@localhost:5432/forex_data"

# Remote database
export DATABASE_URL="postgresql://user:pass@timescale.example.com:5432/forex"

# With SSL
export DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require"
```

### 3. Database Schema

The tool automatically creates the required schema using the `tick_data` table:

```sql
-- Tick data table (optimized for forex tick data)
CREATE TABLE IF NOT EXISTS tick_data (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    bid DECIMAL(10, 5) NOT NULL,
    ask DECIMAL(10, 5) NOT NULL,
    volume DECIMAL(15, 2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('tick_data', 'timestamp', if_not_exists => TRUE);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tick_data_symbol_timestamp ON tick_data (symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tick_data_timestamp ON tick_data (timestamp DESC);
```

## Usage

### Command Line Interface

Use the `-T` or `--import_to_timescaledb` flag to enable TimescaleDB import:

```bash
# Basic usage - import EURUSD M1 data for 2023
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s 2023-01 -e 2023-12 -T

# Import tick data
histdatacom -p xauusd -f ascii -t tick-data-quotes -s 2023-01 -e 2025-07 -T

# Multiple pairs
histdatacom -p eurusd usdjpy gbpusd -f ascii -t 1-minute-bar-quotes -s 2023-01 -T

# Custom batch size
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s 2023-01 -T -b 10000

# Delete files after import
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s 2023-01 -T --delete_after_timescale
```

### Python API

```python
import os
from histdatacom import Options
import histdatacom

# Set database URL
os.environ['DATABASE_URL'] = 'postgresql://user:pass@localhost:5432/forex'

# Configure options
options = Options()
options.pairs = {'eurusd'}
options.formats = {'ascii'}
options.timeframes = {'1-minute-bar-quotes'}
options.start_yearmonth = '2023-01'
options.end_yearmonth = '2023-01'
options.import_to_timescaledb = True

# Run import
histdatacom.main(options)
```

## Data Structure

### Minute Bar Data (M1)
For 1-minute bar data, the following fields are populated:
- `timestamp`: Bar timestamp
- `pair`: Currency pair (e.g., 'EURUSD')
- `source`: Always 'histdata.com'
- `format`: Data format (e.g., 'ascii')
- `timeframe`: 'M1'
- `open_bid`: Opening bid price
- `high_bid`: Highest bid price
- `low_bid`: Lowest bid price
- `close_bid`: Closing bid price
- `volume`: Volume (if available)

### Tick Data (T)
For tick data, the following fields are populated:
- `timestamp`: Tick timestamp
- `pair`: Currency pair
- `source`: Always 'histdata.com'
- `format`: Data format
- `timeframe`: 'T'
- `bid_quote`: Bid price
- `ask_quote`: Ask price
- `volume`: Volume (if available)

## Configuration Options

### CLI Options

| Option | Description |
|--------|-------------|
| `-T, --import_to_timescaledb` | Enable TimescaleDB import |
| `--delete_after_timescale` | Delete data files after successful import |
| `-b, --batch_size` | Batch size for database inserts (default: 5000) |

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL/TimescaleDB connection string | Yes |

## Performance Tuning

### Batch Size
Adjust the batch size based on your system:
```bash
# Smaller batches for limited memory
histdatacom -T -b 1000 ...

# Larger batches for better performance
histdatacom -T -b 10000 ...
```

### Concurrent Processing
Control CPU utilization:
```bash
# Low CPU usage
histdatacom -T -c low ...

# High CPU usage (uses all cores)
histdatacom -T -c high ...

# Specific percentage
histdatacom -T -c 150 ...
```

## Querying Data

Once imported, you can query the data using standard SQL:

```sql
-- Get latest EURUSD prices
SELECT timestamp, open_bid, high_bid, low_bid, close_bid
FROM forex_data
WHERE pair = 'EURUSD' AND timeframe = 'M1'
ORDER BY timestamp DESC
LIMIT 100;

-- Get hourly OHLC data
SELECT 
    time_bucket('1 hour', timestamp) as hour,
    first(open_bid, timestamp) as open,
    max(high_bid) as high,
    min(low_bid) as low,
    last(close_bid, timestamp) as close
FROM forex_data
WHERE pair = 'EURUSD' AND timeframe = 'M1'
GROUP BY hour
ORDER BY hour;

-- Get tick data for a specific time range
SELECT timestamp, bid_quote, ask_quote
FROM forex_data
WHERE pair = 'EURUSD' 
  AND timeframe = 'T'
  AND timestamp BETWEEN '2023-01-01' AND '2023-01-02'
ORDER BY timestamp;
```

## Troubleshooting

### Connection Issues
- Verify `DATABASE_URL` is correctly set
- Check database server is running and accessible
- Ensure user has CREATE TABLE permissions

### Performance Issues
- Reduce batch size if running out of memory
- Increase batch size for better throughput
- Monitor database connection limits

### Schema Issues
- Ensure TimescaleDB extension is installed: `CREATE EXTENSION IF NOT EXISTS timescaledb;`
- Check user has necessary permissions to create tables and indexes

### Data Type Issues
- **Timestamp Conversion**: The tool automatically converts millisecond timestamps from jay files to UTC datetime objects compatible with PostgreSQL's `TIMESTAMPTZ` type
- **Decimal Precision**: Price data is stored as `DECIMAL` type for precise financial calculations
- **NULL Handling**: Fields not applicable to certain timeframes (e.g., OHLC for tick data) are stored as NULL

## Comparison with InfluxDB

| Feature | TimescaleDB | InfluxDB |
|---------|-------------|----------|
| Query Language | SQL | InfluxQL/Flux |
| Data Model | Relational | Time-series |
| Ecosystem | PostgreSQL | InfluxDB |
| Scalability | Horizontal | Horizontal |
| ACID Compliance | Yes | Limited |
| Learning Curve | Lower (SQL) | Higher |

## Migration from InfluxDB

If you're migrating from InfluxDB, you can run both imports simultaneously:

```bash
# Import to both databases
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s 2023-01 -I -T
```

The data structure is similar but adapted to each database's strengths.
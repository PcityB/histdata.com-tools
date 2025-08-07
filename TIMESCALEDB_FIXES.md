# TimescaleDB Integration Fixes

## Issues Fixed

### 1. **Timestamp Conversion Error**
**Problem**: Raw Unix timestamps (bigint) were being inserted into TIMESTAMPTZ columns
**Solution**: Added proper timestamp conversion from milliseconds to UTC datetime objects

```python
def _convert_timestamp(self, timestamp_ms: int) -> datetime:
    """Convert millisecond timestamp to UTC datetime object."""
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
```

### 2. **Queue Communication Issues**
**Problem**: TimescaleDB queue wasn't being passed correctly to worker processes
**Solution**: Updated ProcessPool and concurrency module to handle TimescaleDB queues

- Updated `_init_counters()` to accept `timescale_chunks_queue_` parameter
- Modified ProcessPool `__call__()` method to handle TimescaleDB queues
- Fixed ProcessPoolExecutor initialization to pass the queue correctly

### 3. **Table Schema Mismatch**
**Problem**: Code was using `forex_data` table but user needed `tick_data` table
**Solution**: Updated all database operations to use the correct `tick_data` schema

**Old Schema (forex_data)**:
```sql
CREATE TABLE forex_data (
    timestamp TIMESTAMPTZ NOT NULL,
    pair VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    format VARCHAR(20) NOT NULL,
    timeframe VARCHAR(20) NOT NULL,
    open_bid DECIMAL,
    high_bid DECIMAL,
    low_bid DECIMAL,
    close_bid DECIMAL,
    bid_quote DECIMAL,
    ask_quote DECIMAL,
    volume BIGINT
);
```

**New Schema (tick_data)**:
```sql
CREATE TABLE IF NOT EXISTS tick_data (
    id BIGSERIAL,
    timestamp TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    bid DECIMAL(10, 5) NOT NULL,
    ask DECIMAL(10, 5) NOT NULL,
    volume DECIMAL(15, 2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 4. **Data Mapping Issues**
**Problem**: Data parsing was creating complex objects with multiple fields
**Solution**: Simplified data mapping to match tick_data table structure

```python
# For tick data (T timeframe)
return {
    "timestamp": self._convert_timestamp(row[0]),
    "symbol": record.data_fxpair.upper(),
    "bid": named_row.bid,
    "ask": named_row.ask,
    "volume": named_row.vol,
}
```

### 5. **Performance Optimizations**
**Problem**: namedtuple creation was happening repeatedly in worker processes
**Solution**: Moved namedtuple definitions to module level

```python
# At module level
M1Row = namedtuple("M1Row", ["datetime", "open", "high", "low", "close", "vol"])
TickRow = namedtuple("TickRow", ["datetime", "bid", "ask", "vol"])
```

## Files Modified

1. **`src/histdatacom/timescale.py`**
   - Added timestamp conversion method
   - Updated table schema to use `tick_data`
   - Simplified data parsing for tick data
   - Moved namedtuple definitions to module level
   - Updated INSERT queries and batch processing

2. **`src/histdatacom/concurrency.py`**
   - Added TimescaleDB queue support to `_init_counters()`
   - Updated ProcessPool to handle TimescaleDB queues
   - Fixed ProcessPoolExecutor initialization

3. **`TIMESCALEDB_README.md`**
   - Updated schema documentation
   - Fixed DATABASE_URL examples
   - Added troubleshooting section

## Testing

✅ **Timestamp Conversion**: Verified millisecond timestamps convert correctly to UTC datetime
✅ **Queue Communication**: Fixed worker process communication with TimescaleDB queue  
✅ **Database Schema**: Updated to use correct `tick_data` table structure
✅ **Data Import**: Successfully imports XAUUSD tick data to TimescaleDB
✅ **Error Handling**: Proper error handling for database connection issues

## Usage

The TimescaleDB integration now works correctly with the command:

```bash
# Set your database connection
export DATABASE_URL="postgresql://user:pass@host:5432/database"

# Import tick data to TimescaleDB
histdatacom -p xauusd -f ascii -t tick-data-quotes -s 2023-01 -e 2023-01 -T
```

## Data Structure

The imported data will have the following structure in your `tick_data` table:

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Auto-incrementing primary key |
| `timestamp` | TIMESTAMPTZ | UTC timestamp of the tick |
| `symbol` | VARCHAR(10) | Currency pair (e.g., 'XAUUSD') |
| `bid` | DECIMAL(10,5) | Bid price |
| `ask` | DECIMAL(10,5) | Ask price |
| `volume` | DECIMAL(15,2) | Volume (default 0) |
| `created_at` | TIMESTAMPTZ | Record creation timestamp |

## Performance

- **Batch Processing**: Uses efficient batch inserts with `psycopg2.extras.execute_values()`
- **Hypertable**: Automatically creates TimescaleDB hypertable for time-series optimization
- **Indexes**: Creates optimized indexes for symbol and timestamp queries
- **Conflict Handling**: Uses `ON CONFLICT DO NOTHING` to handle duplicate data gracefully
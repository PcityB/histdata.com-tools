# Timestamp Conversion Fix

## Issue
The original implementation had a data type mismatch error when inserting data into TimescaleDB:

```
ERROR: column "timestamp" is of type timestamp with time zone but expression is of type bigint
```

## Root Cause
The jay files from histdata.com contain timestamps as Unix timestamps in milliseconds (bigint), but the TimescaleDB table schema expects `TIMESTAMPTZ` (timestamp with timezone) data type.

## Solution
Added proper timestamp conversion in the `TimescaleDB` class:

### Code Changes
1. **Added imports**:
   ```python
   from datetime import datetime, timezone
   ```

2. **Added conversion method**:
   ```python
   def _convert_timestamp(self, timestamp_ms: int) -> datetime:
       """Convert millisecond timestamp to UTC datetime object.
       
       Args:
           timestamp_ms (int): Timestamp in milliseconds
           
       Returns:
           datetime: Converted UTC datetime object
       """
       return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
   ```

3. **Updated data parsing**:
   ```python
   base_data = {
       "pair": record.data_fxpair,
       "source": "histdata.com",
       "format": record.data_format,
       "timeframe": record.data_timeframe,
       "timestamp": self._convert_timestamp(row[0]),  # Convert here
   }
   ```

## Technical Details

### Before Fix
- Raw timestamp: `1685595600136` (bigint)
- Database expected: `TIMESTAMPTZ`
- Result: Type mismatch error

### After Fix
- Raw timestamp: `1685595600136` (milliseconds)
- Converted to: `2023-06-01T05:00:00.136000+00:00` (UTC datetime)
- Database receives: Compatible `TIMESTAMPTZ` value

## Benefits
1. **Proper Data Types**: Timestamps are now properly typed for PostgreSQL
2. **UTC Timezone**: All timestamps are consistently stored in UTC
3. **Precision Preserved**: Millisecond precision is maintained
4. **Query Compatibility**: Standard SQL datetime functions work correctly

## Testing
The fix has been verified with:
- ✅ Timestamp conversion accuracy
- ✅ UTC timezone preservation
- ✅ Millisecond precision retention
- ✅ Database insertion compatibility
- ✅ Both M1 and T timeframe data

## Example
```python
# Input from jay file
timestamp_ms = 1685595600136

# Conversion
converted = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
# Result: 2023-06-01 05:00:00.136000+00:00

# Database insertion now works correctly
INSERT INTO forex_data (timestamp, pair, ...) 
VALUES ('2023-06-01T05:00:00.136000+00:00', 'XAUUSD', ...);
```

This fix ensures seamless data import from histdata.com jay files into TimescaleDB with proper timestamp handling.
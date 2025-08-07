#!/usr/bin/env python3
"""
Example script demonstrating TimescaleDB integration with histdatacom-tools.

This script shows how to:
1. Set up the database connection
2. Import forex data to TimescaleDB
3. Query the imported data

Prerequisites:
- TimescaleDB server running
- DATABASE_URL environment variable set
- histdatacom-tools installed with TimescaleDB support
"""

import os
import sys
from datetime import datetime

# Example database URL (replace with your actual connection string)
# os.environ['DATABASE_URL'] = 'postgresql://username:password@localhost:5432/forex_data'

def check_database_url():
    """Check if DATABASE_URL is set."""
    if 'DATABASE_URL' not in os.environ:
        print("❌ DATABASE_URL environment variable not set!")
        print("Please set it like this:")
        print('export DATABASE_URL="postgresql://username:password@localhost:5432/database"')
        return False
    
    print(f"✓ DATABASE_URL is set: {os.environ['DATABASE_URL'][:50]}...")
    return True

def test_database_connection():
    """Test connection to TimescaleDB."""
    try:
        import psycopg2
        from urllib.parse import urlparse
        
        parsed_url = urlparse(os.environ['DATABASE_URL'])
        connection_params = {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 5432,
            "database": parsed_url.path.lstrip('/'),
            "user": parsed_url.username,
            "password": parsed_url.password,
        }
        
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                print(f"✓ Connected to database: {version[:50]}...")
                
                # Check if TimescaleDB extension is available
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
                    );
                """)
                has_timescale = cursor.fetchone()[0]
                
                if has_timescale:
                    print("✓ TimescaleDB extension is installed")
                else:
                    print("⚠️  TimescaleDB extension not found - will work as regular PostgreSQL")
                
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def import_sample_data():
    """Import a small sample of forex data."""
    try:
        from histdatacom import Options
        import histdatacom
        
        print("\n📥 Importing sample forex data...")
        print("This will download and import EURUSD 1-minute data for January 2023")
        print("(This may take a few minutes for the first run)")
        
        # Configure options for a small sample
        options = Options()
        options.pairs = {'eurusd'}  # Just EURUSD
        options.formats = {'ascii'}  # ASCII format required for database import
        options.timeframes = {'1-minute-bar-quotes'}  # 1-minute bars
        options.start_yearmonth = '2023-01'  # January 2023
        options.end_yearmonth = '2023-01'    # Just January
        options.import_to_timescaledb = True  # Enable TimescaleDB import
        options.delete_after_timescale = True  # Clean up files after import
        
        # Run the import
        result = histdatacom.main(options)
        print("✓ Data import completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Data import failed: {e}")
        return False

def query_sample_data():
    """Query the imported data to verify it worked."""
    try:
        import psycopg2
        from urllib.parse import urlparse
        
        parsed_url = urlparse(os.environ['DATABASE_URL'])
        connection_params = {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 5432,
            "database": parsed_url.path.lstrip('/'),
            "user": parsed_url.username,
            "password": parsed_url.password,
        }
        
        with psycopg2.connect(**connection_params) as conn:
            with conn.cursor() as cursor:
                # Check if data exists
                cursor.execute("""
                    SELECT COUNT(*) FROM forex_data 
                    WHERE pair = 'EURUSD' AND timeframe = 'M1'
                """)
                count = cursor.fetchone()[0]
                print(f"\n📊 Found {count:,} EURUSD M1 records in database")
                
                if count > 0:
                    # Get sample data
                    cursor.execute("""
                        SELECT timestamp, open_bid, high_bid, low_bid, close_bid
                        FROM forex_data
                        WHERE pair = 'EURUSD' AND timeframe = 'M1'
                        ORDER BY timestamp DESC
                        LIMIT 5
                    """)
                    
                    print("\n📈 Latest 5 records:")
                    print("Timestamp                | Open    | High    | Low     | Close")
                    print("-" * 65)
                    
                    for row in cursor.fetchall():
                        timestamp, open_bid, high_bid, low_bid, close_bid = row
                        print(f"{timestamp} | {open_bid:7.5f} | {high_bid:7.5f} | {low_bid:7.5f} | {close_bid:7.5f}")
                    
                    # Get date range
                    cursor.execute("""
                        SELECT MIN(timestamp), MAX(timestamp)
                        FROM forex_data
                        WHERE pair = 'EURUSD' AND timeframe = 'M1'
                    """)
                    min_date, max_date = cursor.fetchone()
                    print(f"\n📅 Data range: {min_date} to {max_date}")
                    
        return True
        
    except Exception as e:
        print(f"❌ Query failed: {e}")
        return False

def main():
    """Main function to run the example."""
    print("🚀 TimescaleDB Integration Example")
    print("=" * 50)
    
    # Step 1: Check prerequisites
    if not check_database_url():
        return 1
    
    if not test_database_connection():
        return 1
    
    # Step 2: Ask user if they want to import data
    print("\n" + "=" * 50)
    response = input("Do you want to import sample data? (y/N): ").lower().strip()
    
    if response in ['y', 'yes']:
        if not import_sample_data():
            return 1
        
        # Step 3: Query the data
        print("\n" + "=" * 50)
        if not query_sample_data():
            return 1
    else:
        print("Skipping data import. You can run this script again to import data.")
    
    print("\n🎉 Example completed successfully!")
    print("\nNext steps:")
    print("1. Try importing more data with different pairs/timeframes")
    print("2. Use SQL to query and analyze your forex data")
    print("3. Build applications using the imported time-series data")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
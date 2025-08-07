"""Download (if needed), format, and import data to TimescaleDB."""
# pylint: disable=redefined-outer-name
from __future__ import annotations

import os
import sys
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from functools import partial
from multiprocessing import Process, Queue
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import rx
from rich import print  # pylint: disable=redefined-builtin
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rx import operators as ops

from histdatacom import config
from histdatacom.api import Api
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count

if TYPE_CHECKING:
    from histdatacom.records import Record

# Define namedtuples at module level to avoid repeated creation
M1Row = namedtuple("M1Row", ["datetime", "open", "high", "low", "close", "vol"])
TickRow = namedtuple("TickRow", ["datetime", "bid", "ask", "vol"])


class TimescaleDB:  # noqa:H601
    """Download (if needed), format, and import data to TimescaleDB."""

    def import_data(self) -> None:
        """Initialize a pool of TimescaleDB writers and...

        processes pool with a progress bar.
        """
        print("[TimescaleDB] Starting TimescaleDB import process...")  # noqa:T201
        print(f"[TimescaleDB] Queue size before starting: {config.TIMESCALE_CHUNKS_QUEUE.qsize()}")  # noqa:T201
        
        writer = TimescaleDBWriter(config.ARGS, config.TIMESCALE_CHUNKS_QUEUE)
        print("[TimescaleDB] Starting TimescaleDB writer process...")  # noqa:T201
        writer.start()
        print(f"[TimescaleDB] Writer process started with PID: {writer.pid}")  # noqa:T201

        pool = ProcessPool(
            self._import_file,
            config.ARGS,
            "Adding",
            "CSVs to TimescaleDB queue...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
            join=False,
            dump=False,
        )

        print("[TimescaleDB] Starting worker pool...")  # noqa:T201
        pool(
            config.CURRENT_QUEUE,
            config.NEXT_QUEUE,
            timescale_chunks_queue=config.TIMESCALE_CHUNKS_QUEUE,
        )

        with Progress(
            TextColumn(text_format="[cyan]...finishing upload to TimescaleDB"),
            SpinnerColumn(),
            SpinnerColumn(),
            SpinnerColumn(),
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("waiting", total=0)

            config.CURRENT_QUEUE.join()  # type: ignore
            config.TIMESCALE_CHUNKS_QUEUE.put(None)  # type: ignore
            config.TIMESCALE_CHUNKS_QUEUE.join()  # type: ignore
            progress.advance(task_id, 0.75)

        print("[cyan] done.")  # noqa:T201
        config.NEXT_QUEUE.dump_to_queue(config.CURRENT_QUEUE)  # type: ignore

    def _init_counters(
        self,
        timescale_chunks_queue_: Queue,
        args_: dict,
    ) -> None:
        """Initialize pool with access to these global variables.

        Args:
            timescale_chunks_queue_ (Queue): ReactiveX queue
                * serialized through SyncManager
            args_ (dict): config.ARGS
        """
        # pylint: disable=global-variable-undefined
        global TIMESCALE_CHUNKS_QUEUE  # noqa:WPS100
        TIMESCALE_CHUNKS_QUEUE = timescale_chunks_queue_  # type: ignore
        global ARGS  # noqa:WPS100
        ARGS = args_  # type: ignore

    def _import_file(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
        timescale_chunks_queue: Queue,
    ) -> None:
        """Import ASCII data to TimescaleDB, both for csv and jay.

        Args:
            record (Record): a record from the work queue
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            timescale_chunks_queue (Queue): config.TIMESCALE_CHUNKS_QUEUE

        Raises:
            Exception: on unknown exception.
        """
        try:
            if (
                record.status != "TIMESCALE_UPLOAD"
                and str.lower(record.data_format) == "ascii"
            ):
                jay_path = Path(record.data_dir, ".data")
                if jay_path.exists():
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        timescale_chunks_queue,
                    )
                elif "CSV" in record.status:
                    Api.test_for_jay_or_create(record, args)
                    self._import_jay(
                        record,
                        args,
                        records_current,
                        records_next,
                        timescale_chunks_queue,
                    )

            record.status = "TIMESCALE_UPLOAD"
            record.write_memento_file(base_dir=args["default_download_dir"])

            if args["delete_after_timescale"]:
                Path(record.data_dir, record.zip_filename).unlink()
                Path(record.data_dir, record.jay_filename).unlink()
            records_next.put(record)
        except Exception as err:
            print(  # noqa:T201
                "Unexpected error from here:", sys.exc_info(), err
            )  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    def _import_jay(
        self,
        record: Record,
        args: dict,
        records_current: Records,  # noqa:W0613 # pylint: disable=W0613
        records_next: Records,  # noqa:W0613 # pylint: disable=W0613
        timescale_chunks_queue: Queue,
    ) -> None:
        """Import a jay file with a ReactiveX pub/sub queue.

        Args:
            record (Record): a record from the work queue config.CURRENT_QUEUE
            args (dict): config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
            timescale_chunks_queue (Queue): config.TIMESCALE_CHUNKS_QUEUE
        """
        jay = Api.import_jay_data(record.data_dir + record.jay_filename)

        with ProcessPoolExecutor(
            max_workers=1,
            initializer=self._init_counters,
            initargs=(timescale_chunks_queue, config.ARGS),
        ) as executor:

            rx_data_queue = rx.from_iterable(jay.to_tuples()).pipe(
                ops.buffer_with_count(args["batch_size"]),
                ops.flat_map(
                    lambda rows: executor.submit(  # noqa:BLK100
                        self._parse_jay_rows, rows, record
                    )
                ),
            )

            rx_data_queue.subscribe(
                on_next=lambda x: None,
                on_error=lambda er: print(  # noqa:T201
                    f"Unexpected error: {er}"
                ),  # noqa:T201
            )

    def _parse_jay_rows(self, iterable: Iterable, record: Record) -> None:
        """Create a list by mapping row-by-row from datatable Frame (from jay).

        Args:
            iterable (Iterable): datatable.Frame
            record (Record): a record from the work queue.
        """
        print(f"[TimescaleDB] Processing {len(iterable)} rows for {record.data_fxpair}")  # noqa:T201
        map_func = partial(self._parse_jay_row, record=record)
        parsed_rows = list(map(map_func, iterable))

        print(f"[TimescaleDB] Adding {len(parsed_rows)} rows to queue for {record.data_fxpair}")  # noqa:T201
        TIMESCALE_CHUNKS_QUEUE.put(parsed_rows)  # type: ignore
        print(f"[TimescaleDB] Successfully added batch to queue")  # noqa:T201

    def _convert_timestamp(self, timestamp_ms: int) -> datetime:
        """Convert millisecond timestamp to UTC datetime object.
        
        Args:
            timestamp_ms (int): Timestamp in milliseconds
            
        Returns:
            datetime: Converted UTC datetime object
        """
        return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)

    def _parse_jay_row(self, row: Tuple[Any], record: Record) -> dict:
        """Return dictionary for TimescaleDB insertion from a map function.

            Applies different fields for different timeframes (M1 or T).

        Args:
            row (Tuple[Any]): row from datatable.Frame
            record (Record): record from the work queue

        Returns:
            dict: data dictionary for TimescaleDB insertion
        """
        # For tick_data table schema: timestamp, symbol, bid, ask, volume
        match record.data_timeframe:
            case "T":
                named_row = TickRow(row[0], row[1], row[2], row[3])  # type: ignore
                return {
                    "timestamp": self._convert_timestamp(row[0]),
                    "symbol": record.data_fxpair.upper(),  # Convert to uppercase for consistency
                    "bid": named_row.bid,
                    "ask": named_row.ask,
                    "volume": named_row.vol,
                }
            case "M1":
                # For M1 data, we'll store it as tick data using close price as both bid/ask
                # This is a simplification - you might want a separate table for OHLC data
                named_row = M1Row(
                    row[0],
                    row[1],  # type: ignore
                    row[2],  # type: ignore
                    row[3],  # type: ignore
                    row[4],  # type: ignore
                    row[5],  # type: ignore
                )
                return {
                    "timestamp": self._convert_timestamp(row[0]),
                    "symbol": record.data_fxpair.upper(),
                    "bid": named_row.close,  # Use close price as bid
                    "ask": named_row.close,  # Use close price as ask
                    "volume": named_row.vol,
                }
            case _:
                raise ValueError(f"Unsupported timeframe: {record.data_timeframe}")


class TimescaleDBWriter(Process):
    """Write data from the chunks queue to TimescaleDB.

    Args:
        Process (Process): A Python Process.
    """

    def __init__(self, args: dict, timescale_chunks_queue: Optional[Queue]):
        """Initialize a process for the TimescaleDB writer.

        Args:
            args (dict): config.ARGS
            timescale_chunks_queue (Optional[Queue]): config.TIMESCALE_CHUNKS_QUEUE
        """
        Process.__init__(self)
        self.args = args
        self.timescale_chunks_queue = timescale_chunks_queue
        self.database_url = args["DATABASE_URL"]
        
        # Parse the database URL
        parsed_url = urlparse(self.database_url)
        self.connection_params = {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 5432,
            "database": parsed_url.path.lstrip('/'),
            "user": parsed_url.username,
            "password": parsed_url.password,
        }
        
        self._ensure_tables_exist()

    def _ensure_tables_exist(self) -> None:
        """Ensure the required tables exist in TimescaleDB."""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cursor:
                    # Create the tick_data table to match your schema
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS tick_data (
                            id BIGSERIAL,
                            timestamp TIMESTAMPTZ NOT NULL,
                            symbol VARCHAR(10) NOT NULL,
                            bid DECIMAL(10, 5) NOT NULL,
                            ask DECIMAL(10, 5) NOT NULL,
                            volume DECIMAL(15, 2) DEFAULT 0,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """)
                    
                    # Create hypertable if it doesn't exist
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = 'tick_data'
                        );
                    """)
                    
                    hypertable_exists = cursor.fetchone()[0]
                    
                    if not hypertable_exists:
                        cursor.execute("""
                            SELECT create_hypertable('tick_data', 'timestamp', 
                                                   if_not_exists => TRUE);
                        """)
                    
                    # Create indexes for better query performance
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_forex_data_pair_timestamp 
                        ON forex_data (pair, timestamp DESC);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_forex_data_timeframe 
                        ON forex_data (timeframe, timestamp DESC);
                    """)
                    
                    conn.commit()
                    
        except psycopg2.Error as e:
            print(f"Error creating tables: {e}")  # noqa:T201
            raise

    def run(self) -> None:
        """Process chunks from config.TIMESCALE_CHUNKS_QUEUE."""
        print("[TimescaleDBWriter] Starting writer process...")  # noqa:T201
        try:
            print("[TimescaleDBWriter] Connecting to database...")  # noqa:T201
            with psycopg2.connect(**self.connection_params) as conn:
                print("[TimescaleDBWriter] Connected successfully, waiting for chunks...")  # noqa:T201
                with conn.cursor() as cursor:
                    chunk_count = 0
                    while True:
                        try:
                            print(f"[TimescaleDBWriter] Waiting for chunk {chunk_count + 1}...")  # noqa:T201
                            chunk = self.timescale_chunks_queue.get(timeout=30)  # type: ignore
                            print(f"[TimescaleDBWriter] Received chunk {chunk_count + 1} with {len(chunk) if chunk else 0} items")  # noqa:T201
                        except EOFError:
                            print("[TimescaleDBWriter] EOFError - breaking")  # noqa:T201
                            break
                        except Exception as e:
                            print(f"[TimescaleDBWriter] Error getting chunk: {e}")  # noqa:T201
                            break

                        if chunk is None:
                            print("[TimescaleDBWriter] Received termination signal")  # noqa:T201
                            self.timescale_chunks_queue.task_done()  # type: ignore
                            break

                        # Prepare the data for batch insertion
                        print(f"[TimescaleDBWriter] Inserting batch of {len(chunk)} records...")  # noqa:T201
                        self._insert_batch(cursor, chunk)
                        conn.commit()
                        chunk_count += 1
                        print(f"[TimescaleDBWriter] Successfully inserted batch {chunk_count}")  # noqa:T201
                        self.timescale_chunks_queue.task_done()  # type: ignore
                        
        except KeyboardInterrupt:
            print("[TimescaleDBWriter] KeyboardInterrupt received")  # noqa:T201
            self.terminate()
        except psycopg2.Error as e:
            print(f"[TimescaleDBWriter] Database error: {e}")  # noqa:T201
            self.terminate()
        except Exception as e:
            print(f"[TimescaleDBWriter] Unexpected error: {e}")  # noqa:T201
            import traceback
            traceback.print_exc()
            self.terminate()
        finally:
            print("[TimescaleDBWriter] Writer process finished")  # noqa:T201

    def _insert_batch(self, cursor, chunk: list) -> None:
        """Insert a batch of data into TimescaleDB.
        
        Args:
            cursor: Database cursor
            chunk (list): List of data dictionaries to insert
        """
        if not chunk:
            return
            
        # Prepare the INSERT statement for tick_data table
        insert_query = """
            INSERT INTO tick_data (
                timestamp, symbol, bid, ask, volume
            ) VALUES %s
            ON CONFLICT DO NOTHING;
        """
        
        # Convert chunk data to tuples for batch insertion
        values = []
        for data in chunk:
            values.append((
                data["timestamp"],
                data["symbol"],
                data["bid"],
                data["ask"],
                data["volume"],
            ))
        
        # Use execute_values for efficient batch insertion
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=1000
        )

    def terminate(self) -> None:
        """Terminate the TimescaleDB subprocess."""
        self.close()
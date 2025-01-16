from typing import Any, Iterable, Mapping, Optional
import os
from airbyte_cdk import ConfiguredAirbyteCatalog
import duckdb
from datetime import datetime
from contextlib import contextmanager
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models.airbyte_protocol import (
    AirbyteConnectionStatus,
    Status,
    AirbyteMessage,
    Type,
    AirbyteStateMessage,
    AirbyteRecordMessage
)

class MotherDuckDestination(Destination):
    def __init__(self):
        self._conn = None
        self._current_state = {}
        self._records_processed = 0
        self._batch_size = 10000

    @contextmanager
    def _get_connection(self, connection_string: str):
        """Context manager for database connections"""
        conn = None
        try:
            conn = duckdb.connect(connection_string)
            yield conn
        finally:
            if conn:
                conn.close()

    def _setup_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        stream_name: str,
        primary_key: Optional[list] = None
    ) -> str:
        """Create or update table schema as needed"""
        table_name = f"{schema}.{stream_name}"
        
        if stream_name == "video_frames":
            # Add indices for better query performance
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    frame_id INTEGER,
                    x INTEGER,
                    y INTEGER,
                    value INTEGER,
                    r INTEGER,
                    g INTEGER,
                    b INTEGER,
                    video_url TEXT,
                    PRIMARY KEY (frame_id, x, y, video_url)
                )
            """)
            
            # Create indices for common query patterns
            conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{stream_name}_frame 
                ON {table_name} (frame_id, video_url)
            """)
        
        return table_name

    def _insert_batch(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        records: list,
        temp_table_name: str
    ) -> int:
        """Insert a batch of records using a temporary table"""
        if not records:
            return 0

        try:
            # Create temporary table
            conn.execute(f"""
                CREATE TEMP TABLE {temp_table_name} AS 
                SELECT * FROM {table_name} WHERE 1=0
            """)

            # Format records
            formatted_records = [
                (
                    record.get("frame_id"),
                    record.get("x"),
                    record.get("y"),
                    record.get("value"),
                    record.get("r"),
                    record.get("g"),
                    record.get("b"),
                    record.get("video_url")
                )
                for record in records
            ]

            # Bulk insert into temp table
            conn.executemany(
                f"""
                INSERT INTO {temp_table_name}
                (frame_id, x, y, value, r, g, b, video_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                formatted_records
            )

            # Merge from temp to main table
            conn.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_table_name}
                ON CONFLICT (frame_id, x, y, video_url) DO UPDATE
                SET 
                    value = EXCLUDED.value,
                    r = EXCLUDED.r,
                    g = EXCLUDED.g,
                    b = EXCLUDED.b,
            """)

            return len(records)

        finally:
            # Clean up temp table
            conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        """Write data to MotherDuck with improved error handling and performance"""
        
        service_token = config["motherduck_token"]
        database = config["database"]
        schema = config.get("schema", "public")
        self._batch_size = config.get("batch_size", 10000)
        
        connection_string = f"md:{database}?motherduck_token={service_token}"

        with self._get_connection(connection_string) as conn:
            # Create schema
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            for configured_stream in configured_catalog.streams:
                stream_name = configured_stream.stream.name
                primary_key = configured_stream.stream.source_defined_primary_key
                
                # Setup table
                table_name = self._setup_table(conn, schema, stream_name, primary_key)
                temp_table_name = f"temp_{stream_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                buffer = []
                
                try:
                    conn.begin()
                    
                    for message in input_messages:
                        if message.type == Type.STATE:
                            self._current_state = message.state.data
                            yield message
                            
                        elif message.type == Type.RECORD:
                            record = message.record.data
                            record["video_url"] = config["url"]
                            buffer.append(record)
                            
                            if len(buffer) >= self._batch_size:
                                self._records_processed += self._insert_batch(
                                    conn, table_name, buffer, temp_table_name
                                )
                                buffer = []
                                
                                # Emit statistics
                                yield AirbyteMessage(
                                    type=Type.LOG,
                                    log={
                                        "level": "INFO",
                                        "message": f"Processed {self._records_processed} records for {stream_name}"
                                    }
                                )
                    
                    # Process remaining records
                    if buffer:
                        self._records_processed += self._insert_batch(
                            conn, table_name, buffer, temp_table_name
                        )
                    
                    conn.commit()
                    
                    # Final state message
                    if self._current_state:
                        yield AirbyteMessage(
                            type=Type.STATE,
                            state=AirbyteStateMessage(data=self._current_state)
                        )
                    
                except Exception as e:
                    conn.rollback()
                    yield AirbyteMessage(
                        type=Type.LOG,
                        log={
                            "level": "ERROR",
                            "message": f"Error processing {stream_name}: {str(e)}"
                        }
                    )
                    raise e

    def check(self, logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """Enhanced connection testing"""
        try:
            service_token = config["motherduck_token"]
            database = config["database"]
            schema = config.get("schema", "public")
            
            connection_string = f"md:{database}?motherduck_token={service_token}"
            
            with self._get_connection(connection_string) as conn:
                # Test basic connection
                conn.execute("SELECT 1")
                
                # Test schema creation
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                
                # Test table creation
                test_table = f"{schema}.connection_test"
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {test_table} (
                        test_column INTEGER
                    )
                """)
                
                # Test write
                conn.execute(f"INSERT INTO {test_table} VALUES (1)")
                
                # Clean up
                conn.execute(f"DROP TABLE {test_table}")
            
            return AirbyteConnectionStatus(
                status=Status.SUCCEEDED,
                message="Successfully connected to MotherDuck"
            )
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"Error connecting to MotherDuck: {str(e)}"
            )
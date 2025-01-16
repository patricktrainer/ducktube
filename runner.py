import json
from typing import List, Mapping
from airbyte_cdk import AirbyteRecordMessage
from airbyte_cdk.models.airbyte_protocol import (
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    AirbyteStream,
    AirbyteMessage,
    Type,
    Status
)

from ducktube.source import VideoJsonSource
from ducktube.destination import MotherDuckDestination


def create_catalog(stream_name: str) -> ConfiguredAirbyteCatalog:
    """Create a simple catalog for the video stream"""
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=stream_name,
                    json_schema={
                        "type": "object",
                        "properties": {
                            "frame_id": {"type": "integer"},
                            "x": {"type": "integer"},
                            "y": {"type": "integer"},
                            "value": {"type": ["integer", "null"]},
                            "r": {"type": ["integer", "null"]},
                            "g": {"type": ["integer", "null"]},
                            "b": {"type": ["integer", "null"]}
                        }
                    },
                    supported_sync_modes=["full_refresh"]
                ),
                sync_mode="full_refresh",
                destination_sync_mode="overwrite"
            )
        ]
    )

def run_pipeline(config_path: str):
    """Run the complete pipeline from source to destination"""
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Initialize source and destination
    source = VideoJsonSource()
    destination = MotherDuckDestination()
    
    # Check source connection
    source_status = source.check_connection(logger=None, config=config['source'])
    if not source_status[0]:
        raise Exception(f"Source connection failed: {source_status[1]}")
    print("✓ Source connection successful")
    
    # Check destination connection
    dest_status = destination.check(logger=None, config=config['destination'])
    if dest_status.status != Status.SUCCEEDED:
        raise Exception(f"Destination connection failed: {dest_status.message}")
    print("✓ Destination connection successful")
    
    # Create catalog
    catalog = create_catalog("video_frames")
    
    # Get source streams
    source_messages = []
    for stream in source.streams(config['source']):
        for record in stream.read_records(sync_mode="full_refresh"):
            # Convert record to AirbyteMessage
            source_messages.append(
                AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream="video_frames",
                        data=record,
                        emitted_at=int(datetime.now().timestamp() * 1000)
                    )
                )
            )
    print(f"✓ Processed {len(source_messages)} frames from source")
    
    # Add URL to destination config
    config['destination']['url'] = config['source']['url']
    
    # Write to destination
    records_written = 0
    try:
        for message in destination.write(
            config=config['destination'],
            configured_catalog=catalog,
            input_messages=source_messages
        ):
            if message.type == Type.LOG:
                print(f"Log: {message.log}")
            records_written += 1
        
        print(f"✓ Pipeline completed successfully")
        print(f"✓ Wrote {records_written} records to MotherDuck")
    except Exception as e:
        print(f"Error writing to destination: {str(e)}")
        raise

if __name__ == "__main__":
    from datetime import datetime
    run_pipeline("config.json")
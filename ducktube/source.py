import tempfile
from typing import Any, Iterable, List, Mapping, Optional
import os
from datetime import datetime
from yt_dlp import YoutubeDL
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models.airbyte_protocol import SyncMode

from ducktube.processor import process_video

class VideoStream(Stream):
    def __init__(self, url: str, processor_config: dict):
        self.url = url
        self.processor_config = processor_config
        super().__init__()

    @property
    def name(self) -> str:
        return "video_frames"

    @property
    def primary_key(self) -> Optional[List[str]]:
        return ["frame_id", "x", "y"]
        
    @property
    def supported_sync_modes(self) -> List[SyncMode]:
        return [SyncMode.full_refresh]

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "properties": {
                "frame_id": {"type": "integer"},
                "x": {"type": "integer"},
                "y": {"type": "integer"},
                "value": {"type": ["integer", "null"]},
                "r": {"type": ["integer", "null"]},
                "g": {"type": ["integer", "null"]},
                "b": {"type": ["integer", "null"]}
            },
            "required": ["frame_id", "x", "y"]
        }

    def process_video_file(self, video_path: str) -> Iterable[Mapping[str, Any]]:
        """Process video file and yield frame data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                data = process_video(
                    video_path,
                    target_width=self.processor_config['target_width'],
                    target_height=self.processor_config['target_height'],
                    mode=self.processor_config['mode'],
                    threshold=self.processor_config['threshold'],
                    max_duration=self.processor_config['max_duration']
                )
                
                for frame in data['frames']:
                    yield frame
                    
            except Exception as e:
                self.logger.error(f"Error processing video: {str(e)}")
                raise e

    def download_video(self, url: str, temp_dir: str) -> str:
        """Download video and return path"""
        ydl_opts = {
            'format': 'mp4',
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'quiet': True
        }

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            video_path = os.path.join(temp_dir, f"{info['id']}.mp4")
            return video_path

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                video_path = self.download_video(self.url, temp_dir)
                yield from self.process_video_file(video_path)
            except Exception as e:
                self.logger.error(f"Error processing video: {str(e)}")
                raise e

class VideoJsonSource(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> tuple[bool, Optional[str]]:
        try:
            url = config['url']
            with YoutubeDL() as ydl:
                ydl.extract_info(url, download=False)
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        processor_config = {
            'target_width': config.get('target_width', 160),
            'target_height': config.get('target_height', 90),
            'mode': config.get('mode', 'binary'),
            'threshold': config.get('threshold', 10),
            'max_duration': config.get('max_duration', 10)
        }
        
        return [VideoStream(url=config['url'], processor_config=processor_config)]
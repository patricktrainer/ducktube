from ducktube.processor import process_video
from ducktube.source import VideoJsonSource
from airbyte_cdk.entrypoint import launch
import sys
import argparse

def main():
    # Check if being run as Airbyte connector
    if len(sys.argv) > 1 and sys.argv[1] in ['check', 'discover', 'read', 'spec']:
        source = VideoJsonSource()
        launch(source, sys.argv[1:])
        return

    # Original CLI functionality
    parser = argparse.ArgumentParser(description='Convert video to frames')
    parser.add_argument('video_path', help='Path to input video file')
    parser.add_argument('--width', type=int, default=160, help='Target width (default: 160)')
    parser.add_argument('--height', type=int, default=90, help='Target height (default: 90)')
    parser.add_argument('--mode', choices=['binary', 'grayscale', 'color'], default='binary',
                        help='Color mode (default: binary)')
    parser.add_argument('--threshold', type=int, default=10,
                        help='Binary threshold, 0-255 (default: 10, only used in binary mode)')

    args = parser.parse_args()

    process_video(
        video_path=args.video_path,
        target_width=args.width,
        target_height=args.height,
        mode=args.mode,
        threshold=args.threshold
    )

if __name__ == "__main__":
    main()
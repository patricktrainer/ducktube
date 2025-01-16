import cv2
import numpy as np
import json
import os

def fit_frame_to_canvas(frame, target_width, target_height):
    """
    Resize and pad the frame to fit within target dimensions while maintaining 16:9 aspect ratio.
    """
    height, width = frame.shape[:2]
    
    # Force 16:9 aspect ratio for target dimensions
    target_aspect = 16/9
    if target_width/target_height != target_aspect:
        target_width = int(target_height * target_aspect)
    
    # Resize frame maintaining aspect ratio
    scale = min(target_width/width, target_height/height)
    new_width = int(width * scale)
    new_height = int(height * scale)
    
    # Calculate padding
    padding_x = (target_width - new_width) // 2
    padding_y = (target_height - new_height) // 2
    
    # Resize frame
    resized = cv2.resize(frame, (new_width, new_height))
    
    # Create canvas with appropriate number of channels
    if len(frame.shape) == 3:
        canvas = np.full((target_height, target_width, 3), 255, dtype=np.uint8)
    else:
        canvas = np.full((target_height, target_width), 255, dtype=np.uint8)
    
    # Place resized frame on canvas
    if len(frame.shape) == 3:
        canvas[padding_y:padding_y+new_height, padding_x:padding_x+new_width, :] = resized
    else:
        canvas[padding_y:padding_y+new_height, padding_x:padding_x+new_width] = resized
    
    return canvas

def process_video(video_path, target_width=160, target_height=90, mode='binary', threshold=10, max_duration=10):
    """
    Process video file into frames with specified color mode.
    
    Args:
        video_path (str): Path to input video file
        target_width (int): Width of output frame
        target_height (int): Height of output frame
        mode (str): Color mode - 'binary', 'grayscale', or 'color'
        threshold (int): Brightness threshold for binary mode (0-255)
        max_duration (int): Maximum duration in seconds to process
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"Could not open video file: {video_path}")
    
    # Get video info
    original_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    original_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    # Calculate frames to process based on duration limit
    max_frames = int(max_duration * fps)
    total_frames = min(total_frames, max_frames)
    
    # Adjust dimensions to maintain 16:9
    target_aspect = 16/9
    if target_width/target_height != target_aspect:
        target_width = int(target_height * target_aspect)
    
    print(f"Input video: {original_width}x{original_height} @ {fps}fps")
    print(f"Output size: {target_width}x{target_height}")
    print(f"Mode: {mode}")
    print(f"Processing first {max_duration} seconds ({total_frames} frames)")
    
    metadata = {
        'original_width': original_width,
        'original_height': original_height,
        'target_width': target_width,
        'target_height': target_height,
        'fps': fps,
        'total_frames': total_frames,
        'mode': mode,
        'max_duration': max_duration
    }
    
    # Create output filename based on input video
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    output_filename = f'frames_{base_name}_{mode}.json'
    
    frames_data = []
    frame_id = 0
    total_pixels = 0
    
    while cap.isOpened() and frame_id < max_frames:
        ret, frame = cap.read()
        if not ret:
            break
        
        frame_pixels = []
        
        if mode == 'binary':
            # Convert to grayscale and binary
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            fitted_frame = fit_frame_to_canvas(gray, target_width, target_height)
            binary = (fitted_frame < threshold).astype(int)
            
            # Store only black pixels
            for y in range(target_height):
                for x in range(target_width):
                    if binary[y, x] == 1:
                        frame_pixels.append({
                            'frame_id': frame_id,
                            'x': x,
                            'y': y,
                            'value': 1
                        })
                
        elif mode == 'grayscale':
            # Convert to grayscale
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            fitted_frame = fit_frame_to_canvas(gray, target_width, target_height)
            
            # Store non-white pixels
            for y in range(target_height):
                for x in range(target_width):
                    value = int(fitted_frame[y, x])
                    if value < 255:  # Only store non-white pixels
                        frame_pixels.append({
                            'frame_id': frame_id,
                            'x': x,
                            'y': y,
                            'value': value
                        })
                        
        else:  # color mode
            fitted_frame = fit_frame_to_canvas(frame, target_width, target_height)
            
            # Store non-white pixels
            for y in range(target_height):
                for x in range(target_width):
                    b, g, r = [int(v) for v in fitted_frame[y, x]]
                    if not (r == 255 and g == 255 and b == 255):  # Only store non-white pixels
                        frame_pixels.append({
                            'frame_id': frame_id,
                            'x': x,
                            'y': y,
                            'r': r,
                            'g': g,
                            'b': b
                        })
        
        frames_data.extend(frame_pixels)
        total_pixels += len(frame_pixels)
        frame_id += 1
        
        if frame_id % 10 == 0:
            print(f"Processed {frame_id}/{total_frames} frames ({(frame_id/total_frames)*100:.1f}%)")
    
    cap.release()
    
    # Save to JSON file
    output_data = {
        'metadata': metadata,
        'frames': frames_data
    }
    
    with open(output_filename, 'w') as f:
        json.dump(output_data, f)
    
    print(f"\nProcessing complete!")
    print(f"Processed {frame_id} frames")
    print(f"Output saved to: {output_filename}")
    print(f"Total pixels stored: {total_pixels}")
    
    return output_data
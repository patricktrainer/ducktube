import React, { useState, useEffect, useCallback, useRef } from 'react';
import { MDConnection } from '@motherduck/wasm-client';

const MotherDuckVideoPlayer = () => {
  const [conn, setConn] = useState(null);
  const [canvasSize, setCanvasSize] = useState({ width: 160, height: 90 });
  const [currentFrame, setCurrentFrame] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [totalFrames, setTotalFrames] = useState(0);
  const [pixels, setPixels] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [metadata, setMetadata] = useState(null);
  const [videoUrls, setVideoUrls] = useState([]);
  const [selectedVideo, setSelectedVideo] = useState('');
  const [token, setToken] = useState('');
  const canvasRef = useRef(null);
  const frameInterval = useRef(null);

  const connectToMotherDuck = async (event) => {
    event.preventDefault();
    setIsLoading(true);
    try {
      const connection = MDConnection.create({
        mdToken: token
      });
      
      // Wait for initialization
      await connection.isInitialized();
      setConn(connection);
      await loadAvailableVideos(connection);
    } catch (error) {
      console.error('Error connecting to MotherDuck:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const loadAvailableVideos = async (connection) => {
    if (!connection) return;
    
    try {
      const result = await connection.evaluateQuery(`
        SELECT DISTINCT video_url 
        FROM ducktube.main.video_frames 
        ORDER BY video_url
      `);
      
      const urls = result.data.toRows().map(row => row.video_url);
      setVideoUrls(urls);
      
      if (urls.length > 0) {
        setSelectedVideo(urls[0]);
        await loadVideoMetadata(connection, urls[0]);
      }
    } catch (error) {
      console.error('Error loading video list:', error);
    }
  };

  const loadVideoMetadata = async (connection, videoUrl) => {
    if (!connection) return;
    
    try {
      const result = await connection.evaluateQuery(`
        SELECT 
          CAST(MAX(x) + 1 AS INTEGER) as width,
          CAST(MAX(y) + 1 AS INTEGER) as height,
          CAST(COUNT(DISTINCT frame_id) AS INTEGER) as total_frames,
          CASE 
            WHEN MAX(r) IS NOT NULL THEN 'color'
            WHEN MAX(value) <= 1 THEN 'binary'
            ELSE 'grayscale'
          END as mode
        FROM ducktube.main.video_frames
        WHERE video_url = '${videoUrl}'
      `);
      
      const meta = result.data.toRows()[0];
      setMetadata({
        target_width: meta.width,
        target_height: meta.height,
        fps: 25,
        mode: meta.mode
      });
      setCanvasSize({
        width: meta.width,
        height: meta.height
      });
      setTotalFrames(meta.total_frames);
      setCurrentFrame(0);
    } catch (error) {
      console.error('Error loading video metadata:', error);
    }
  };

  const loadFrame = useCallback(async (frameId) => {
    if (!conn || !selectedVideo) return;
    
    try {
      const result = await conn.evaluateQuery(`
        SELECT 
          CAST(x AS INTEGER) as x,
          CAST(y AS INTEGER) as y,
          CAST(value AS INTEGER) as value,
          CAST(r AS INTEGER) as r,
          CAST(g AS INTEGER) as g,
          CAST(b AS INTEGER) as b
        FROM ducktube.main.video_frames
        WHERE frame_id = ${frameId}
        AND video_url = '${selectedVideo}'
        ORDER BY y, x
      `);
      
      setPixels(result.data.toRows());
    } catch (error) {
      console.error('Error loading frame:', error);
    }
  }, [conn, selectedVideo]);

  const renderFrame = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || pixels.length === 0 || !metadata) return;

    const ctx = canvas.getContext('2d');
    const pixelSize = canvas.width / canvasSize.width;

    ctx.fillStyle = '#FFFFFF';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    
    pixels.forEach(pixel => {
      if (metadata.mode === 'binary') {
        ctx.fillStyle = pixel.value === 1 ? '#000000' : '#FFFFFF';
      }
      else if (metadata.mode === 'grayscale') {
        const value = pixel.value;
        ctx.fillStyle = `rgb(${value},${value},${value})`;
      }
      else if (metadata.mode === 'color') {
        ctx.fillStyle = `rgb(${pixel.r},${pixel.g},${pixel.b})`;
      }
      
      ctx.fillRect(
        pixel.x * pixelSize,
        pixel.y * pixelSize,
        pixelSize,
        pixelSize
      );
    });
  }, [pixels, canvasSize, metadata]);

  const playVideo = useCallback(() => {
    if (frameInterval.current || totalFrames === 0) return;
    
    setIsPlaying(true);
    frameInterval.current = setInterval(() => {
      setCurrentFrame(prev => {
        const next = (prev + 1) % totalFrames;
        return next;
      });
    }, 1000 / (metadata?.fps || 25));
  }, [totalFrames, metadata]);

  const pauseVideo = useCallback(() => {
    if (frameInterval.current) {
      clearInterval(frameInterval.current);
      frameInterval.current = null;
    }
    setIsPlaying(false);
  }, []);

  const handleVideoSelect = async (event) => {
    const url = event.target.value;
    setSelectedVideo(url);
    await loadVideoMetadata(url);
  };

  useEffect(() => {
    return () => {
      if (frameInterval.current) {
        clearInterval(frameInterval.current);
      }
    };
  }, []);

  useEffect(() => {
    loadFrame(currentFrame);
  }, [currentFrame, loadFrame]);

  useEffect(() => {
    renderFrame();
  }, [pixels, renderFrame]);

  return (
    <div className="flex flex-col items-center gap-4 p-4">
    {!conn ? (
      <form onSubmit={connectToMotherDuck} className="flex flex-col gap-2">
        <input
          type="password"
          placeholder="MotherDuck Token"
          value={token}
          onChange={(e) => setToken(e.target.value)}
          className="px-4 py-2 border rounded"
        />
        <button
          type="submit"
          disabled={isLoading || !token}
          className="px-4 py-2 bg-blue-500 text-white rounded disabled:bg-gray-400"
        >
          {isLoading ? 'Connecting...' : 'Connect to MotherDuck'}
        </button>
      </form>
      ) : (
        <>
          <select
            value={selectedVideo}
            onChange={handleVideoSelect}
            className="px-4 py-2 border rounded"
          >
            {videoUrls.map(url => (
              <option key={url} value={url}>{url}</option>
            ))}
          </select>

          {metadata && (
            <div className="text-sm text-gray-600 mb-2">
              Size: {metadata.target_width}x{metadata.target_height} | 
              FPS: {metadata.fps} | 
              Mode: {metadata.mode} |
              Frames: {totalFrames}
            </div>
          )}

          <canvas
            ref={canvasRef}
            width={canvasSize.width * 4}
            height={canvasSize.height * 4}
            className="border border-gray-300"
          />
          
          <div className="flex gap-4 items-center">
            <button
              onClick={isPlaying ? pauseVideo : playVideo}
              disabled={totalFrames === 0}
              className="px-4 py-2 bg-blue-500 text-white rounded disabled:bg-gray-400"
            >
              {isPlaying ? 'Pause' : 'Play'}
            </button>
            <input
              type="range"
              min="0"
              max={Math.max(0, totalFrames - 1)}
              value={currentFrame}
              onChange={(e) => setCurrentFrame(parseInt(e.target.value))}
              disabled={totalFrames === 0}
              className="w-64"
            />
            <span>
              Frame: {currentFrame + 1} / {totalFrames}
            </span>
          </div>
        </>
      )}
    </div>
  );
};

export default MotherDuckVideoPlayer;
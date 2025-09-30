import { useFileProcessingCache } from "@/hooks/useFileProcessingCache"; // Import the hook
import {
  decodeValueWithType,
  isPreviewableType,
} from "@/utils/fileTypeEncoder.js";
import React, { useEffect, useRef, useState } from "react";
import { FaMusic, FaPause, FaPlay } from "react-icons/fa";
import ReactMarkdown from "react-markdown";

export const Record = ({ record }) => {
  const audioRef = useRef(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isAudioLoading, setIsAudioLoading] = useState(false);
  const [duration, setDuration] = useState(0);
  const [currentTime, setCurrentTime] = useState(0);

  // Use the cache hook - limit to 5 processed files in memory
  const { processFile, cleanupCache } = useFileProcessingCache(5);

  // Decode the value to check if it has a file type
  const { type: fileType, value: actualValue } = decodeValueWithType(
    record.value
  );

  // Process the file with caching (only runs when fileType/actualValue changes)
  const processedFile = React.useMemo(() => {
    if (!fileType || !isPreviewableType(fileType)) {
      return null;
    }

    // Use record.key + timestamp as unique identifier for caching
    const recordId = `${record.key}-${record.timestamp}`;
    return processFile(fileType, actualValue, recordId);
  }, [fileType, actualValue, record.key, record.timestamp, processFile]);

  // Clean up cache when component unmounts
  useEffect(() => {
    return () => {
      cleanupCache();
    };
  }, [cleanupCache]);

  // Clean up audio when component unmounts or audio URL changes
  useEffect(() => {
    return () => {
      if (audioRef.current) {
        audioRef.current.pause();
        audioRef.current.currentTime = 0;
      }
    };
  }, [processedFile?.objectUrl]);

  const toggleAudio = async () => {
    if (!audioRef.current || !processedFile?.objectUrl) return;

    try {
      if (isPlaying) {
        audioRef.current.pause();
        setIsPlaying(false);
      } else {
        if (audioRef.current.ended) {
          audioRef.current.currentTime = 0;
        }

        const playPromise = audioRef.current.play();
        if (playPromise !== undefined) {
          await playPromise;
        }
      }
    } catch (error) {
      console.error("Error toggling audio:", error);
      setIsPlaying(false);
      setIsAudioLoading(false);
    }
  };

  // Helper function to truncate text with hover tooltip
  const TruncatedText = ({ text, maxLength = 30, className = "" }) => {
    if (!text) return null;

    const shouldTruncate = text.length > maxLength;
    const displayText = shouldTruncate
      ? `${text.substring(0, maxLength)}...`
      : text;

    return (
      <span
        className={`${className} ${
          shouldTruncate ? "cursor-help" : ""
        } !select-text text-left`}
        title={shouldTruncate ? text : undefined}
      >
        {displayText}
      </span>
    );
  };

  // Scrollable text component for values
  const ScrollableText = ({ text, className = "", maxHeight = "20.25rem" }) => {
    if (!text) return null;

    return (
      <div
        className={`${className} scroll-bare-green`}
        style={{
          maxHeight,
          overflowY: "auto",
          overflowX: "auto",
        }}
      >
        <p className="font-mono whitespace-pre-wrap break-all !text-left !select-text">
          {text}
        </p>
      </div>
    );
  };

  // Helper function to format timestamp
  const formatTimestamp = (timestamp) => {
    if (!timestamp) return "Unknown";
    const date = new Date(timestamp / 1000000);
    return date.toLocaleString();
  };

  const formatTime = (time) => {
    if (!time || isNaN(time)) return "0:00";
    const minutes = Math.floor(time / 60);
    const seconds = Math.floor(time % 60);
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  };

  const handleProgressClick = (e) => {
    if (!audioRef.current || !duration) return;

    const rect = e.currentTarget.getBoundingClientRect();
    const clickX = e.clientX - rect.left;
    const newTime = (clickX / rect.width) * duration;

    audioRef.current.currentTime = newTime;
    setCurrentTime(newTime);
  };

  // Animated Music Bars Component
  const AnimatedMusicBars = () => (
    <>
      <style>
        {`
          .music-bars-blue {
            display: flex;
            align-items: end;
            gap: 1px;
            height: 16px;
            width: 16px;
          }
          
          .music-bar-blue {
            background-color: currentColor;
            width: 2px;
            animation: musicBounceBlue 0.8s ease-in-out infinite;
          }
          
          .music-bar-blue:nth-child(1) { animation-delay: 0s; height: 40%; }
          .music-bar-blue:nth-child(2) { animation-delay: 0.2s; height: 80%; }
          .music-bar-blue:nth-child(3) { animation-delay: 0.4s; height: 60%; }
          .music-bar-blue:nth-child(4) { animation-delay: 0.6s; height: 100%; }
          .music-bar-blue:nth-child(5) { animation-delay: 0.8s; height: 50%; }
          
          @keyframes musicBounceBlue {
            0%, 100% { transform: scaleY(0.3); }
            50% { transform: scaleY(1); }
          }
        `}
      </style>
      <div className="music-bars-blue">
        <div className="music-bar-blue"></div>
        <div className="music-bar-blue"></div>
        <div className="music-bar-blue"></div>
        <div className="music-bar-blue"></div>
        <div className="music-bar-blue"></div>
      </div>
    </>
  );

  const renderValue = () => {
    if (!fileType || !isPreviewableType(fileType)) {
      // Regular text content
      return (
        <ScrollableText
          text={actualValue}
          className="text-gray-700 bg-white px-3 py-2 rounded"
          maxHeight="20.25rem"
        />
      );
    }

    // Use cached processed file data
    if (!processedFile) {
      return (
        <div className="text-red-700 bg-red-100 px-3 py-2 rounded">
          <span className="text-sm">❌ Error processing file</span>
        </div>
      );
    }

    const { objectUrl, uint8Array } = processedFile;

    // Handle different file types using cached data
    if (
      ["png", "jpg", "jpeg", "gif", "bmp", "webp", "svg"].includes(fileType)
    ) {
      return (
        <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
          <div className="flex items-center justify-between gap-2 mb-2">
            <span className="text-sm font-semibold">🖼️ Image Preview:</span>
            <DownloadButton
              fileUrl={objectUrl}
              fileName={`image.${fileType}`}
              fileType={fileType}
            />
          </div>
          <img
            src={objectUrl}
            alt="Preview"
            className="max-w-full max-h-80 object-contain rounded border-2 border-green-200"
          />
        </div>
      );
    }

    if (["mp3", "wav", "ogg"].includes(fileType)) {
      return (
        <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
          <div className="flex items-center justify-between gap-2 mb-3">
            <div className="flex items-center gap-3">
              <div className="flex items-center justify-center">
                <FaMusic className="w-4 h-4 text-green-600 mr-2" />
                <span className="text-sm font-semibold">Audio File:</span>
              </div>
              <button
                type="button"
                onClick={toggleAudio}
                disabled={isAudioLoading || !objectUrl}
                className={`flex items-center gap-2 px-3 py-1 ${
                  isPlaying
                    ? "bg-sky-500 hover:bg-sky-600 border-sky-700 shadow-[2px_2px_0px_0px_rgba(3,105,161,1)] hover:shadow-[3px_3px_0px_0px_rgba(3,105,161,1)]"
                    : isAudioLoading || !objectUrl
                    ? "bg-gray-400 border-gray-600 shadow-[2px_2px_0px_0px_rgba(75,85,99,1)] cursor-not-allowed"
                    : "bg-blue-500 hover:bg-blue-600 border-blue-700 shadow-[2px_2px_0px_0px_rgba(29,78,216,1)] hover:shadow-[3px_3px_0px_0px_rgba(29,78,216,1)]"
                } text-white rounded-lg transition-all duration-200 border-2 ${
                  !isAudioLoading && objectUrl
                    ? "active:shadow-none active:translate-x-[2px] active:translate-y-[2px]"
                    : ""
                } text-sm font-bold`}
              >
                {isAudioLoading ? (
                  <>
                    <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    <span className="text-xs font-bold">Loading...</span>
                  </>
                ) : !objectUrl ? (
                  <>
                    <span className="text-xs font-bold">❌ Error</span>
                  </>
                ) : isPlaying ? (
                  <>
                    <AnimatedMusicBars />
                    <span className="text-xs font-bold">Playing...</span>
                    <FaPause className="w-3 h-3 flex-shrink-0" />
                  </>
                ) : (
                  <>
                    <FaPlay className="w-3 h-3 flex-shrink-0" />
                    <span className="text-xs font-bold">Play Audio</span>
                  </>
                )}
              </button>
            </div>
            <DownloadButton
              fileUrl={objectUrl}
              fileName={`audio.${fileType}`}
              fileType={fileType}
            />
          </div>

          {/* Audio Progress Bar */}
          <div className="bg-white px-[0.825rem] py-2 rounded-full border-2 border-green-200">
            <div className="flex items-center gap-3">
              <span className="text-xs font-mono text-green-700 min-w-[2.5rem]">
                {formatTime(currentTime)}
              </span>

              <div className="flex-1 relative">
                <div
                  className="w-full h-2 bg-green-200 rounded-full cursor-pointer border-2 border-green-300 shadow-[1px_1px_0px_0px_rgba(34,197,94,0.3)]"
                  onClick={handleProgressClick}
                >
                  <div
                    className="h-full bg-green-500 rounded-full transition-all duration-100 shadow-[1px_1px_0px_0px_rgba(34,197,94,0.6)]"
                    style={{
                      width:
                        duration > 0
                          ? `${(currentTime / duration) * 100}%`
                          : "0%",
                    }}
                  />
                  {/* Progress handle */}
                  <div
                    className="absolute top-1/2 -translate-y-1/2 w-4 h-4 bg-green-600 border-2 border-white rounded-full shadow-[2px_2px_0px_0px_rgba(34,197,94,0.4)] transition-all duration-100"
                    style={{
                      left:
                        duration > 0
                          ? `calc(${(currentTime / duration) * 100}% - 8px)`
                          : "-8px",
                    }}
                  />
                </div>
              </div>

              <span className="text-xs font-mono text-green-700 min-w-[2.5rem]">
                {formatTime(duration)}
              </span>
            </div>
          </div>

          {objectUrl && (
            <audio
              ref={audioRef}
              src={objectUrl}
              onLoadStart={() => setIsAudioLoading(true)}
              onCanPlay={() => setIsAudioLoading(false)}
              onLoadedMetadata={() => {
                if (audioRef.current) {
                  setDuration(audioRef.current.duration);
                }
              }}
              onTimeUpdate={() => {
                if (audioRef.current) {
                  setCurrentTime(audioRef.current.currentTime);
                }
              }}
              onEnded={() => {
                setIsPlaying(false);
                setIsAudioLoading(false);
                setCurrentTime(0);
              }}
              onPlay={() => {
                setIsPlaying(true);
                setIsAudioLoading(false);
              }}
              onPause={() => setIsPlaying(false)}
              onError={(e) => {
                console.error("Audio error:", e);
                setIsPlaying(false);
                setIsAudioLoading(false);
              }}
              preload="metadata"
            />
          )}
        </div>
      );
    }

    if (fileType === "pdf") {
      return (
        <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
          <div className="flex items-center justify-between gap-2 mb-2">
            <span className="text-sm font-semibold">📄 PDF Document:</span>
            <DownloadButton
              fileUrl={objectUrl}
              fileName="document.pdf"
              fileType={fileType}
            />
          </div>
          <iframe
            src={objectUrl}
            className="w-full h-[20.25rem] border-2 border-green-200 rounded"
            title="PDF Preview"
            type="application/pdf"
          />
        </div>
      );
    }

    if (fileType === "md") {
      try {
        const markdownContent = new TextDecoder().decode(uint8Array);

        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-sm font-semibold">
                📝 Markdown Content:
              </span>
              <DownloadButton
                fileUrl={objectUrl}
                fileName="document.md"
                fileType={fileType}
              />
            </div>
            <div
              className="bg-white px-2 py-1 border border-green-200 rounded prose prose-sm max-w-full scroll-bare-green !text-left !select-text"
              style={{ maxHeight: "20.25rem", overflowY: "auto" }}
            >
              <ReactMarkdown>{markdownContent}</ReactMarkdown>
            </div>
          </div>
        );
      } catch (e) {
        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm">
                📝 Markdown file (preview unavailable)
              </span>
              <DownloadButton
                fileUrl={objectUrl}
                fileName="document.md"
                fileType={fileType}
              />
            </div>
          </div>
        );
      }
    }

    if (fileType === "txt") {
      try {
        const textContent = new TextDecoder().decode(uint8Array);

        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-sm font-semibold">
                📝 Text File Content:
              </span>
              <DownloadButton
                fileUrl={objectUrl}
                fileName="document.txt"
                fileType={fileType}
              />
            </div>
            <ScrollableText
              text={textContent}
              className="bg-white text-gray-700 px-2 py-1 border border-green-200 rounded"
              maxHeight="20.25rem"
            />
          </div>
        );
      } catch (e) {
        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm">
                📝 Text file (preview unavailable)
              </span>
              <DownloadButton
                fileUrl={objectUrl}
                fileName="document.txt"
                fileType={fileType}
              />
            </div>
          </div>
        );
      }
    }

    // Fallback for other file types
    return (
      <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
        <span className="text-sm">📎 {fileType.toUpperCase()} file</span>
      </div>
    );
  };

  return (
    <div className="space-y-4">
      <div className="bg-white/50 rounded-lg p-4 px-5 sm:px-6 border-2 border-green-300">
        <div className="grid gap-3 align-baseline">
          {/* Key */}
          <div className="flex items-start gap-2">
            <span className="font-semibold text-green-800 min-w-[80px]">
              Key:
            </span>
            <TruncatedText
              text={record.key}
              className="font-mono text-green-700 bg-green-100 px-2 py-1 rounded"
              maxLength={40}
            />
          </div>

          {/* Value - Now with cached file preview support */}
          <div className="flex items-start gap-2">
            <span className="font-semibold text-green-800 min-w-[80px]">
              Value:
            </span>
            <div className="flex-1">{renderValue()}</div>
          </div>

          {/* Timestamp */}
          <div className="flex items-start gap-2">
            <span className="font-semibold text-green-800 min-w-[80px]">
              Created:
            </span>
            <span className="text-green-700 text-sm">
              {formatTimestamp(record.timestamp).replace(/\//g, ".")}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

const DownloadButton = ({ fileUrl, fileName, fileType }) => (
  <a
    href={fileUrl}
    download={fileName || `file.${fileType}`}
    className="px-2 py-1 border-2 border-gray-800 bg-gray-500 hover:bg-gray-600 active:bg-gray-500 text-white rounded text-xs font-bold flex-shrink-0"
  >
    ⬇️ Download
  </a>
);

export default Record;

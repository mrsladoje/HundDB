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
  const [audioUrl, setAudioUrl] = useState(null);
  const [isAudioLoading, setIsAudioLoading] = useState(false);

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

  // Decode the value to check if it has a file type
  const { type: fileType, value: actualValue } = decodeValueWithType(
    record.value
  );

  // FIXED: Moved audio URL creation to proper useEffect
  useEffect(() => {
    let newAudioUrl = null;

    // Only create audio URL if we have audio file type
    if (fileType && ["mp3", "wav", "ogg"].includes(fileType)) {
      try {
        const byteArray = actualValue.split(",").map((x) => {
          const num = parseInt(x.trim());
          return isNaN(num) ? 0 : num;
        });
        const fileBlob = new Blob([new Uint8Array(byteArray)]);
        newAudioUrl = URL.createObjectURL(fileBlob);
        setAudioUrl(newAudioUrl);
      } catch (error) {
        console.error("Error creating audio blob:", error);
        setIsAudioLoading(false);
      }
    }

    // Cleanup function
    return () => {
      if (newAudioUrl) {
        URL.revokeObjectURL(newAudioUrl);
      }
    };
  }, [fileType, actualValue]);

  // Clean up audio and URLs when component unmounts
  useEffect(() => {
    return () => {
      if (audioRef.current) {
        audioRef.current.pause();
        audioRef.current.currentTime = 0;
      }
      if (audioUrl) {
        URL.revokeObjectURL(audioUrl);
      }
    };
  }, [audioUrl]);

  const toggleAudio = async () => {
    if (!audioRef.current) return;

    try {
      if (isPlaying) {
        audioRef.current.pause();
        setIsPlaying(false);
      } else {
        // Reset audio to beginning if it ended
        if (audioRef.current.ended) {
          audioRef.current.currentTime = 0;
        }

        const playPromise = audioRef.current.play();
        if (playPromise !== undefined) {
          await playPromise;
          // Don't set isPlaying here - let the onPlay event handler do it
        }
      }
    } catch (error) {
      console.error("Error toggling audio:", error);
      setIsPlaying(false);
      setIsAudioLoading(false); // FIXED: Reset loading state on error
    }
  };

  // Animated Music Bars Component (blue version)
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

    // Handle file previews
    let fileBlob;
    let fileUrl;

    try {
      const byteArray = actualValue.split(",").map((x) => {
        const num = parseInt(x.trim());
        return isNaN(num) ? 0 : num;
      });

      // Set correct MIME type based on file type
      const mimeType =
        {
          pdf: "application/pdf",
          png: "image/png",
          jpg: "image/jpeg",
          jpeg: "image/jpeg",
          gif: "image/gif",
          mp3: "audio/mpeg",
          wav: "audio/wav",
          ogg: "audio/ogg",
          txt: "text/plain",
          md: "text/markdown",
        }[fileType] || "application/octet-stream";

      fileBlob = new Blob([new Uint8Array(byteArray)], { type: mimeType });
      fileUrl = URL.createObjectURL(fileBlob);
    } catch (error) {
      console.error("Error creating blob:", error);
      return (
        <div className="text-red-700 bg-red-100 px-3 py-2 rounded">
          <span className="text-sm">❌ Error loading file</span>
        </div>
      );
    }

    if (
      ["png", "jpg", "jpeg", "gif", "bmp", "webp", "svg"].includes(fileType)
    ) {
      return (
        <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
          <div className="flex items-center justify-between gap-2 mb-2">
            <span className="text-sm font-semibold">🖼️ Image Preview:</span>
            <DownloadButton
              fileUrl={fileUrl}
              fileName={`image.${fileType}`}
              fileType={fileType}
            />
          </div>
          <img
            src={fileUrl}
            alt="Preview"
            className="max-w-full max-h-80 object-contain rounded border-2 border-green-200"
            onLoad={() => URL.revokeObjectURL(fileUrl)}
          />
        </div>
      );
    }

    if (["mp3", "wav", "ogg"].includes(fileType)) {
      return (
        <div className="text-green-700 bg-green-100 px-3 py-2 rounded flex items-center justify-between">
          <div className="flex items-center gap-3 mb-2">
            <div className="flex items-center justify-center">
              <FaMusic className="w-4 h-4 text-green-600 mr-2" />
              <span className="text-sm font-semibold">Audio File:</span>
            </div>
            <button
              type="button"
              onClick={toggleAudio}
              disabled={isAudioLoading || !audioUrl}
              className={`flex items-center gap-2 px-3 py-1 ${
                isPlaying
                  ? "bg-sky-500 hover:bg-sky-600 border-sky-700 shadow-[2px_2px_0px_0px_rgba(3,105,161,1)] hover:shadow-[3px_3px_0px_0px_rgba(3,105,161,1)]"
                  : isAudioLoading || !audioUrl
                  ? "bg-gray-400 border-gray-600 shadow-[2px_2px_0px_0px_rgba(75,85,99,1)] cursor-not-allowed"
                  : "bg-blue-500 hover:bg-blue-600 border-blue-700 shadow-[2px_2px_0px_0px_rgba(29,78,216,1)] hover:shadow-[3px_3px_0px_0px_rgba(29,78,216,1)]"
              } text-white rounded-lg transition-all duration-200 border-2 ${
                !isAudioLoading && audioUrl
                  ? "active:shadow-none active:translate-x-[2px] active:translate-y-[2px]"
                  : ""
              } text-sm font-bold`}
            >
              {isAudioLoading ? (
                <>
                  <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                  <span className="text-xs font-bold">Loading...</span>
                </>
              ) : !audioUrl ? (
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
          {audioUrl && (
            <audio
              ref={audioRef}
              src={audioUrl}
              onLoadStart={() => setIsAudioLoading(true)}
              onCanPlay={() => setIsAudioLoading(false)}
              onEnded={() => {
                setIsPlaying(false);
                setIsAudioLoading(false);
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
              fileUrl={fileUrl}
              fileName="document.pdf"
              fileType={fileType}
            />
          </div>
          <iframe
            src={fileUrl}
            className="w-full h-[20.25rem] border-2 border-green-200 rounded"
            title="PDF Preview"
            type="application/pdf"
            onLoad={() => {
              setTimeout(() => URL.revokeObjectURL(fileUrl), 5000);
            }}
          />
        </div>
      );
    }

    if (fileType === "md") {
      try {
        const byteArray = actualValue.split(",").map((x) => {
          const num = parseInt(x.trim());
          return isNaN(num) ? 0 : num;
        });
        const markdownContent = new TextDecoder().decode(
          new Uint8Array(byteArray)
        );

        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-sm font-semibold">
                📝 Markdown Content:
              </span>
              <DownloadButton
                fileUrl={fileUrl}
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
        URL.revokeObjectURL(fileUrl);
        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm">
                📝 Markdown file (preview unavailable)
              </span>
              <DownloadButton
                fileUrl={fileUrl}
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
        const byteArray = actualValue.split(",").map((x) => {
          const num = parseInt(x.trim());
          return isNaN(num) ? 0 : num;
        });
        const textContent = new TextDecoder().decode(new Uint8Array(byteArray));

        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-sm font-semibold">
                📝 Text File Content:
              </span>
              <DownloadButton
                fileUrl={fileUrl}
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
        URL.revokeObjectURL(fileUrl);
        return (
          <div className="text-green-700 bg-green-100 px-3 py-2 rounded">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm">
                📝 Text file (preview unavailable)
              </span>
              <DownloadButton
                fileUrl={fileUrl}
                fileName="document.txt"
                fileType={fileType}
              />
            </div>
          </div>
        );
      }
    }

    // Fallback for other file types
    URL.revokeObjectURL(fileUrl); // Clean up
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

          {/* Value - Now with file preview support */}
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
    onClick={() => setTimeout(() => URL.revokeObjectURL(fileUrl), 1000)}
  >
    ⬇️ Download
  </a>
);

export default Record;

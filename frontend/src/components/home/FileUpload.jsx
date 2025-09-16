import { useEffect, useState } from "react";
import { useDropzone } from "react-dropzone";
import { FiHelpCircle, FiUpload, FiX } from "react-icons/fi";
import { Tooltip } from "react-tooltip";

export const FileUpload = ({
  className,
  file,
  setFile,
  handleClearExtra = () => {},
  ...rest
}) => {
  const [error, setError] = useState(null);
  const [preview, setPreview] = useState(null);

  useEffect(() => {
    if (file) {
      if (file.type?.startsWith("image/")) {
        const objectUrl = URL.createObjectURL(file);
        setPreview(objectUrl);
        return () => URL.revokeObjectURL(objectUrl);
      }
    } else {
      setPreview(null);
    }
  }, [file]);

  const onDrop = (acceptedFiles, fileRejections) => {
    if (fileRejections.length > 0) {
      const rejection = fileRejections[0];
      if (rejection.errors[0]?.code === "file-too-large") {
        setError({
          message: "File is larger than 8 MB. Please choose a smaller file.",
        });
      } else if (rejection.errors[0]?.code === "file-invalid-type") {
        setError({
          message:
            "File type not supported. Please upload an image, audio file, PDF, text, or markdown file.",
        });
      } else {
        setError({
          message: `Error uploading file: ${rejection.errors[0]?.message}`,
        });
      }
      setTimeout(() => setError(null), 4000);
      return;
    }

    if (acceptedFiles.length > 0) {
      setFile(acceptedFiles[0]);
    }
  };

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    multiple: false,
    maxSize: 8388608, // 8MB
    accept: {
      "image/*": [".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".svg"],
      "audio/*": [".mp3", ".wav", ".ogg"],
      "application/pdf": [".pdf"],
      "text/plain": [".txt"],
      "text/markdown": [".md"],
    },
  });

  const handleClear = (e) => {
    e.stopPropagation();
    setFile(null);
    setPreview(null);
    handleClearExtra();
  };

  const getDisplayFileName = (file) => {
    if (!file) return "";
    return file.name;
  };

  const safeRest = Object.keys(rest).reduce((acc, key) => {
    if (
      key.startsWith("data-") ||
      key.startsWith("aria-") ||
      ["id", "role", "tabIndex"].includes(key)
    ) {
      acc[key] = rest[key];
    }
    return acc;
  }, {});

  return (
    <div className={`${className} relative isolate mt-0.5`} {...safeRest}>
      {file && (
        <button
          type="button"
          onClick={handleClear}
          className="p-[0.115rem] hover:scale-105 active:scale-95 z-10 absolute -top-2 -right-2 overflow-visible rounded-full border-2 border-sloth-brown-dark text-sloth-brown-dark bg-red-400 hover:bg-red-300 active:bg-red-500 flex justify-center items-center transition-all duration-200"
        >
          <FiX className="z-10 w-[1.15rem] h-[1.15rem]" />
        </button>
      )}
      <div
        {...getRootProps()}
        className={`w-full h-full border-4 border-dashed rounded-lg p-6 text-center relative cursor-pointer transition-all duration-300 
        ${
          isDragActive
            ? "border-sloth-brown hover:border-sloth-brown-dark bg-sloth-yellow-lite/50"
            : "border-sloth-brown-dark hover:border-sloth-brown"
        } 
        ${
          error
            ? "border-red-500 hover:border-red-300 bg-red-50 showerror"
            : "bg-sloth-yellow/30 hover:bg-sloth-yellow/50"
        }
        shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] hover:shadow-[4px_4px_0px_0px_rgba(139,119,95,1)]`}
      >
        <input {...getInputProps()} />
        <div className="absolute inset-0 flex items-center justify-center p-4">
          {file ? (
            <div className="w-full h-full flex items-center justify-center">
              {file.type?.startsWith("image/") && preview ? (
                <div className="relative w-full h-full">
                  <img
                    src={preview}
                    alt="Preview"
                    className="w-full h-full object-contain rounded-lg"
                  />
                  <div className="absolute bottom-2 left-2 right-2 text-sm bg-sloth-brown bg-opacity-90 text-sloth-yellow p-2 rounded border-2 border-sloth-brown-dark">
                    <span className="font-bold">
                      🐕 {getDisplayFileName(file)}
                    </span>
                  </div>
                </div>
              ) : (
                <div className="flex items-center justify-center w-full h-full rounded-xl border-4 border-sloth-brown bg-sloth-yellow/80 shadow-[2px_2px_0px_0px_rgba(139,119,95,1)]">
                  <span className="text-sloth-brown-dark font-bold text-center px-4">
                    📄 {getDisplayFileName(file)}
                  </span>
                </div>
              )}
            </div>
          ) : (
            <div>
              <FiUpload className="mx-auto h-12 w-12 text-sloth-brown-dark mb-3" />
              <p className="text-sloth-brown-dark font-bold tracking-tight">
                {isDragActive
                  ? "🐕 Drop the treasure here, woof!"
                  : "🦴 Drag 'n' drop a file here, or click to fetch one"}
              </p>
              <p className="text-sm text-sloth-brown mt-2">
                Almost all types welcome • Max 8MB
              </p>
            </div>
          )}
        </div>
      </div>
      {!file && (
        <div className="absolute top-2 left-2 z-20">
          <FiHelpCircle className="w-[1.2rem] h-[1.2rem] text-sloth-brown-dark hover:text-sloth-brown cursor-help transition-colors duration-200 showhelp" />
        </div>
      )}

      {error && (
        <Tooltip
          place="bottom"
          anchorSelect=".showerror"
          isOpen={true}
          opacity={1}
          border="3px solid #f87171"
          style={{
            backgroundColor: "#fef2f2",
            color: "#991b1b",
            zIndex: "999",
            maxWidth: "20rem",
            borderRadius: "0.5rem",
            fontSize: "0.9rem",
            whiteSpace: "normal",
            wordBreak: "break-word",
            padding: "0",
            boxShadow:
              "0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)",
          }}
          positionStrategy="fixed"
          className="!p-0"
        >
          <div className="flex items-center p-2.5 text-sm text-red-800 font-bold tracking-tighter">
            <svg
              className="flex-shrink-0 inline w-4 h-4 mr-3"
              aria-hidden="true"
              xmlns="http://www.w3.org/2000/svg"
              fill="currentColor"
              viewBox="0 0 20 20"
            >
              <path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z" />
            </svg>
            {error?.message}
          </div>
        </Tooltip>
      )}
      <Tooltip
        place="bottom"
        anchorSelect=".showhelp"
        opacity={1}
        border="3px solid #3b82f6"
        style={{
          backgroundColor: "#eff6ff",
          color: "#1e40af",
          zIndex: "999",
          maxWidth: "18rem",
          borderRadius: "0.5rem",
          fontSize: "0.85rem",
          whiteSpace: "normal",
          wordBreak: "break-word",
          padding: "0",
          boxShadow:
            "0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)",
        }}
        positionStrategy="fixed"
        className="!p-0 !z-[999]"
      >
        <div className="p-2.5 text-sm text-blue-800 font-medium">
          <div className="font-bold mb-1">📎 Allowed file types:</div>
          <div className="text-xs space-y-1">
            <div>🖼️ Images: PNG, JPG, JPEG, GIF, BMP, WebP, SVG</div>
            <div>🎵 Audio: MP3, WAV, OGG</div>
            <div>📄 Documents: PDF, TXT, MD</div>
          </div>
          <div className="mt-2 text-xs opacity-80">Max size: 8MB</div>
        </div>
      </Tooltip>
    </div>
  );
};

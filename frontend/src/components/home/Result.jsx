import React from "react";
import { FaBone, FaDog } from "react-icons/fa";
import Record from "@/components/home/Record";

const Result = ({
  operation,
  result,
  error,
  notFoundMessage,
  isSuccess = false,
}) => {
  // Helper function to truncate text with hover tooltip
  const TruncatedText = ({ text, maxLength = 30, className = "" }) => {
    if (!text) return null;

    const shouldTruncate = text.length > maxLength;
    const displayText = shouldTruncate
      ? `${text.substring(0, maxLength)}...`
      : text;

    return (
      <span
        className={`${className} ${shouldTruncate ? "cursor-help" : ""} !select-text text-left`}
        title={shouldTruncate ? text : undefined}
      >
        {displayText}
      </span>
    );
  };

  // Parse record data for GET operations
  const parseRecord = (resultString) => {
    try {
      // Extract JSON from "Found record: {...}" format
      const jsonStart = resultString.indexOf("{");
      if (jsonStart === -1) return null;

      const jsonString = resultString.substring(jsonStart);
      return JSON.parse(jsonString);
    } catch (e) {
      return null;
    }
  };

  // Determine the color scheme based on result type
  const getColorScheme = () => {
    if (error) {
      return {
        bg: "bg-red-50",
        border: "border-red-700",
        shadow: "shadow-[6px_6px_0px_0px_#f87171]",
        iconColor: "text-red-600",
        titleColor: "text-red-800",
        contentColor: "text-red-700",
        boneColor: "text-red-400",
      };
    } else if (notFoundMessage) {
      return {
        bg: "bg-yellow-50",
        border: "border-yellow-600",
        shadow: "shadow-[6px_6px_0px_0px_#fde047]",
        iconColor: "text-yellow-600",
        titleColor: "text-yellow-800",
        contentColor: "text-yellow-700",
        boneColor: "text-yellow-400",
      };
    } else {
      return {
        bg: "bg-green-50",
        border: "border-green-600",
        shadow: "shadow-[6px_6px_0px_0px_#4ade80]",
        iconColor: "text-green-600",
        titleColor: "text-green-800",
        contentColor: "text-green-700",
        boneColor: "text-green-400",
      };
    }
  };

  const colors = getColorScheme();

  const renderContent = () => {
    switch (operation) {
      case "GET":
        if (isSuccess && result) {
          const record = parseRecord(result);
          if (record) {
            // Use the new Record component for GET operations
            return <Record record={record} />;
          }
        }
        // Fallback to original text display for GET
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );

      case "PUT":
        if (isSuccess && result) {
          // Extract key from success message
          const keyMatch = result.match(/key: (.+)$/);
          const extractedKey = keyMatch ? keyMatch[1] : "Unknown";

          return (
            <div className="space-y-4">
              <div className="bg-white/50 rounded-lg p-4 border-2 border-green-300">
                <div className="flex items-center gap-3">
                  <div className="w-3 h-3 bg-green-500 rounded-full"></div>
                  <span className="font-semibold text-green-800">
                    Record Successfully Stored
                  </span>
                </div>
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-green-700 text-sm">Key:</span>
                  <TruncatedText
                    text={extractedKey}
                    className="font-mono text-green-700 bg-green-100 px-2 py-1 rounded text-sm"
                    maxLength={50}
                  />
                </div>
              </div>
            </div>
          );
        }
        // Fallback to original text display for PUT
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || error}
          </pre>
        );

      default:
        // For all other operations, use the original text display
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
    }
  };

  // Determine title based on result type
  const getTitle = () => {
    if (error) return "Something went wrong!";
    if (notFoundMessage) return "Record not found!";
    return "Operation successful! Woof-hoo!";
  };

  return (
    <div
      className={`rounded-xl p-6 border-4 font-mono text-sm relative overflow-hidden ${colors.bg} ${colors.border} ${colors.shadow}`}
    >
      <div className="flex items-center gap-2 mb-4">
        <FaDog className={`text-xl ${colors.iconColor}`} />
        <h3 className={`font-bold text-lg ${colors.titleColor}`}>
          {getTitle()}
        </h3>
      </div>

      <div className={colors.contentColor}>{renderContent()}</div>

      {/* Result decoration */}
      <FaBone
        className={`absolute top-2 right-2 text-2xl opacity-20 ${colors.boneColor}`}
      />
    </div>
  );
};

export default Result;
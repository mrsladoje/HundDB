import { FaDog, FaBone, FaRegSave, FaRegTrashAlt } from "react-icons/fa";
import PrefixScanTable from "@/components/table/PrefixScanTable";
import Record from "@/components/home/Record";
import { Get } from "@wails/main/App.js";

const Result = ({
  operation,
  result,
  error,
  notFoundMessage,
  isSuccess = false,
  iteratorOperation = null,
  onIteratorNext = null,
  operations = [],
  onPrefixScanPageChange = null,
  currentPrefix = "",
  activeOperationId = null,
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

  const renderIteratorContent = (op) => {
    if (!op.currentRecord && !op.notFoundMessage && !op.message) {
      return (
        <div className="text-center py-4">
          <span className="text-gray-500">No current record</span>
        </div>
      );
    }

    return (
      <div className="space-y-4">
        {op.currentRecord && <Record record={op.currentRecord} />}

        {op.notFoundMessage && !op.currentRecord && (
          <div className="text-center py-4 text-yellow-700 bg-yellow-50 rounded-lg border border-yellow-200">
            {op.notFoundMessage}
          </div>
        )}

        {op.message && !op.success && !op.currentRecord && (
          <div className="text-center py-4 text-red-700 bg-red-50 rounded-lg border border-red-200">
            {op.message}
          </div>
        )}

        <div className="flex justify-between pt-[0.4rem]">
          <div className="mt-2 flex items-center gap-2">
            <span
              className={`${
                op.notFoundMessage
                  ? "text-yellow-700"
                  : !op.success
                  ? "text-red-700"
                  : "text-green-700"
              } text-md font-medium`}
            >
              Prefix:
            </span>
            <TruncatedText
              text={op.prefix}
              className={`font-mono ${
                op.notFoundMessage
                  ? "text-yellow-700 bg-yellow-100"
                  : !op.success
                  ? "text-red-700 bg-red-100"
                  : "text-green-700 bg-green-100"
              }  px-2 py-1 rounded text-md`}
              maxLength={50}
            />
          </div>
          <button
            onClick={() => onIteratorNext && onIteratorNext(op)}
            disabled={op.ended}
            className={`px-4 py-2 rounded-lg font-bold text-sm border-2 transition-all duration-200 ${
              op.ended
                ? "bg-gray-300 text-gray-500 border-gray-400 cursor-not-allowed"
                : "bg-blue-500 text-white border-blue-700 hover:bg-blue-600 shadow-[2px_2px_0px_0px_rgba(29,78,216,1)] hover:shadow-[3px_3px_0px_0px_rgba(29,78,216,1)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px]"
            }`}
          >
            {op.ended ? "Iterator Ended" : "Next →"}
          </button>
        </div>
      </div>
    );
  };

  const renderContent = () => {
    switch (operation) {
      case "GET": {
        if (isSuccess && result) {
          const record = parseRecord(result);
          if (record) return <Record record={record} />;
        }
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
      }

      case "PUT": {
        if (isSuccess && result) {
          const keyMatch = result.match(/key: (.+)$/);
          const extractedKey = keyMatch ? keyMatch[1] : "Unknown";
          return (
            <div className="space-y-4">
              <div className="bg-white/50 rounded-lg p-4 border-2 border-green-300">
                <div className="flex items-center gap-3">
                  <FaRegSave className="w-[1.05rem] h-[1.05rem] text-green-500" />
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
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || error}
          </pre>
        );
      }

      case "DELETE": {
        if (isSuccess && result) {
          const keyMatch = result.match(/key: (.+)$/);
          const extractedKey = keyMatch ? keyMatch[1] : "Unknown";
          return (
            <div className="space-y-4">
              <div className="bg-white/50 rounded-lg p-4 border-2 border-green-300">
                <div className="flex items-center gap-3">
                  <FaRegTrashAlt className="w-[1.05rem] h-[1.05rem] text-green-500 rounded-full" />
                  <span className="font-semibold text-green-800">
                    Record Successfully Deleted
                  </span>
                </div>
                <div className="mt-2 flex items-center gap-2">
                  <span className="text-green-700 text-sm">Deleted Key:</span>
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
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
      }

      case "PREFIX_ITERATE": {
        // Prefer the active op (e.g., when a RecentOperations item is clicked),
        // otherwise fall back to the most recent executed iterator op
        const op = (activeOperationId
          ? operations.find(
              (o) => o.id === activeOperationId && o.type === "PREFIX_ITERATE"
            )
          : operations.find((o) => o.type === "PREFIX_ITERATE")) || null;
        if (op) {
          return renderIteratorContent(op);
        }
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
      }

      case "PREFIX_SCAN": {
        if (!error) {
          // Prefer the active op (e.g., when a RecentOperations item is clicked),
          // otherwise fall back to the most recent executed PREFIX_SCAN op
          const currentOperation = (activeOperationId
            ? operations.find(
                (op) => op.id === activeOperationId && op.type === "PREFIX_SCAN"
              )
            : operations.find((op) => op.type === "PREFIX_SCAN")) || null;
          if (currentOperation) {
            return (
              <div className="flex flex-col gap-4">
                <div className="mt-2 flex items-center gap-2">
                  <span
                    className={`${
                      currentOperation.notFoundMessage
                        ? "text-yellow-700"
                        : !currentOperation.success
                        ? "text-red-700"
                        : "text-green-700"
                    } text-md font-medium`}
                  >
                    Prefix:
                  </span>
                  <TruncatedText
                    text={currentOperation.prefix}
                    className={`${
                      currentOperation.notFoundMessage
                        ? "text-yellow-700 bg-yellow-100"
                        : !currentOperation.success
                        ? "text-red-700 bg-red-100"
                        : "text-green-700 bg-green-100"
                    }  px-2 py-1 rounded text-md`}
                    maxLength={50}
                  />
                </div>
                <PrefixScanTable
                  operation={currentOperation}
                  onPageChange={async (newPage, newPageSize) => {
                    if (onPrefixScanPageChange) {
                      await onPrefixScanPageChange(newPage, newPageSize);
                    }
                  }}
                  onViewRecord={async (key) => {
                    try {
                      const record = await Get(key);
                      if (record) {
                        return {
                          key: record.key,
                          value: record.value,
                          timestamp: record.timestamp,
                          deleted: record.deleted,
                        };
                      }
                      return null;
                    } catch (e) {
                      console.error("Error fetching record:", e);
                      return null;
                    }
                  }}
                  isLoading={false}
                />
              </div>
            );
          }
        }
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
      }

      default:
        return (
          <pre className={`whitespace-pre-wrap ${colors.contentColor}`}>
            {result || notFoundMessage || error}
          </pre>
        );
    }
  };

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

      <FaBone
        className={`absolute top-2 right-2 text-2xl opacity-20 ${colors.boneColor}`}
      />
    </div>
  );
};

export default Result;

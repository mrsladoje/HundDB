import Record from "@/components/home/Record";
import React, { useRef, useState } from "react";
import {
  FaAngleDoubleLeft,
  FaAngleLeft,
  FaAngleRight,
  FaEye,
  FaTimes,
  FaSpinner,
} from "react-icons/fa";
import Select from "react-select";
import NoResultsFoundCard from "@/components/table/NoResultsFoundCard";

const PrefixScanTable = ({
  operation,
  onPageChange,
  onViewRecord,
  isLoading = false,
}) => {
  const [selectedRecord, setSelectedRecord] = useState(null);
  const [loadingRecord, setLoadingRecord] = useState(null);
  const [gotoPage, setGotoPage] = useState("");
  const scrollContainerRef = useRef(null);

  const keys = operation?.keys || [];
  const currentPage = operation?.currentPage || 1;
  const pageSize = operation?.pageSize || 10;
  const isPaginationError = !!operation?.paginationError;

  // Green theme colors
  const themeColors = {
    primary: "bg-green-500",
    secondary: "bg-green-100",
    accent: "text-green-600",
    hover: "hover:bg-green-50/90",
    active: "active:bg-green-100/70",
    text: "text-green-900",
    lightText: "text-green-700",
    border: "border-green-300",
    midBorder: "border-green-500/90",
    darkBorder: "border-green-800",
    tableHeader: "bg-green-100/60",
    tableHeaderHover: "hover:bg-green-200/60",
    buttonBg: "bg-green-100",
    buttonHover: "hover:bg-green-200",
    buttonActive: "active:bg-green-300",
    scrollThumb: "scrollbar-thumb-green-200",
  };

  const handleViewRecord = async (key) => {
    setLoadingRecord(key);
    try {
      const record = await onViewRecord(key);
      if (record) {
        setSelectedRecord(record);
      }
    } catch (error) {
      console.error("Error fetching record:", error);
    } finally {
      setLoadingRecord(null);
    }
  };

  const pageSizeOptions = [
    { value: 5, label: "Show 5" },
    { value: 10, label: "Show 10" },
    { value: 20, label: "Show 20" },
  ];

  const customSelectStyles = {
    control: (provided, state) => ({
      ...provided,
      backgroundColor: "#dcfce7",
      borderColor: "#22c55e",
      borderWidth: "2px",
      borderRadius: "0.5rem",
      boxShadow: state.isFocused ? "0 0 0 1px #16a34a" : "none",
      "&:hover": { borderColor: "#15803d" },
      minHeight: "40px",
      cursor: "pointer",
    }),
    option: (provided, state) => ({
      ...provided,
      backgroundColor: state.isSelected
        ? "#86efac"
        : state.isFocused
        ? "#bbf7d0"
        : "white",
      color: state.isSelected ? "#14532d" : "#1e293b",
      cursor: "pointer",
      ":active": {
        backgroundColor: state.isSelected ? "#86ffac" : "#bbf2e0",
        color: "#14532d",
      },
    }),
    menu: (provided) => ({
      ...provided,
      borderRadius: "0.5rem",
      boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
      borderWidth: "2px",
      borderStyle: "solid",
      borderColor: "#22c55e",
      zIndex: 9999,
    }),
    singleValue: (provided) => ({
      ...provided,
      color: "#15803d",
      fontWeight: "500",
    }),
    dropdownIndicator: (provided) => ({
      ...provided,
      color: "#16a34a",
      "&:hover": { color: "#15803d" },
    }),
    indicatorSeparator: () => ({ display: "none" }),
  };

  if (isLoading) {
    return (
      <div className="flex flex-col items-center justify-center py-16 px-4">
        <div className={`relative ${themeColors.text}`}>
          <FaSpinner className="w-12 h-12 animate-spin" />
        </div>
        <p className={`mt-4 text-lg font-medium ${themeColors.text}`}>
          Loading scan results...
        </p>
      </div>
    );
  }

  if (!keys || keys.length === 0) {
    return (
      <NoResultsFoundCard
        searchQuery={operation?.prefix || ""}
        containerRef={scrollContainerRef}
        isPaginationError={isPaginationError}
        onResetPagination={() => onPageChange(1, pageSize)}
        theme={!isPaginationError ? "yellow" : "green"}
      />
    );
  }

  return (
    <div className="space-y-4">
      {/* Table */}
      <div
        className={`relative overflow-x-auto scrollbar scrollbar-h-[0.64rem] scrollbar-w-[0.61rem] scrollbar-thumb-rounded-lg ${themeColors.scrollThumb} border-2 ${themeColors.midBorder} rounded-xl shadow-[4px_4px_0_0_rgba(0,0,0,0.1)]`}
        ref={scrollContainerRef}
      >
        <div className="absolute inset-0 bg-white rounded-xl -z-10"></div>
        <table className="w-full text-sm text-left border-separate border-spacing-0">
          <thead
            className={`text-xs sm:text-sm uppercase ${themeColors.tableHeader} border-b-2 ${themeColors.darkBorder}`}
          >
            <tr>
              <th
                className={`select-none px-2 py-3 border-r-2 border-b-2 ${themeColors.border} ${themeColors.tableHeaderHover} transition-colors font-bold text-gray-700 w-10`}
              ></th>
              <th
                className={`select-none px-2 sm:px-3 py-2 sm:py-2.5 border-r-2 border-b-2 ${themeColors.border} ${themeColors.tableHeaderHover} transition-colors font-bold text-gray-700`}
              >
                <span
                  className={`uppercase tracking-wider text-[0.785rem] font-bold ${themeColors.text}`}
                >
                  Key
                </span>
              </th>
              <th
                className={`select-none px-2 sm:px-3 py-2 sm:py-2.5 border-b-2 ${themeColors.border} ${themeColors.tableHeaderHover} transition-colors font-bold text-gray-700 w-20`}
              >
                <span
                  className={`uppercase tracking-wider text-[0.785rem] font-bold ${themeColors.text}`}
                >
                  View
                </span>
              </th>
            </tr>
          </thead>
          <tbody>
            {keys.map((key, index) => (
              <tr
                key={`${key}-${index}`}
                className={`border-b ${themeColors.hover} cursor-pointer transition-all duration-150 bg-white`}
              >
                <td className="px-1 sm:px-2 py-[0.325rem] sm:py-[0.375rem] border-r text-center text-xs font-medium border-gray-200 text-gray-500 w-10">
                  {index + 1}
                </td>
                <td className="px-2 sm:px-3 py-[0.325rem] sm:py-[0.375rem] text-gray-600 border-r border-gray-200">
                  <div className="font-mono text-sm truncate max-w-[20rem]">
                    {key}
                  </div>
                </td>
                <td className="px-2 sm:px-3 py-[0.325rem] sm:py-[0.375rem] text-gray-600 border-gray-200">
                  <div className="flex justify-center">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleViewRecord(key);
                      }}
                      disabled={loadingRecord === key}
                      className={`p-1 md:p-[0.35rem] rounded-full transition-all duration-300 transform select-none ${themeColors.buttonBg} ${themeColors.text} ${themeColors.buttonHover} ${themeColors.buttonActive} border-2 ${themeColors.darkBorder} shadow-[2px_2px_0_0_rgba(0,0,0,0.2)] hover:shadow-[1px_1px_0_0_rgba(0,0,0,0.3)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px] disabled:opacity-50 disabled:cursor-not-allowed`}
                    >
                      {loadingRecord === key ? (
                        <FaSpinner className="w-[0.95rem] h-[0.95rem] animate-spin" />
                      ) : (
                        <FaEye className="w-[0.95rem] h-[0.95rem]" />
                      )}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Selected Record Display */}
      {selectedRecord && (
        <div className="!mt-6 md:!mt-[1.525rem]">
          <div className={`mb-3 flex items-center ${themeColors.text}`}>
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <FaEye className="w-4 h-4" />
              Record Details
            </h3>
            <button
              type="button"
              onClick={() => setSelectedRecord(null)}
              title="Hide details"
              className={`ml-auto flex items-center gap-1 h-[1.775rem] px-[0.7rem] text-xs sm:text-sm font-bold rounded-lg transition-all duration-200 border-2 select-none ${themeColors.buttonBg} ${themeColors.text} ${themeColors.buttonHover} ${themeColors.buttonActive} ${themeColors.darkBorder} shadow-[2px_2px_0_0_rgba(0,0,0,0.2)] hover:shadow-[1px_1px_0_0_rgba(0,0,0,0.3)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px]`}
            >
              <FaTimes className="w-[0.85rem] h-[0.85rem]" />
              <span className="hidden sm:inline">Close</span>
            </button>
          </div>
          <Record record={selectedRecord} />
        </div>
      )}

      {/* Pagination Controls */}
      <div className="mt-4 pb-1 grid grid-cols-1 md:grid-cols-[1fr_3fr_2fr] gap-4 items-center select-none">
        {/* Page info */}
        <div className="flex flex-col items-center md:items-start">
          <span className={`text-sm font-medium ${themeColors.text}`}>
            Page <span className="font-bold">{currentPage}</span>
            <span className="ml-2 text-gray-500/80">
              ({keys.length} records)
            </span>
          </span>
        </div>

        {/* Navigation buttons */}
        <div className="flex flex-wrap justify-center gap-2">
          <button
            onClick={() => onPageChange(1, pageSize)}
            disabled={currentPage <= 1}
            className={`flex items-center justify-center h-10 px-3 text-sm font-medium rounded-lg transition-all duration-200 border-2 select-none ${
              currentPage > 1
                ? `${themeColors.buttonBg} ${themeColors.text} ${themeColors.buttonHover} ${themeColors.buttonActive} ${themeColors.darkBorder} shadow-[3px_3px_0_0_rgba(0,0,0,0.1)] hover:shadow-[1px_1px_0_0_rgba(0,0,0,0.2)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px]`
                : "bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed"
            }`}
          >
            <FaAngleDoubleLeft className="w-4 h-4" />
            <span className="hidden sm:inline ml-1">First</span>
          </button>
          <button
            onClick={() => onPageChange(currentPage - 1, pageSize)}
            disabled={currentPage <= 1}
            className={`flex items-center justify-center h-10 px-3 text-sm font-medium rounded-lg transition-all duration-200 border-2 select-none ${
              currentPage > 1
                ? `${themeColors.buttonBg} ${themeColors.text} ${themeColors.buttonHover} ${themeColors.buttonActive} ${themeColors.darkBorder} shadow-[3px_3px_0_0_rgba(0,0,0,0.1)] hover:shadow-[1px_1px_0_0_rgba(0,0,0,0.2)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px]`
                : "bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed"
            }`}
          >
            <FaAngleLeft className="w-4 h-4" />
            <span className="hidden sm:inline ml-1">Previous</span>
          </button>
          <button
            onClick={() => onPageChange(currentPage + 1, pageSize)}
            disabled={keys.length < pageSize}
            className={`flex items-center justify-center h-10 px-3 text-sm font-medium rounded-lg transition-all duration-200 border-2 select-none ${
              keys.length >= pageSize
                ? `${themeColors.buttonBg} ${themeColors.text} ${themeColors.buttonHover} ${themeColors.buttonActive} ${themeColors.darkBorder} shadow-[3px_3px_0_0_rgba(0,0,0,0.1)] hover:shadow-[1px_1px_0_0_rgba(0,0,0,0.2)] active:shadow-none active:translate-x-[2px] active:translate-y-[2px]`
                : "bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed"
            }`}
          >
            <span className="hidden sm:inline mr-1">Next</span>
            <FaAngleRight className="w-4 h-4" />
          </button>
        </div>

        {/* Page size selector and quick jump */}
        <div className="flex flex-col sm:flex-row items-center justify-end gap-4">
          <div className="flex items-center justify-center w-full sm:w-auto space-x-2">
            <label className="text-sm font-medium text-gray-600 select-none whitespace-nowrap">
              Go to:
            </label>
            <div
              className={`relative rounded-lg overflow-hidden border-2 ${themeColors.darkBorder} shadow-[2px_2px_0_0_rgba(0,0,0,0.1)]`}
            >
              <input
                type="number"
                min="1"
                placeholder="Page"
                className={`w-18 sm:w-24 p-2 text-sm text-gray-900 border-0 focus:ring-0 focus:outline-none ${themeColors.secondary} transition-all duration-200`}
                value={gotoPage}
                onChange={(e) => {
                  // Allow only digits
                  const raw = e.target.value;
                  const digitsOnly = raw.replace(/\D+/g, "");
                  setGotoPage(digitsOnly);
                }}
                onKeyDown={(e) => {
                  // Block non-numeric input (except control keys)
                  const allowedKeys = [
                    "Backspace",
                    "Delete",
                    "ArrowLeft",
                    "ArrowRight",
                    "Home",
                    "End",
                    "Tab",
                    "Enter",
                  ];
                  if (
                    allowedKeys.includes(e.key) ||
                    // Allow Ctrl/Cmd + A/C/V/X
                    e.ctrlKey ||
                    e.metaKey
                  ) {
                    if (e.key === "Enter") {
                      const page = parseInt(gotoPage, 10);
                      if (!isNaN(page) && page >= 1) {
                        onPageChange(page, pageSize);
                      }
                    }
                    return;
                  }
                  // Allow digits only
                  if (!/^[0-9]$/.test(e.key)) {
                    e.preventDefault();
                  }
                }}
              />
            </div>
          </div>

          <div className="w-full sm:w-48">
            <Select
              value={pageSizeOptions.find(
                (option) => option.value === pageSize
              )}
              onChange={(option) => onPageChange(currentPage, option.value)}
              options={pageSizeOptions}
              className="react-select-container max-w-[12rem] mx-auto"
              classNamePrefix="react-select"
              styles={customSelectStyles}
              theme={(theme) => ({
                ...theme,
                colors: {
                  ...theme.colors,
                  primary: "#22c55e", // green-500 for active states
                  primary25: "#bbf7d0", // green-200 on hover
                  primary50: "#86efac", // green-300 on stronger focus/active
                },
              })}
              isSearchable={false}
              menuPortalTarget={document.body}
            />
          </div>
        </div>
      </div>
    </div>
  );
};

export default PrefixScanTable;

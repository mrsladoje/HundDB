import React, { useEffect, useState } from "react";
import { GetSSTableLevels, GetSSTableStats, CheckSSTableIntegrity } from "@wails/main/App.js";

// Background decorations matching Home page
const BgDecorations = () => (
  <>
    <div className="absolute top-10 left-10 text-4xl opacity-10 rotate-12 select-none pointer-events-none">
      🐾
    </div>
    <div className="absolute top-32 right-20 text-5xl opacity-10 -rotate-12 select-none pointer-events-none">
      🐾
    </div>
    <div className="absolute bottom-32 left-32 text-3xl opacity-10 rotate-45 select-none pointer-events-none">
      🐾
    </div>
    <div className="absolute top-1/2 right-10 text-4xl opacity-10 -rotate-6 select-none pointer-events-none">
      🐾
    </div>
    <div className="absolute bottom-20 right-40 text-3xl opacity-10 rotate-12 select-none pointer-events-none">
      🐾
    </div>
  </>
);

export const Data = () => {
  const [sstableLevels, setSstableLevels] = useState([]);
  const [sstableStats, setSstableStats] = useState({});
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showIntegrityModal, setShowIntegrityModal] = useState(false);
  const [selectedSSTable, setSelectedSSTable] = useState(null);
  const [integrityResults, setIntegrityResults] = useState([]);
  const [isCheckingIntegrity, setIsCheckingIntegrity] = useState(false);
  const containerRef = React.useRef(null);

  // Maximum number of integrity result boxes to keep
  const MAX_INTEGRITY_RESULTS = 3;

  useEffect(() => {
    fetchSSTableLevels();
  }, []);

  const fetchSSTableLevels = async () => {
    try {
      setIsLoading(true);
      
      // Fetch both levels and stats from the real backend
      const [levels, stats] = await Promise.all([
        GetSSTableLevels(),
        GetSSTableStats()
      ]);
      
      // Filter out empty levels for display (levels might be null arrays)
      const nonEmptyLevels = (levels || []).filter(level => level && level.length > 0);
      
      setSstableLevels(nonEmptyLevels);
      setSstableStats(stats);
      setError(null);
    } catch (err) {
      console.error("Failed to fetch SSTable levels:", err);
      setError("🐕 Woof! Couldn't fetch the SSTable data. Something went wrong in the database yard!");
    } finally {
      setIsLoading(false);
    }
  };

  const getTotalSSTables = () => {
    return sstableStats.totalSSTables || sstableLevels.reduce((sum, level) => sum + level.length, 0);
  };

  const handleSSTableClick = (sstableIndex, levelIndex) => {
    setSelectedSSTable({ index: sstableIndex, level: levelIndex });
    setShowIntegrityModal(true);
  };

  const handleIntegrityCheck = async () => {
    if (!selectedSSTable) return;

    setIsCheckingIntegrity(true);
    try {
      const result = await CheckSSTableIntegrity(selectedSSTable.index);
      
      // Add timestamp to the result
      const timestampedResult = {
        ...result,
        timestamp: new Date().toLocaleString()
      };

      // Add to results array and maintain cap
      setIntegrityResults(prev => {
        const newResults = [timestampedResult, ...prev];
        return newResults.slice(0, MAX_INTEGRITY_RESULTS);
      });

      setShowIntegrityModal(false);
      setSelectedSSTable(null);
    } catch (err) {
      console.error("Failed to check SSTable integrity:", err);
      // Add error result
      const errorResult = {
        sstableIndex: selectedSSTable.index,
        passed: false,
        error: err.message || "Unknown error occurred",
        timestamp: new Date().toLocaleString(),
        fatalError: true,
        corruptBlocks: []
      };
      
      setIntegrityResults(prev => {
        const newResults = [errorResult, ...prev];
        return newResults.slice(0, MAX_INTEGRITY_RESULTS);
      });
      
      setShowIntegrityModal(false);
      setSelectedSSTable(null);
    } finally {
      setIsCheckingIntegrity(false);
    }
  };

  const closeModal = () => {
    setShowIntegrityModal(false);
    setSelectedSSTable(null);
  };

  return (
    <div
      ref={containerRef}
      className="bg-sloth-yellow-lite/80 p-6 pt-[2.6rem] relative overflow-hidden select-none min-h-screen"
    >
      {/* Background paw prints */}
      <BgDecorations />

      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header Sign */}
        <div className="mb-3 md:mb-6 bg-sloth-brown rounded-2xl p-8 border-4 border-sloth-brown-dark shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] relative overflow-hidden">
          <div className="absolute top-0 left-0 w-full h-2 bg-gradient-to-r from-sloth-yellow via-sloth-yellow-lite to-sloth-yellow"></div>
          <div className="absolute bottom-0 left-0 w-full h-2 bg-gradient-to-r from-sloth-yellow via-sloth-yellow-lite to-sloth-yellow"></div>
          
          <div className="text-center relative z-10">
            <h1 className="text-5xl font-black text-sloth-yellow mb-2 tracking-tight" style={{ textShadow: '3px 3px 0px rgba(0,0,0,0.3)' }}>
              🗄️ SSTable Storage Levels
            </h1>
            <p className="text-sloth-yellow-lite text-lg font-semibold">
              Visual map of your database's sorted string tables
            </p>
          </div>
        </div>

        <div className="grid lg:grid-cols-3 gap-8 !mt-0 sm:!mt-3">
          {/* Main Content */}
          <div className="lg:col-span-2 space-y-6">
            {/* Stats Panel */}
            {!isLoading && !error && (
              <div className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] relative overflow-hidden">
                <div className="flex items-center gap-3 mb-4">
                  <svg className="w-6 h-6 text-sloth-brown-dark" fill="currentColor" viewBox="0 0 20 20">
                    <path d="M9 2a1 1 0 000 2h2a1 1 0 100-2H9z"/>
                    <path fillRule="evenodd" d="M4 5a2 2 0 012-2 3 3 0 003 3h2a3 3 0 003-3 2 2 0 012 2v11a2 2 0 01-2 2H6a2 2 0 01-2-2V5zm3 4a1 1 0 000 2h.01a1 1 0 100-2H7zm3 0a1 1 0 000 2h3a1 1 0 100-2h-3zm-3 4a1 1 0 100 2h.01a1 1 0 100-2H7zm3 0a1 1 0 100 2h3a1 1 0 100-2h-3z" clipRule="evenodd"/>
                  </svg>
                  <h2 className="text-2xl font-bold text-sloth-brown-dark">
                    Storage Statistics
                  </h2>
                </div>

                <div className="grid grid-cols-3 gap-4">
                  <div className="bg-sloth-yellow-lite rounded-lg p-4 border-3 border-sloth-brown-dark text-center">
                    <p className="text-sm font-bold text-sloth-brown mb-1">Active Levels</p>
                    <p className="text-3xl font-black text-sloth-brown-dark">{sstableLevels.length}</p>
                  </div>
                  <div className="bg-sloth-yellow-lite rounded-lg p-4 border-3 border-sloth-brown-dark text-center">
                    <p className="text-sm font-bold text-sloth-brown mb-1">Total SSTables</p>
                    <p className="text-3xl font-black text-sloth-brown-dark">{getTotalSSTables()}</p>
                  </div>
                  <div className="bg-sloth-yellow-lite rounded-lg p-4 border-3 border-sloth-brown-dark text-center">
                    <p className="text-sm font-bold text-sloth-brown mb-1">Max per Level</p>
                    <p className="text-3xl font-black text-sloth-brown-dark">
                      {sstableStats.maxTablesPerLevel || 
                        (sstableLevels.length > 0 
                          ? Math.max(...sstableLevels.map(level => level.length))
                          : 0)
                      }
                    </p>
                  </div>
                </div>
              </div>
            )}

            {/* Loading State */}
            {isLoading && (
              <div className="bg-sloth-yellow rounded-xl p-12 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] text-center">
                <div className="animate-pulse">
                  <div className="text-6xl mb-4">🐕</div>
                  <p className="text-2xl font-bold text-sloth-brown-dark">
                    Sniffing out SSTable data...
                  </p>
                  <p className="text-sloth-brown mt-2">
                    The database dog is fetching your storage levels!
                  </p>
                </div>
              </div>
            )}

            {/* Error State */}
            {error && !isLoading && (
              <div className="bg-red-100 rounded-xl p-8 border-4 border-red-400 shadow-[6px_6px_0px_0px_rgba(220,38,38,1)]">
                <div className="flex items-center gap-3 mb-4">
                  <div className="text-4xl">🐕</div>
                  <div>
                    <p className="text-xl font-bold text-red-700">{error}</p>
                  </div>
                </div>
                <button
                  onClick={fetchSSTableLevels}
                  className="mt-2 px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200"
                >
                  🔄 Try Again
                </button>
              </div>
            )}

            {/* SSTable Levels Display */}
            {!isLoading && !error && sstableLevels.length === 0 && (
              <div className="bg-sloth-yellow rounded-xl p-12 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] text-center">
                <div className="text-6xl mb-4 opacity-50">🗄️</div>
                <p className="text-2xl font-bold text-sloth-brown-dark">
                  No Active SSTable Levels
                </p>
                <p className="text-sloth-brown mt-2">
                  All data is currently in memory (MemTables). SSTables are created when data is flushed to disk.
                </p>
                <p className="text-sloth-brown mt-1 text-sm">
                  Try adding more data to trigger SSTable creation!
                </p>
              </div>
            )}

            {!isLoading && !error && sstableLevels.length > 0 && (
              <div className="space-y-6">
                {sstableLevels.map((level, levelIndex) => (
                  <div
                    key={levelIndex}
                    className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] relative overflow-hidden"
                  >
                    <div className="flex items-center gap-3 mb-4">
                      <div className="bg-sloth-brown text-sloth-yellow font-black px-5 py-2 rounded-lg border-3 border-sloth-brown-dark shadow-[3px_3px_0px_0px_rgba(0,0,0,0.5)] text-xl">
                        Level {levelIndex}
                      </div>
                      <div className="bg-sloth-yellow-lite px-4 py-2 rounded-lg border-2 border-sloth-brown">
                        <span className="text-sm font-bold text-sloth-brown">
                          {level.length} SSTable{level.length !== 1 ? 's' : ''}
                        </span>
                      </div>
                    </div>

                    <div className="flex flex-wrap gap-3">
                      {level.map((sstableIndex) => (
                        <div
                          key={sstableIndex}
                          className="w-20 h-20 bg-sloth-yellow-lite border-4 border-sloth-brown-dark rounded-lg shadow-[4px_4px_0px_0px_rgba(107,94,74,1)] flex items-center justify-center font-black text-2xl text-sloth-brown-dark hover:scale-110 hover:shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] hover:-translate-y-1 transition-all duration-200 cursor-pointer select-none"
                          title={`SSTable ${sstableIndex} - Click to check integrity`}
                          onClick={() => handleSSTableClick(sstableIndex, levelIndex)}
                        >
                          {sstableIndex}
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Side Panel */}
          <div className="space-y-6">
            {/* Refresh Control */}
            <div className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)]">
              <h3 className="text-xl font-bold text-sloth-brown-dark mb-4 flex items-center gap-2">
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clipRule="evenodd"/>
                </svg>
                Controls
              </h3>
              <button
                onClick={fetchSSTableLevels}
                disabled={isLoading}
                className="w-full flex items-center justify-center gap-2 px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clipRule="evenodd"/>
                </svg>
                {isLoading ? 'Refreshing...' : 'Refresh Data'}
              </button>
            </div>

            {/* Info Panel */}
            <div className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)]">
              <h3 className="text-xl font-bold text-sloth-brown-dark mb-4 flex items-center gap-2">
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                  <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd"/>
                </svg>
                What are SSTables?
              </h3>
              <p className="text-sloth-brown leading-relaxed text-sm">
                <strong>Sorted String Tables (SSTables)</strong> are immutable data structures in your LSM-tree database. Level 0 contains the newest data, while higher levels store progressively older, more compacted data. Each numbered square represents an actual SSTable file on disk. <strong>Click any SSTable to check its integrity!</strong>
              </p>
            </div>
          </div>
        </div>

        {/* Integrity Check Results */}
        {integrityResults.length > 0 && (
          <div className="mt-8 space-y-4">
            <div className="bg-sloth-brown rounded-2xl p-6 border-4 border-sloth-brown-dark shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] relative overflow-hidden">
              <div className="absolute top-0 left-0 w-full h-2 bg-gradient-to-r from-sloth-yellow via-sloth-yellow-lite to-sloth-yellow"></div>
              <div className="absolute bottom-0 left-0 w-full h-2 bg-gradient-to-r from-sloth-yellow via-sloth-yellow-lite to-sloth-yellow"></div>
              
              <div className="text-center relative z-10">
                <h2 className="text-3xl font-black text-sloth-yellow mb-2 tracking-tight" style={{ textShadow: '3px 3px 0px rgba(0,0,0,0.3)' }}>
                  🔍 Integrity Check Results
                </h2>
                <p className="text-sloth-yellow-lite text-sm font-semibold">
                  Latest {Math.min(integrityResults.length, MAX_INTEGRITY_RESULTS)} integrity check results (newest first)
                </p>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {integrityResults.map((result, index) => (
                <div
                  key={`${result.sstableIndex}-${result.timestamp}-${index}`}
                  className={`rounded-xl p-6 border-4 shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] ${
                    result.passed 
                      ? 'bg-green-100 border-green-500' 
                      : 'bg-red-100 border-red-500'
                  }`}
                >
                  <div className="flex items-center gap-3 mb-4">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center text-white font-bold ${
                      result.passed ? 'bg-green-500' : 'bg-red-500'
                    }`}>
                      {result.passed ? '✓' : '✗'}
                    </div>
                    <div>
                      <h3 className="text-lg font-bold text-gray-800">
                        SSTable {result.sstableIndex}
                      </h3>
                      <p className="text-sm text-gray-600">{result.timestamp}</p>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className={`px-3 py-2 rounded-lg font-bold text-center ${
                      result.passed 
                        ? 'bg-green-200 text-green-800' 
                        : 'bg-red-200 text-red-800'
                    }`}>
                      {result.passed ? '🐕 Integrity Passed!' : '⚠️ Integrity Failed!'}
                    </div>

                    {result.error && (
                      <div className="bg-red-50 border-2 border-red-200 rounded-lg p-3">
                        <p className="text-sm font-bold text-red-800 mb-1">Error:</p>
                        <p className="text-xs text-red-700 break-words">{result.error}</p>
                      </div>
                    )}

                    {result.fatalError && (
                      <div className="bg-orange-50 border-2 border-orange-200 rounded-lg p-3">
                        <p className="text-sm font-bold text-orange-800">🚨 Fatal Error Detected</p>
                      </div>
                    )}

                    {result.corruptBlocks && result.corruptBlocks.length > 0 && (
                      <div className="bg-yellow-50 border-2 border-yellow-200 rounded-lg p-3">
                        <p className="text-sm font-bold text-yellow-800 mb-2">
                          🔧 Corrupt Blocks ({result.corruptBlocks.length}):
                        </p>
                        <div className="max-h-20 overflow-y-auto">
                          {result.corruptBlocks.map((block, blockIndex) => (
                            <div key={blockIndex} className="text-xs text-yellow-700 mb-1">
                              • Block {block.blockIndex} in {block.filePath}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Dog Tips */}
        <div className="bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed border-sloth-brown rounded-xl p-6 mt-8 max-w-full mx-auto md:max-w-[90%]">
          <div className="flex items-start gap-3">
            <div className="text-3xl flex-shrink-0">🐕</div>
            <div>
              <h4 className="text-lg font-bold text-sloth-brown-dark mb-2">
                🐕 Storage Structure Tips
              </h4>
              <p className="text-sloth-brown leading-relaxed">
                <strong>Woof! Live storage insights:</strong> This shows your actual database state! When you PUT data, it goes to MemTables first. When they fill up, data gets flushed to Level 0 SSTables. The system reads from MemTables → Cache → SSTables (newest to oldest) to find your data. Each number is a real SSTable file on disk! 🦴 <strong>Click any SSTable to check its integrity!</strong>
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Integrity Check Modal */}
      {showIntegrityModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-sloth-yellow rounded-2xl p-8 border-4 border-sloth-brown-dark shadow-[12px_12px_0px_0px_rgba(0,0,0,1)] max-w-md w-full relative">
            
            <div className="text-center">
              <div className="text-5xl mb-4">🔍</div>
              <h2 className="text-2xl font-black text-sloth-brown-dark mb-4" style={{ textShadow: '2px 2px 0px rgba(0,0,0,0.2)' }}>
                Check SSTable Integrity
              </h2>
              
              {selectedSSTable && (
                <div className="bg-sloth-yellow-lite rounded-lg p-4 border-3 border-sloth-brown-dark mb-6">
                  <p className="text-lg font-bold text-sloth-brown-dark">
                    SSTable {selectedSSTable.index}
                  </p>
                  <p className="text-sm text-sloth-brown">
                    Level {selectedSSTable.level}
                  </p>
                </div>
              )}
              
              <p className="text-sloth-brown mb-6 leading-relaxed">
                This will perform a comprehensive integrity check on the selected SSTable, including:
              </p>
              
              <div className="text-left bg-sloth-yellow-lite rounded-lg p-4 border-2 border-sloth-brown mb-6">
                <ul className="text-sm text-sloth-brown-dark space-y-1">
                  <li>• 🔐 CRC checksum validation</li>
                  <li>• 🌳 Merkle tree verification</li>
                  <li>• 📁 File structure integrity</li>
                  <li>• 🧩 Data block consistency</li>
                </ul>
              </div>
              
              <div className="flex gap-3 justify-center">
                <button
                  onClick={handleIntegrityCheck}
                  disabled={isCheckingIntegrity}
                  className="px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
                >
                  {isCheckingIntegrity ? (
                    <>
                      <div className="animate-spin w-4 h-4 border-2 border-sloth-yellow border-t-transparent rounded-full"></div>
                      Checking...
                    </>
                  ) : (
                    <>
                      🔍 Check Integrity
                    </>
                  )}
                </button>
                
                <button
                  onClick={closeModal}
                  disabled={isCheckingIntegrity}
                  className="px-6 py-3 bg-gray-500 text-white font-bold rounded-lg border-4 border-gray-600 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Data;
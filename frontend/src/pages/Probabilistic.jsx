import React, { useState, useContext, useEffect, useRef } from 'react';
import { FaPlus, FaFolderOpen, FaChartBar, FaSearch, FaFilter, FaDice, FaDog } from 'react-icons/fa';
import { BsGraphUp } from 'react-icons/bs';
import { Tooltip } from 'react-tooltip';

const HyperRokica = "../../pics/rokica_hyper.png";
const RokicaLeft = "../../pics/rokica_left.png";
const SleepyCousin = "../../pics/rokica_sleepy.png";
const SleepingCousin = "../../pics/rokica_sleeping.png";

const dogMessages = [
  "Woof! These probabilistic structures are <em>paw-some</em> for handling massive datasets!",
  "Bark bark! I can <em>sniff</em> out approximate answers faster than exact ones!",
  "RUFF! Bloom filters are like my nose - sometimes I think I smell a bone that isn't there!",
  "<b>*Tail wagging!*</b> HyperLogLog helps me count things without remembering everything - I'm not that smart!",
  "Count-Min Sketch is perfect for tracking how many times I've fetched the same stick!",
];

const sleepyDogMessages = [
  "<b>*Yawn...*</b> Probabilistic structures? Sounds like a fancy way to be lazy with data...",
  "<b>*Stretches...*</b> Why count everything when you can just... estimate? Smart and sleepy...",
  "<b>*Half asleep...*</b> Bloom filters let you be wrong sometimes... my kind of structure...",
  "<b>*Mumbles...*</b> Rodney gets too excited about hash functions... zzz...",
];

const BgDecorations = () => (
  <>
    <div className="absolute top-10 left-[5%] text-6xl opacity-10 rotate-12">🎲</div>
    <div className="absolute top-32 right-[8%] text-5xl opacity-15 -rotate-12">📊</div>
    <div className="absolute bottom-20 left-[15%] text-4xl opacity-10 rotate-45">🔢</div>
    <div className="absolute top-[45%] right-[20%] text-7xl opacity-10 -rotate-30">🌸</div>
    <div className="absolute bottom-32 right-[12%] text-5xl opacity-15 rotate-12">🔍</div>
    <div className="absolute top-[60%] left-[8%] text-6xl opacity-10 -rotate-20">📈</div>
  </>
);

const Probabilistic = () => {
  const [activeStructure, setActiveStructure] = useState(null);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showLoadModal, setShowLoadModal] = useState(false);
  const [selectedStructureType, setSelectedStructureType] = useState('');
  const [isDogHovered, setIsDogHovered] = useState(false);
  const [isSleepyDogHovered, setIsSleepyDogHovered] = useState(false);
  const [dogMessage, setDogMessage] = useState("Woof! Ready to help! 🐕");
  const [sleepyMessage, setSleepyMessage] = useState("Zzz... 😴");
  
  // State for loaded structures and their interfaces
  const [loadedStructures, setLoadedStructures] = useState({});
  const [structureValues, setStructureValues] = useState({});
  const [structureResults, setStructureResults] = useState({});
  
  const containerRef = useRef(null);

  useEffect(() => {
    if (!isDogHovered) {
      setTimeout(() => setDogMessage("Woof! Ready to help! 🐕"), 160);
      return;
    }
    const randomIndex = Math.floor(Math.random() * dogMessages.length);
    setDogMessage(dogMessages[randomIndex]);
  }, [isDogHovered]);

  useEffect(() => {
    if (!isSleepyDogHovered) {
      setTimeout(() => setSleepyMessage("Zzz... 😴"), 160);
      return;
    }
    const randomIndex = Math.floor(Math.random() * sleepyDogMessages.length);
    setSleepyMessage(sleepyDogMessages[randomIndex]);
  }, [isSleepyDogHovered]);

  const probabilisticStructures = [
    {
      id: 'count-min-sketch',
      name: 'Count-Min Sketch',
      icon: <FaChartBar />,
      emoji: '📊',
      description: 'Frequency estimation with minimal space usage',
      longDescription: 'A probabilistic data structure that serves as a frequency table of events in a stream of data. Uses hash functions to map events to frequencies, but over-counts some events due to hash collisions.',
      useCase: 'Perfect for counting frequency of items in large datasets (web analytics, network monitoring)',
      parameters: ['Width (w)', 'Depth (d)', 'Hash Functions'],
      color: 'from-sloth-brown-dark to-sloth-brown'
    },
    {
      id: 'hyperloglog',
      name: 'HyperLogLog',
      icon: <BsGraphUp />,
      emoji: '🔢',
      description: 'Cardinality estimation algorithm',
      longDescription: 'Estimates the cardinality of large multisets using significantly less memory than exact counting. Uses probabilistic algorithms to provide approximate counts.',
      useCase: 'Count unique visitors, distinct database entries, unique IP addresses',
      parameters: ['Precision (b)', 'Hash Function'],
      color: 'from-sloth-brown-dark to-sloth-brown'
    },
    {
      id: 'simhash',
      name: 'SimHash',
      icon: <FaSearch />,
      emoji: '🔍',
      description: 'Document similarity detection',
      longDescription: 'Locality-sensitive hashing technique used to detect near-duplicate documents. Creates fingerprints that are similar for similar documents.',
      useCase: 'Duplicate detection, plagiarism checking, content deduplication',
      parameters: ['Hash Size', 'Tokenizer Type'],
      color: 'from-sloth-brown-dark to-sloth-brown'
    },
    {
      id: 'bloom-filter',
      name: 'Bloom Filter',
      icon: <FaFilter />,
      emoji: '🌸',
      description: 'Membership testing with false positives',
      longDescription: 'Space-efficient probabilistic data structure used to test whether an element is a member of a set. False positive matches are possible, but false negatives are not.',
      useCase: 'Cache filtering, database query optimization, malware detection',
      parameters: ['Capacity', 'False Positive Rate', 'Hash Functions'],
      color: 'from-sloth-brown-dark to-sloth-brown'
    }
  ];

  const handleCreateStructure = (structureType) => {
    setSelectedStructureType(structureType);
    setShowCreateModal(true);
  };

  const handleLoadStructure = (structureType) => {
    setSelectedStructureType(structureType);
    setShowLoadModal(true);
  };

  // Simulate loading a structure for testing
  const simulateLoadStructure = (structureType) => {
    const structureName = `test-${structureType}-${Date.now()}`;
    setLoadedStructures({
      ...loadedStructures,
      [structureType]: structureName
    });
    setShowLoadModal(false);
  };

  // Backend integration functions
  const handleAddToBloomFilter = async (name) => {
    const element = structureValues['bloom-add'];
    if (!element) return;
    
    try {
      await window.go.main.App.AddToBloomFilter(name, element);
      setStructureValues({...structureValues, 'bloom-add': ''});
      // Show success notification
    } catch (error) {
      console.error('Error adding to bloom filter:', error);
    }
  };

  const handleCheckBloomFilter = async (name) => {
    const element = structureValues['bloom-check'];
    if (!element) return;
    
    try {
      const result = await window.go.main.App.TestBloomFilter(name, element);
      setStructureResults({...structureResults, 'bloom-check': result ? 'Possibly in set' : 'Definitely not in set'});
    } catch (error) {
      console.error('Error checking bloom filter:', error);
    }
  };

  const handleAddToSimHash = async (name) => {
    const document = structureValues['simhash-add'];
    if (!document) return;
    
    try {
      await window.go.main.App.AddToSimHash(name, document);
      setStructureValues({...structureValues, 'simhash-add': ''});
      // Show success notification
    } catch (error) {
      console.error('Error adding to simhash:', error);
    }
  };

  const handleCompareSimHash = async (name) => {
    const document1 = structureValues['simhash-add'] || "test document";
    const document2 = structureValues['simhash-compare'];
    if (!document2) return;
    
    try {
      const result = await window.go.main.App.CompareDocumentsSimilarity(document1, document2);
      const displayResult = `${result.similarity} (${result.similarityPercentage}% - Distance: ${result.distance})`;
      setStructureResults({...structureResults, 'simhash-compare': displayResult});
    } catch (error) {
      console.error('Error comparing simhash:', error);
    }
  };

  const handleAddToHyperLogLog = async (name) => {
    const element = structureValues['hll-add'];
    if (!element) return;
    
    try {
      await window.go.main.App.AddToHyperLogLog(name, element);
      setStructureValues({...structureValues, 'hll-add': ''});
      // Show success notification
    } catch (error) {
      console.error('Error adding to hyperloglog:', error);
    }
  };

  const handleQueryHyperLogLog = async (name) => {
    try {
      const result = await window.go.main.App.EstimateHyperLogLog(name);
      setStructureResults({...structureResults, 'hll-count': result});
    } catch (error) {
      console.error('Error querying hyperloglog:', error);
    }
  };

  const handleAddToCountMinSketch = async (name) => {
    const element = structureValues['cms-add'];
    if (!element) return;
    
    try {
      await window.go.main.App.AddToCountMinSketch(name, element);
      setStructureValues({...structureValues, 'cms-add': ''});
      // Show success notification
    } catch (error) {
      console.error('Error adding to count-min sketch:', error);
    }
  };

  const handleQueryCountMinSketch = async (name) => {
    const element = structureValues['cms-query'];
    if (!element) return;
    
    try {
      const result = await window.go.main.App.QueryCountMinSketch(name, element);
      setStructureResults({...structureResults, 'cms-query': result});
    } catch (error) {
      console.error('Error querying count-min sketch:', error);
    }
  };

  return (
    <div ref={containerRef} className="select-none min-h-screen bg-sloth-yellow-lite/80 relative overflow-hidden p-6 pt-[2.6rem]">
      <BgDecorations />
      
      <div className="relative z-10 max-w-7xl mx-auto">
        {/* Header */}
        <div className="bg-sloth-yellow border-4 border-sloth-brown-dark rounded-xl shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] mb-8 p-6">
          <div className="flex items-center gap-4 mb-2">
            <div className="text-5xl">🎲</div>
            <div>
              <h1 className="text-4xl font-bold text-sloth-brown-dark">Probabilistic Data Structures</h1>
              <p className="text-sloth-brown text-lg mt-2">
                Efficient algorithms for approximate computations with bounded error
              </p>
            </div>
          </div>
        </div>

        {/* Structure Cards Grid */}
        <div className="grid md:grid-cols-2 gap-8 mb-8">
          {probabilisticStructures.map((structure) => (
            <div
              key={structure.id}
              className="bg-sloth-yellow border-4 border-sloth-brown-dark rounded-xl shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] hover:shadow-[8px_8px_0px_0px_rgba(107,94,74,1)] hover:-translate-x-0.5 hover:-translate-y-0.5 transition-all duration-200 overflow-hidden"
            >
              {/* Card Header */}
              <div className={`bg-gradient-to-r ${structure.color} p-6 text-white`}>
                <div className="flex items-center gap-4 mb-3">
                  <div className="text-4xl">{structure.emoji}</div>
                  <div>
                    <h2 className="text-2xl font-bold">{structure.name}</h2>
                    <p className="text-white/90 text-sm">{structure.description}</p>
                  </div>
                </div>
              </div>

              {/* Card Content */}
              <div className="p-6">
                <div className="mb-4">
                  <h3 className="font-bold text-sloth-brown-dark mb-2 text-lg">📖 Description</h3>
                  <p className="text-sloth-brown text-sm leading-relaxed">
                    {structure.longDescription}
                  </p>
                </div>

                <div className="mb-4">
                  <h3 className="font-bold text-sloth-brown-dark mb-2 text-lg">🎯 Use Case</h3>
                  <p className="text-sloth-brown text-sm">
                    {structure.useCase}
                  </p>
                </div>

                <div className="mb-6">
                  <h3 className="font-bold text-sloth-brown-dark mb-2 text-lg">⚙️ Parameters</h3>
                  <div className="flex flex-wrap gap-2">
                    {structure.parameters.map((param, index) => (
                      <span
                        key={index}
                        className="bg-sloth-yellow-lite text-sloth-brown-dark text-xs font-semibold px-3 py-1.5 rounded-lg border-2 border-sloth-brown"
                      >
                        {param}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Action Buttons */}
                <div className="flex gap-3">
                  <button
                    onClick={() => handleCreateStructure(structure.id)}
                    className="flex items-center gap-2 px-4 py-3 bg-sloth-green text-white font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200"
                  >
                    <FaPlus />
                    Create New
                  </button>
                  <button
                    onClick={() => handleLoadStructure(structure.id)}
                    className="flex items-center gap-2 px-4 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200"
                  >
                    <FaFolderOpen />
                    Load
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Quick Info Section */}
        <div className="bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed border-sloth-brown rounded-xl p-6">
          <div className="flex items-start gap-4">
            <div className="text-4xl">💡</div>
            <div className="flex-1">
              <h2 className="text-2xl font-bold text-sloth-brown-dark mb-4">About Probabilistic Structures</h2>
              <div className="grid md:grid-cols-2 gap-6 text-sloth-brown">
                <div>
                  <h3 className="font-bold text-sloth-brown-dark mb-2 text-lg">✨ Advantages</h3>
                  <ul className="space-y-2 text-sm">
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">🐾</span>
                      <span><strong>Space Efficient:</strong> Use much less memory than exact algorithms</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">⚡</span>
                      <span><strong>Fast Operations:</strong> Constant or near-constant time complexity</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">📈</span>
                      <span><strong>Scalable:</strong> Handle massive datasets with bounded resource usage</span>
                    </li>
                  </ul>
                </div>
                <div>
                  <h3 className="font-bold text-sloth-brown-dark mb-2 text-lg">⚖️ Trade-offs</h3>
                  <ul className="space-y-2 text-sm">
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">📊</span>
                      <span><strong>Approximate Results:</strong> Provide estimates, not exact values</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">📐</span>
                      <span><strong>Bounded Error:</strong> Error rates are mathematically predictable</span>
                    </li>
                    <li className="flex items-start gap-2">
                      <span className="text-sloth-brown-dark mt-0.5">🔒</span>
                      <span><strong>One-way Operations:</strong> Some structures don't support deletion</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Dog Tips */}
        <div className="bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed border-sloth-brown rounded-xl p-6 mt-8">
          <div className="flex items-start gap-3">
            <FaDog className="text-3xl text-sloth-brown mt-1 flex-shrink-0" />
            <div>
              <h4 className="text-lg font-bold text-sloth-brown-dark mb-2">
                🐕 Pro Puppy Tips
              </h4>
              <p className="text-sloth-brown leading-relaxed">
                <strong>Woof! From the pack:</strong> Rodney loves how Bloom filters are like his nose - sometimes they give false positives (thinking there's a bone when there isn't), but they never give false negatives! Del Boy prefers HyperLogLog because it lets him be lazy about counting. Remember - approximate is often <em>ruff-ly</em> good enough! 🦴
              </p>
            </div>
          </div>
        </div>

        {/* Using Loaded Structures */}
        {Object.keys(loadedStructures).length > 0 && (
          <div className="mt-8 space-y-6">
            <h2 className="text-3xl font-bold text-sloth-brown-dark flex items-center gap-3">
              <FaDice className="text-sloth-brown" />
              Using Loaded Structures
            </h2>

            {/* Bloom Filter Using Section */}
            {loadedStructures['bloom-filter'] && (
              <div className="bg-gradient-to-r from-pink-500 to-pink-600 border-4 border-sloth-brown-dark rounded-xl p-6 shadow-[6px_6px_0px_0px_rgba(0,0,0,1)]">
                <h3 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
                  <FaFilter />
                  🌸 Using Bloom Filter: {loadedStructures['bloom-filter']}
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Add Element</h4>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        placeholder="Enter element to add"
                        value={structureValues['bloom-add'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'bloom-add': e.target.value})}
                        className="flex-1 px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark"
                      />
                      <button 
                        onClick={() => handleAddToBloomFilter(loadedStructures['bloom-filter'])}
                        className="px-4 py-2 bg-pink-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Add
                      </button>
                    </div>
                  </div>
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Check Element</h4>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        placeholder="Enter element to check"
                        value={structureValues['bloom-check'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'bloom-check': e.target.value})}
                        className="flex-1 px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark"
                      />
                      <button 
                        onClick={() => handleCheckBloomFilter(loadedStructures['bloom-filter'])}
                        className="px-4 py-2 bg-pink-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Check
                      </button>
                    </div>
                    {structureResults['bloom-check'] && (
                      <div className="mt-2 p-2 bg-white rounded border text-sm">
                        <strong>Result:</strong> {structureResults['bloom-check']}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* SimHash Using Section */}
            {loadedStructures['simhash'] && (
              <div className="bg-gradient-to-r from-purple-500 to-purple-600 border-4 border-sloth-brown-dark rounded-xl p-6 shadow-[6px_6px_0px_0px_rgba(0,0,0,1)]">
                <h3 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
                  <FaSearch />
                  🔍 Using SimHash: {loadedStructures['simhash']}
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Add Document</h4>
                    <div className="space-y-2">
                      <textarea
                        placeholder="Enter document content"
                        value={structureValues['simhash-add'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'simhash-add': e.target.value})}
                        className="w-full px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark h-20 resize-none"
                      />
                      <button 
                        onClick={() => handleAddToSimHash(loadedStructures['simhash'])}
                        className="w-full px-4 py-2 bg-purple-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Add Document
                      </button>
                    </div>
                  </div>
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Compare Similarity</h4>
                    <div className="space-y-2">
                      <textarea
                        placeholder="Enter document to compare"
                        value={structureValues['simhash-compare'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'simhash-compare': e.target.value})}
                        className="w-full px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark h-20 resize-none"
                      />
                      <button 
                        onClick={() => handleCompareSimHash(loadedStructures['simhash'])}
                        className="w-full px-4 py-2 bg-purple-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Compare
                      </button>
                    </div>
                    {structureResults['simhash-compare'] && (
                      <div className="mt-2 p-2 bg-white rounded border text-sm">
                        <strong>Similarity:</strong> {structureResults['simhash-compare']}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* HyperLogLog Using Section */}
            {loadedStructures['hyperloglog'] && (
              <div className="bg-gradient-to-r from-orange-500 to-orange-600 border-4 border-sloth-brown-dark rounded-xl p-6 shadow-[6px_6px_0px_0px_rgba(0,0,0,1)]">
                <h3 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
                  <BsGraphUp />
                  🔢 Using HyperLogLog: {loadedStructures['hyperloglog']}
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Add Element</h4>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        placeholder="Enter element to add"
                        value={structureValues['hll-add'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'hll-add': e.target.value})}
                        className="flex-1 px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark"
                      />
                      <button className="px-4 py-2 bg-orange-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Add
                      </button>
                    </div>
                  </div>
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Get Cardinality</h4>
                    <div className="flex gap-2">
                      <button 
                        onClick={() => handleQueryHyperLogLog(loadedStructures['hyperloglog'])}
                        className="flex-1 px-4 py-2 bg-orange-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Get Count
                      </button>
                    </div>
                    {structureResults['hll-count'] && (
                      <div className="mt-2 p-2 bg-white rounded border text-sm">
                        <strong>Estimated Count:</strong> {structureResults['hll-count']}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* Count-Min Sketch Using Section */}
            {loadedStructures['count-min-sketch'] && (
              <div className="bg-gradient-to-r from-blue-500 to-blue-600 border-4 border-sloth-brown-dark rounded-xl p-6 shadow-[6px_6px_0px_0px_rgba(0,0,0,1)]">
                <h3 className="text-2xl font-bold text-white mb-4 flex items-center gap-2">
                  <FaChartBar />
                  📊 Using Count-Min Sketch: {loadedStructures['count-min-sketch']}
                </h3>
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Add Element</h4>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        placeholder="Enter element to add"
                        value={structureValues['cms-add'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'cms-add': e.target.value})}
                        className="flex-1 px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark"
                      />
                      <button 
                        onClick={() => handleAddToCountMinSketch(loadedStructures['count-min-sketch'])}
                        className="px-4 py-2 bg-blue-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Add
                      </button>
                    </div>
                  </div>
                  <div className="bg-white/90 p-4 rounded-lg border-2 border-sloth-brown">
                    <h4 className="font-bold text-sloth-brown-dark mb-2">Query Frequency</h4>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        placeholder="Enter element to query"
                        value={structureValues['cms-query'] || ''}
                        onChange={(e) => setStructureValues({...structureValues, 'cms-query': e.target.value})}
                        className="flex-1 px-3 py-2 border-2 border-sloth-brown rounded focus:outline-none focus:border-sloth-brown-dark"
                      />
                      <button 
                        onClick={() => handleQueryCountMinSketch(loadedStructures['count-min-sketch'])}
                        className="px-4 py-2 bg-blue-500 text-white font-bold border-2 border-sloth-brown-dark rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
                        Query
                      </button>
                    </div>
                    {structureResults['cms-query'] && (
                      <div className="mt-2 p-2 bg-white rounded border text-sm">
                        <strong>Frequency:</strong> {structureResults['cms-query']}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Create Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-sloth-yellow border-4 border-sloth-brown-dark rounded-xl shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] p-6 max-w-md w-full">
            <h3 className="text-2xl font-bold text-sloth-brown-dark mb-4 flex items-center gap-2">
              <FaPlus />
              Create {probabilisticStructures.find(s => s.id === selectedStructureType)?.name}
            </h3>
            <p className="text-sloth-brown mb-6">Creation form will be implemented here...</p>
            <div className="flex gap-3">
              <button
                onClick={() => setShowCreateModal(false)}
                className="flex-1 px-4 py-2 bg-sloth-brown text-sloth-yellow font-bold border-4 border-sloth-brown-dark rounded-lg shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200"
              >
                Cancel
              </button>
              <button className="flex-1 px-4 py-2 bg-sloth-green text-white font-bold border-4 border-sloth-brown-dark rounded-lg shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200">
                Create
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Load Modal */}
      {showLoadModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-sloth-yellow border-4 border-sloth-brown-dark rounded-xl shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] p-6 max-w-md w-full">
            <h3 className="text-2xl font-bold text-sloth-brown-dark mb-4 flex items-center gap-2">
              <FaFolderOpen />
              Load {probabilisticStructures.find(s => s.id === selectedStructureType)?.name}
            </h3>
            <p className="text-sloth-brown mb-6">Load form will be implemented here...</p>
            <div className="flex gap-3">
              <button
                onClick={() => setShowLoadModal(false)}
                className="flex-1 px-4 py-2 bg-sloth-brown text-sloth-yellow font-bold border-4 border-sloth-brown-dark rounded-lg shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200"
              >
                Cancel
              </button>
              <button 
                onClick={() => simulateLoadStructure(selectedStructureType)}
                className="flex-1 px-4 py-2 bg-sloth-green text-white font-bold border-4 border-sloth-brown-dark rounded-lg shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200">
                Load
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Tooltips */}
      <Tooltip
        anchorSelect=".hyper-dog"
        place="left-start"
        delayShow={350}
        offset={12}
        opacity={1}
        className="!bg-white !p-4 !rounded-xl !z-[9999] !max-w-sm border-2 border-sloth-brown shadow-[4px_4px_0px_0px_#6b5e4a]"
        border="3px solid #4b4436"
      >
        <div>
          <p className="font-semibold text-sloth-brown mb-2 text-lg">Rodney says:</p>
          <p className="text-gray-600 italic" dangerouslySetInnerHTML={{ __html: dogMessage }} />
        </div>
      </Tooltip>

      <Tooltip
        anchorSelect=".sleepy-dog"
        place="right-start"
        delayShow={775}
        offset={12}
        opacity={1}
        className="!bg-white !p-4 !rounded-xl !z-[9999] !max-w-sm border-2 border-sloth-brown shadow-[4px_4px_0px_0px_#6b5e4a]"
        border="3px solid #4b4436"
      >
        <div>
          <p className="font-semibold text-sloth-brown mb-2 text-lg">Del Boy says:</p>
          <p className="text-gray-600 italic" dangerouslySetInnerHTML={{ __html: sleepyMessage }} />
        </div>
      </Tooltip>

      {/* Peeking Dogs */}
      <img
        src={isDogHovered ? HyperRokica : RokicaLeft}
        alt="Rodney"
        className="hyper-dog hidden sm:block absolute -bottom-2 -right-2 w-auto h-[7.5rem] hover:h-[9.25rem] -rotate-[8deg] hover:-rotate-3 object-contain transform translate-x-1/4 translate-y-1/4 opacity-95 transition-all duration-[465ms] hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer"
        onMouseEnter={() => setIsDogHovered(true)}
        onMouseLeave={() => setIsDogHovered(false)}
      />
      <img
        src={isSleepyDogHovered ? SleepyCousin : SleepingCousin}
        alt="Del Boy"
        className="sleepy-dog hidden sm:block absolute -bottom-2 -left-2 w-auto h-[7.5rem] hover:h-[9.25rem] -rotate-[28deg] hover:rotate-1 object-contain transform -translate-x-1/4 translate-y-1/4 opacity-95 transition-all duration-[925ms] hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer"
        onMouseEnter={() => setIsSleepyDogHovered(true)}
        onMouseLeave={() => setIsSleepyDogHovered(false)}
      />
    </div>
  );
};

export default Probabilistic;
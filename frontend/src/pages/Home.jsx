import StyledOperationSelect from "@/components/select/StyledOperationSelect";
import { NavbarContext } from "@/context/NavbarContext";
import {
  Get,
  Put,
  Delete,
  PrefixScan,
  RangeScan,
  PrefixIterate,
  RangeIterate,
  IsDataLost,
} from "@wails/main/App.js";
import React, { useContext, useEffect, useState } from "react";
import {
  FaBone,
  FaDatabase,
  FaDog,
  FaHeart,
  FaPlus,
  FaSearch,
  FaTrash,
  FaList,
  FaArrowRight,
} from "react-icons/fa";
import { FiActivity } from "react-icons/fi";
import { Tooltip } from "react-tooltip";

const RokicaLeft = "../../pics/rokica_left.png";
const HyperRokica = "../../pics/rokica_hyper.png";
const SleepyCousin = "../../pics/rokica_sleepy.png";
const SleepingCousin = "../../pics/rokica_sleeping.png";
const RokicaRunning = "../../pics/rokica_running.png";
const Bone = "../../pics/bone.png";

const dogMessages = [
  "Woof! Time to fetch some data from the database! No need to <em>paws</em> - your queries are in good hands!",
  "Good human! That's some <em>paw-some</em> database work! I'd give you a treat, but data persistence is reward enough!",
  "Bark bark! Looking for records? I'll <em>retriever</em>! No data will escape my long nose!",
  "<b>*Tail wagging intensifies!*</b> Your database skills are simply <em>im-paw-sible</em> to ignore!",
  "Ruff day? Don't worry - I'll help you <em>dig</em> up those records! No bone left unturned!",
  "WOOF WOOF! Can we run that query NOW? I'm practically <em>bouncing</em> off the database walls! Let's SELECT * FROM everything!",
  "<b>*Spinning in circles with excitement!*</b> Come ON! Let's INSERT some records! I'm so hyper I could <em>lab-rador</em> all day long!",
  "Bark! Bark! BARK! Why are we going so slow?! I want to <em>hound</em> that database until it gives us ALL the data! Speed is my <em>breed</em>!",
  "<b>*Panting with excitement!*</b> Quick! Quick! Let me <em>fetch</em> those joins before my energy runs out! I'm more excited than a <em>bull-dog</em> with a new chew toy!",
  "RUFF! Can't we just run ALL the queries at once?! I'm so eager I'm getting <em>mutts</em> just thinking about it! Let's make this database <em>pup-roductive</em>!",
];

const sleepyDogMessages = [
  "<b>*Yawn...*</b> Still working on that database? Well, I suppose someone has to keep the data <em>well-trained</em>...",
  "<b>*Stretches lazily...*</b> Unlike my energetic cousin, I prefer the <em>slow retrieval</em> approach to data...",
  "<b>*Takes a long nap...*</b> Wake me when you need to <em>fetch</em> something important... zzz...",
  "<b>*Blinks slowly...*</b> Your database operations are making me <em>dog-tired</em>... but keep going...",
  "<b>*Yawns again...*</b> At least you're not <em>barking</em> up the wrong tree with your queries...",
  "<b>*Rolls eyes sleepily...*</b> Ugh, my cousin Rodney is at it again... 'WOOF WOOF LET'S OPTIMIZE EVERYTHING!' He's such a <em>work-a-holic</em> terrier...",
  "<b>*Sighs heavily...*</b> Rodney thinks every query is a <em>golden</em> opportunity... Honestly, he's more <em>re-pup-etitive</em> than a broken record player...",
  "<b>*Stretches and yawns...*</b> That hyperactive cousin of mine is so <em>bor-ing</em>... Get it? <em>Boring</em>? All work and no <em>paws</em> for rest...",
  "<b>*Mumbles sleepily...*</b> Rodney's idea of fun is running <em>lab-oratory</em> tests on database performance... What a <em>hound-dog</em> workaholic...",
  "<b>*Yawns extensively...*</b> Sometimes I think Rodney needs to learn to <em>chill-huahua</em>... Life's not all about being the <em>top dog</em> in productivity, you know...",
];

export const Home = () => {
  const [selectedOperation, setSelectedOperation] = useState("GET");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [prefix, setPrefix] = useState("");
  const [minKey, setMinKey] = useState("");
  const [maxKey, setMaxKey] = useState("");
  const [pageNumber, setPageNumber] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [validationError, setValidationError] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [dataLost, setDataLost] = useState(false);
  const [dogMessage, setDogMessage] = useState("");
  const [sleepyMessage, setSleepyMessage] = useState("");
  const [isDogHovered, setIsDogHovered] = useState(false);
  const [isSleepyDogHovered, setIsSleepyDogHovered] = useState(false);
  const [operations, setOperations] = useState([]);
  const [stats, setStats] = useState({
    gets: 0,
    puts: 0,
    deletes: 0,
    scans: 0,
    iterates: 0,
    errors: 0,
  });
  const containerRef = React.useRef(null);
  const { navbarHeight } = useContext(NavbarContext);

  useEffect(() => window.scrollTo(0, 0), []);

  useEffect(() => {
    const fillScreenVertically = () => {
      if (containerRef.current) {
        containerRef.current.style.minHeight = `calc(100vh - ${navbarHeight}px)`;
      }
    };
    fillScreenVertically();
    setTimeout(fillScreenVertically, 100);
  }, [navbarHeight]);

  useEffect(() => {
    checkDataLoss();
  }, []);

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

  const checkDataLoss = async () => {
    try {
      const lost = await IsDataLost();
      setDataLost(lost);
    } catch (err) {
      console.error("Error checking data loss:", err);
    }
  };

  const addOperation = (type, key, success, message) => {
    const operation = {
      id: Date.now(),
      type,
      key,
      success,
      message,
      timestamp: new Date().toLocaleTimeString(),
    };
    setOperations((prev) => [operation, ...prev.slice(0, 4)]);
  };

  const validateOperation = () => {
    switch (selectedOperation) {
      case "GET":
      case "DELETE":
        if (!key.trim()) {
          return selectedOperation === "GET"
            ? "Please enter a key to fetch!"
            : "Please enter a key to delete!";
        }
        break;
      case "PUT":
        if (!key.trim() || !value.trim()) {
          return "Please enter both key and value!";
        }
        break;
      case "PREFIX_SCAN":
      case "PREFIX_ITERATE":
        if (!prefix.trim()) {
          return selectedOperation === "PREFIX_SCAN"
            ? "Please enter a prefix to scan!"
            : "Please enter a prefix to iterate!";
        }
        break;
      case "RANGE_SCAN":
      case "RANGE_ITERATE":
        if (!minKey.trim() || !maxKey.trim()) {
          return "Please enter both minimum and maximum keys!";
        }
        if (minKey.trim() > maxKey.trim()) {
          return "Minimum key cannot be greater than maximum key!";
        }
        break;
    }
    return null;
  };

  const handleGet = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const record = await Get(key);
      if (record) {
        const resultText = `Found record: ${JSON.stringify(record, null, 2)}`;
        setResult(resultText);
        addOperation("GET", key, true, "Record found");
        setStats((prev) => ({ ...prev, gets: prev.gets + 1 }));
      } else {
        setResult("Record not found - maybe it's buried in another database!");
        addOperation("GET", key, false, "Record not found");
      }
    } catch (err) {
      const errorMsg = `Error getting key: ${err}`;
      setError(errorMsg);
      addOperation("GET", key, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePut = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      await Put(key, value);
      setResult(`Successfully stored record with key: ${key}`);
      addOperation("PUT", key, true, "Record stored");
      setStats((prev) => ({ ...prev, puts: prev.puts + 1 }));

      // Clear inputs after successful put
      setKey("");
      setValue("");
    } catch (err) {
      const errorMsg = `Error storing record: ${err}`;
      setError(errorMsg);
      addOperation("PUT", key, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleDelete = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      await Delete(key);
      setResult(`Successfully deleted record with key: ${key}`);
      addOperation("DELETE", key, true, "Record deleted");
      setStats((prev) => ({ ...prev, deletes: prev.deletes + 1 }));
      setKey("");
    } catch (err) {
      const errorMsg = `Error deleting record: ${err}`;
      setError(errorMsg);
      addOperation("DELETE", key, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePrefixScan = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const records = await PrefixScan(prefix, pageNumber, pageSize);
      const resultText = `Prefix scan results (page ${pageNumber}):\n${JSON.stringify(
        records,
        null,
        2
      )}`;
      setResult(resultText);
      addOperation(
        "PREFIX_SCAN",
        prefix,
        true,
        `Found ${records.length} records`
      );
      setStats((prev) => ({ ...prev, scans: prev.scans + 1 }));
    } catch (err) {
      const errorMsg = `Error in prefix scan: ${err}`;
      setError(errorMsg);
      addOperation("PREFIX_SCAN", prefix, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleRangeScan = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const records = await RangeScan(minKey, maxKey, pageNumber, pageSize);
      const resultText = `Range scan results (page ${pageNumber}):\n${JSON.stringify(
        records,
        null,
        2
      )}`;
      setResult(resultText);
      addOperation(
        "RANGE_SCAN",
        `${minKey}-${maxKey}`,
        true,
        `Found ${records.length} records`
      );
      setStats((prev) => ({ ...prev, scans: prev.scans + 1 }));
    } catch (err) {
      const errorMsg = `Error in range scan: ${err}`;
      setError(errorMsg);
      addOperation("RANGE_SCAN", `${minKey}-${maxKey}`, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePrefixIterate = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const iterator = await PrefixIterate(prefix);
      setResult(
        `Created prefix iterator for: ${prefix}\nUse next() and stop() methods to control iteration.`
      );
      addOperation("PREFIX_ITERATE", prefix, true, "Iterator created");
      setStats((prev) => ({ ...prev, iterates: prev.iterates + 1 }));
    } catch (err) {
      const errorMsg = `Error creating prefix iterator: ${err}`;
      setError(errorMsg);
      addOperation("PREFIX_ITERATE", prefix, false, err.toString());
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleRangeIterate = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);

    try {
      const iterator = await RangeIterate(minKey, maxKey);
      setResult(
        `Created range iterator for: ${minKey} to ${maxKey}\nUse next() and stop() methods to control iteration.`
      );
      addOperation(
        "RANGE_ITERATE",
        `${minKey}-${maxKey}`,
        true,
        "Iterator created"
      );
      setStats((prev) => ({ ...prev, iterates: prev.iterates + 1 }));
    } catch (err) {
      const errorMsg = `Error creating range iterator: ${err}`;
      setError(errorMsg);
      addOperation(
        "RANGE_ITERATE",
        `${minKey}-${maxKey}`,
        false,
        err.toString()
      );
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleExecute = () => {
    // Clear any previous validation errors
    setValidationError(null);

    // Validate the operation
    const validation = validateOperation();
    if (validation) {
      setValidationError(validation);
      return;
    }

    switch (selectedOperation) {
      case "GET":
        handleGet();
        break;
      case "PUT":
        handlePut();
        break;
      case "DELETE":
        handleDelete();
        break;
      case "PREFIX_SCAN":
        handlePrefixScan();
        break;
      case "RANGE_SCAN":
        handleRangeScan();
        break;
      case "PREFIX_ITERATE":
        handlePrefixIterate();
        break;
      case "RANGE_ITERATE":
        handleRangeIterate();
        break;
    }
  };

  const getOperationIcon = () => {
    switch (selectedOperation) {
      case "GET":
        return <FaSearch />;
      case "PUT":
        return <FaPlus />;
      case "DELETE":
        return <FaTrash />;
      case "PREFIX_SCAN":
      case "RANGE_SCAN":
        return <FaList />;
      case "PREFIX_ITERATE":
      case "RANGE_ITERATE":
        return <FaArrowRight />;
      default:
        return <FaDatabase />;
    }
  };

  const getOperationText = () => {
    switch (selectedOperation) {
      case "GET":
        return isLoading ? "Sniffing..." : "Fetch Record";
      case "PUT":
        return isLoading ? "Saving..." : "Save Record";
      case "DELETE":
        return isLoading ? "Digging up..." : "Delete Record";
      case "PREFIX_SCAN":
        return isLoading ? "Scanning..." : "Prefix Scan";
      case "RANGE_SCAN":
        return isLoading ? "Scanning..." : "Range Scan";
      case "PREFIX_ITERATE":
        return isLoading ? "Creating..." : "Create Iterator";
      case "RANGE_ITERATE":
        return isLoading ? "Creating..." : "Create Iterator";
      default:
        return "Execute";
    }
  };

  const renderInputFields = () => {
    switch (selectedOperation) {
      case "GET":
      case "DELETE":
        return (
          <div>
            <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
              🔑 Key{" "}
              {selectedOperation === "GET"
                ? "(What are we looking for?)"
                : "(What are we trying to delete?)"}
            </label>
            <input
              type="text"
              placeholder="Enter your ...woof.. key!"
              value={key}
              onChange={(e) => setKey(e.target.value)}
              className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
              disabled={isLoading}
            />
          </div>
        );
      case "PUT":
        return (
          <div className="grid md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                🔑 Key (The map to the treasure...)
              </label>
              <input
                type="text"
                placeholder="Enter your ...woof.. key!"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
                disabled={isLoading}
              />
            </div>
            <div>
              <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                📝 Value (The treasure to stash!)
              </label>
              <textarea
                placeholder="Enter the value... woof!"
                value={value}
                onChange={(e) => setValue(e.target.value)}
                rows={4}
                className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200 resize-vertical"
                disabled={isLoading}
              />
            </div>
          </div>
        );
      case "PREFIX_SCAN":
      case "PREFIX_ITERATE":
        return (
          <div>
            <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
              🎯 Prefix (Starting pattern)
            </label>
            <input
              type="text"
              placeholder="Enter prefix... woof!"
              value={prefix}
              onChange={(e) => setPrefix(e.target.value)}
              className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
              disabled={isLoading}
            />
          </div>
        );
      case "RANGE_SCAN":
      case "RANGE_ITERATE":
        return (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                🏁 Min Key (Range start)
              </label>
              <input
                type="text"
                placeholder="From key..."
                value={minKey}
                onChange={(e) => setMinKey(e.target.value)}
                className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
                disabled={isLoading}
              />
            </div>
            <div>
              <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                🏆 Max Key (Range end)
              </label>
              <input
                type="text"
                placeholder="To key..."
                value={maxKey}
                onChange={(e) => setMaxKey(e.target.value)}
                className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
                disabled={isLoading}
              />
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div
      ref={containerRef}
      className="bg-sloth-yellow-lite/80 p-6 pt-[2.6rem] relative overflow-hidden select-none flex justify-center items-center"
    >
      {/* Background paw prints */}
      <div className="absolute inset-0 opacity-10 overflow-hidden pointer-events-none">
        <FaBone className="absolute top-20 left-10 text-sloth-brown rotate-12 text-6xl" />
        <FaBone className="absolute top-40 right-20 text-sloth-brown -rotate-45 text-4xl" />
        <FaBone className="absolute bottom-32 left-1/4 text-sloth-brown rotate-75 text-5xl" />
        <FaBone className="absolute bottom-20 right-1/3 text-sloth-brown -rotate-12 text-3xl" />
        <FaHeart className="absolute top-60 left-1/2 text-sloth-brown rotate-12 text-4xl" />
      </div>

      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="flex justify-center items-start space-x-5 md:space-x-8 relative">
          <div className="flex-shrink-0">
            <img
              src={Bone}
              alt="Bone"
              className="hidden sm:block h-12 w-auto object-contain flex-shrink-0 rotate-[24deg] mt-2.5"
            />
          </div>
          <div className="text-center mb-8">
            <h1 className="text-3xl sm:text-4xl md:text-5xl font-bold text-sloth-brown-dark mb-4 tracking-wide">
              <i>Dash</i>board
            </h1>
            <p className="text-lg text-sloth-brown font-medium">
              Sit... Fetch... Query... Gooood database!
            </p>

            {dataLost && (
              <div className="mt-4 bg-red-100 border-4 border-red-500 rounded-xl p-4 shadow-[4px_4px_0px_0px_rgba(239,68,68,1)]">
                <div className="flex items-center justify-center gap-2 text-red-800">
                  <FaDog className="text-4xl flex-shrink-0" />
                  <span className="font-bold">
                    Ruff news! Previous data was lost during loading. Starting
                    fresh!
                  </span>
                </div>
              </div>
            )}
          </div>
          <div className="flex-shrink-0">
            <img
              src={RokicaRunning}
              alt="Rokica the dog running"
              className="hidden sm:block h-20 w-auto object-contain flex-shrink-0"
            />
          </div>
        </div>

        <div className="grid lg:grid-cols-3 gap-8 !mt-0 sm:!mt-3">
          {/* Main Operations Panel */}
          <div className="lg:col-span-2 space-y-6">
            {/* Input Section */}
            <div className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] relative overflow-hidden">
              <div className="flex items-center gap-3 mb-6">
                <FaDatabase className="text-2xl text-sloth-brown-dark" />
                <h2 className="text-2xl font-bold text-sloth-brown-dark">
                  Database Operations
                </h2>
              </div>

              {/* Operation Selection */}
              <div className="mb-6">
                <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                  🐕 Operation Type (What's the mission?)
                </label>
                <StyledOperationSelect
                  value={selectedOperation}
                  onChange={(o) => {
                    setValidationError(null);
                    setSelectedOperation(o);
                  }}
                  isDisabled={isLoading}
                />
              </div>

              {/* Dynamic Input Fields */}
              <div className="mb-6">{renderInputFields()}</div>

              <div className="flex flex-wrap justify-between items-center gap-4">
                {/* Validation Error */}
                {validationError && (
                  <div className="flex items-center gap-2 text-red-600 bg-red-50 px-4 py-2 rounded-lg border-2 border-red-300">
                    <FaDog className="text-lg flex-shrink-0" />
                    <span className="font-medium">{validationError}</span>
                  </div>
                )}

                {/* Spacer when no validation error */}
                {!validationError && <div></div>}

                {/* Execute Button */}
                <button
                  onClick={handleExecute}
                  disabled={isLoading}
                  className="flex items-center gap-2 px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {getOperationIcon()}
                  {getOperationText()}
                </button>
              </div>
            </div>

            {/* Results Section - Only show for execution errors, not validation errors */}
            {(result || error) && !validationError && (
              <div
                className={`rounded-xl p-6 border-4 font-mono text-sm relative overflow-hidden ${
                  error
                    ? "bg-red-50 border-red-500 shadow-[6px_6px_0px_0px_rgba(239,68,68,1)]"
                    : "bg-green-50 border-green-500 shadow-[6px_6px_0px_0px_rgba(34,197,94,1)]"
                }`}
              >
                <div className="flex items-center gap-2 mb-4">
                  <FaDog
                    className={`text-xl ${
                      error ? "text-red-600" : "text-green-600"
                    }`}
                  />
                  <h3
                    className={`font-bold text-lg ${
                      error ? "text-red-800" : "text-green-800"
                    }`}
                  >
                    {error
                      ? "Woof! Something went wrong!"
                      : "Good dog! Operation successful!"}
                  </h3>
                </div>
                <pre
                  className={`whitespace-pre-wrap ${
                    error ? "text-red-700" : "text-green-700"
                  }`}
                >
                  {error || result}
                </pre>

                {/* Result decoration */}
                <FaBone
                  className={`absolute top-2 right-2 text-2xl opacity-20 ${
                    error ? "text-red-400" : "text-green-400"
                  }`}
                />
              </div>
            )}
          </div>

          {/* Side Panel */}
          <div className="space-y-6">
            {/* Stats Panel */}
            <div className="bg-sloth-brown rounded-xl p-6 border-4 border-sloth-brown-dark shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] text-sloth-yellow">
              <div className="flex items-center gap-2 mb-4">
                <FiActivity className="text-xl" />
                <h3 className="font-bold text-lg">Today's Pack Statistics</h3>
              </div>

              <div className="space-y-3">
                <div className="flex justify-between">
                  <span>🔍 Successful Fetches:</span>
                  <span className="font-bold">{stats.gets}</span>
                </div>
                <div className="flex justify-between">
                  <span>💾 Records Saved:</span>
                  <span className="font-bold">{stats.puts}</span>
                </div>
                <div className="flex justify-between">
                  <span>🗑️ Records Deleted:</span>
                  <span className="font-bold">{stats.deletes}</span>
                </div>
                <div className="flex justify-between">
                  <span>📋 Scans Performed:</span>
                  <span className="font-bold">{stats.scans}</span>
                </div>
                <div className="flex justify-between">
                  <span>🔄 Iterators Created:</span>
                  <span className="font-bold">{stats.iterates}</span>
                </div>
                <div className="flex justify-between">
                  <span>❌ Errors:</span>
                  <span className="font-bold">{stats.errors}</span>
                </div>
              </div>
            </div>

            {/* Recent Operations */}
            <div className="bg-white rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)]">
              <h3 className="font-bold text-lg text-sloth-brown-dark mb-4 flex items-center gap-2">
                <FaDog />
                Recent Tail Wags
              </h3>

              <div className="space-y-2">
                {operations.length === 0 ? (
                  <p className="text-sloth-brown italic text-center py-1 -mt-1">
                    No operations yet... ready to fetch some data?
                  </p>
                ) : (
                  operations.map((op) => (
                    <div
                      key={op.id}
                      className={`p-3 rounded-lg border-2 ${
                        op.success
                          ? "bg-green-50 border-green-300"
                          : "bg-red-50 border-red-300"
                      }`}
                    >
                      <div className="flex justify-between items-start text-sm">
                        <div className="flex items-start">
                          <span className="font-bold">{op.type}</span>
                          <span className="text-gray-600 ml-2 inline-block w-32 truncate text-left">
                            {op.key}
                          </span>
                        </div>
                        <span className="text-xs text-gray-500">
                          {op.timestamp}
                        </span>
                      </div>
                      <div
                        className={`text-xs mt-1 ${
                          op.success ? "text-green-600" : "text-red-600"
                        }`}
                      >
                        {op.message}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Dog Tips */}
        <div className="bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed border-sloth-brown rounded-xl p-6 mt-8 max-w-full mx-auto md:max-w-[90%]">
          <div className="flex items-start gap-3">
            <FaDog className="text-2xl text-sloth-brown mt-1 flex-shrink-0" />
            <div>
              <h4 className="text-lg font-bold text-sloth-brown-dark mb-2">
                🐕 Pro Puppy Tips
              </h4>
              <p className="text-sloth-brown leading-relaxed mr-3">
                <strong>Woof! From the pack:</strong> Rodney's always ready to{" "}
                <em>fetch</em> your data lightning fast, while Del Boy prefers
                the <em>paws-ed</em> approach to database operations. Whether
                you're <em>retrieving</em> records or <em>lab-oring</em> over
                complex scans, remember - every good query deserves a treat! 🦴
              </p>
            </div>
          </div>
        </div>
      </div>

      <Tooltip
        anchorSelect=".hyper-dog"
        place="left-start"
        delayShow={350}
        offset={12}
        opacity={1}
        className="!bg-white !p-4 !rounded-xl !z-[9999] !max-w-sm border-2 border-sloth-brown shadow-[4px_4px_0px_0px_#6b5e4a]"
        border="3px solid #4b4436"
        globalCloseEvents={[
          "scroll",
          "mouseout",
          "mouseleave",
          "click",
          "dblclick",
          "mouseup",
          "mouseenter",
        ]}
      >
        <div className="flex items-start">
          <div>
            <p className="font-semibold text-sloth-brown mb-2 text-lg">
              Rodney says:
            </p>
            <p
              className="text-gray-600 italic"
              dangerouslySetInnerHTML={{ __html: dogMessage }}
            />
          </div>
        </div>
      </Tooltip>

      {/* Tooltip for Sleepy Database Dog */}
      <Tooltip
        anchorSelect=".sleepy-dog"
        place="right-start"
        delayShow={775}
        offset={12}
        opacity={1}
        className="!bg-white !p-4 !rounded-xl !z-[9999] !max-w-sm border-2 border-sloth-brown shadow-[4px_4px_0px_0px_#6b5e4a]"
        border="3px solid #4b4436"
        globalCloseEvents={[
          "scroll",
          "mouseout",
          "mouseleave",
          "click",
          "dblclick",
          "mouseup",
          "mouseenter",
        ]}
      >
        <div className="flex items-start">
          <div>
            <p className="font-semibold text-sloth-brown mb-2 text-lg">
              Rodney's lazy cousin Del Boy:
            </p>
            <p
              className="text-gray-600 italic"
              dangerouslySetInnerHTML={{ __html: sleepyMessage }}
            />
          </div>
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
        alt="Rodney's Sleepy Cousin"
        className="sleepy-dog hidden sm:block absolute -bottom-2 -left-2 w-auto h-[7.5rem] hover:h-[9.25rem] -rotate-[28deg] hover:rotate-1 object-contain transform ease-in -translate-x-1/4 translate-y-1/4 opacity-95 transition-all duration-[925ms] hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer"
        onMouseEnter={() => setIsSleepyDogHovered(true)}
        onMouseLeave={() => setIsSleepyDogHovered(false)}
      />
    </div>
  );
};

export default Home;

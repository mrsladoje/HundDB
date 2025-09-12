import { NavbarContext } from "@/context/NavbarContext";
import { Get, IsDataLost, Put } from "@wails/main/App.js";
import React, { useContext, useEffect, useState } from "react";
import {
  FaBone,
  FaDatabase,
  FaDog,
  FaHeart,
  FaPlus,
  FaSearch,
} from "react-icons/fa";
import { FiActivity } from "react-icons/fi";

// Dog images - you'll need to add these to your pics folder
const HappyDogPic = "../../pics/rokica.png"; // Replace with actual dog image
const SleepyDogPic = "../../pics/rokica.png"; // Replace with actual dog image
const RokicaRunning = "../../pics/rokica_running.png";
const Bone = "../../pics/bone.png";

const dogMessages = [
  "Woof! Time to fetch some data from the database! No need to <em>paws</em> - your queries are in good hands!",
  "Good boy/girl! That's some <em>paw-some</em> database work! I'd give you a treat, but data persistence is reward enough!",
  "Bark bark! Looking for records? I'm your <em>retriever</em>! No data will escape my nose!",
  "Tail wagging intensifies! Your database skills are simply <em>im-paw-sible</em> to ignore!",
  "Ruff day? Don't worry - I'll help you <em>dig</em> up those records! No bone left unturned!",
];

const sleepyDogMessages = [
  "Yawn... Still working on that database? Well, I suppose someone has to keep the data <em>well-trained</em>...",
  "Stretches lazily... Unlike my energetic friend, I prefer the <em>slow retrieval</em> approach to data...",
  "Takes a long nap... Wake me when you need to <em>fetch</em> something important... zzz...",
  "Blinks slowly... Your database operations are making me <em>dog-tired</em>... but keep going...",
  "Yawns again... At least you're not <em>barking</em> up the wrong tree with your queries...",
];

export const Home = () => {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [dataLost, setDataLost] = useState(false);
  const [dogMessage, setDogMessage] = useState("");
  const [sleepyMessage, setSleepyMessage] = useState("");
  const [isDogHovered, setIsDogHovered] = useState(false);
  const [isSleepyDogHovered, setIsSleepyDogHovered] = useState(false);
  const [operations, setOperations] = useState([]);
  const [stats, setStats] = useState({ gets: 0, puts: 0, errors: 0 });
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

  const handleGet = async () => {
    if (!key.trim()) {
      setError("Please enter a key to fetch");
      return;
    }

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
    if (!key.trim() || !value.trim()) {
      setError("Please enter both key and value");
      return;
    }

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

              <div className="grid md:grid-cols-2 gap-4 mb-6">
                <div>
                  <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                    🔑 Key (What are we looking for?)
                  </label>
                  <input
                    type="text"
                    placeholder="Enter your key... good dog!"
                    value={key}
                    onChange={(e) => setKey(e.target.value)}
                    className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[4px_4px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[-1px] focus:translate-y-[-1px] transition-all duration-200"
                    disabled={isLoading}
                  />
                </div>

                <div>
                  <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                    📝 Value (The treasure to bury!)
                  </label>
                  <input
                    type="text"
                    placeholder="Enter your value... such a good value!"
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[4px_4px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[-1px] focus:translate-y-[-1px] transition-all duration-200"
                    disabled={isLoading}
                  />
                </div>
              </div>

              <div className="flex flex-wrap justify-end gap-4">
                <button
                  onClick={handleGet}
                  disabled={isLoading}
                  className="flex items-center gap-2 px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <FaSearch />
                  {isLoading ? "Sniffing..." : "Fetch Record"}
                </button>

                <button
                  onClick={handlePut}
                  disabled={isLoading}
                  className="flex items-center gap-2 px-6 py-3 bg-sloth-brown text-sloth-yellow font-bold rounded-lg border-4 border-sloth-brown-dark shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <FaPlus />
                  {isLoading ? "Burying..." : "Bury Record"}
                </button>
              </div>
            </div>

            {/* Results Section */}
            {(result || error) && (
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
                  <span>💾 Records Buried:</span>
                  <span className="font-bold">{stats.puts}</span>
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
                  <p className="text-sloth-brown italic text-center py-4">
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
                        <div>
                          <span className="font-bold">{op.type}</span>
                          <span className="text-gray-600 ml-2">{op.key}</span>
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
        <div className="bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed border-sloth-brown rounded-xl p-6 mt-8">
          <div className="flex items-start gap-3">
            <FaDog className="text-2xl text-sloth-brown mt-1 flex-shrink-0" />
            <div>
              <h4 className="text-lg font-bold text-sloth-brown-dark mb-2">
                🐕 Pro Puppy Tips
              </h4>
              <p className="text-sloth-brown leading-relaxed">
                <strong>Training your database:</strong> Use GET to retrieve
                records (like fetching a stick!), PUT to store new data (like
                burying a bone). The backend handles all the heavy lifting
                automatically - no need to worry about <em>ruff</em> details!
                Remember, every good database needs regular maintenance, just
                like every good dog needs daily walks! 🦴
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Tooltips for dogs */}
      {isDogHovered && (
        <div className="fixed bottom-20 right-4 bg-white p-4 rounded-xl border-4 border-green-500 shadow-[4px_4px_0px_0px_rgba(34,197,94,1)] z-50 max-w-sm">
          <div className="flex items-start">
            <div>
              <p className="font-bold text-green-600 mb-2 text-sm">
                🐕 Database Dog says:
              </p>
              <p
                className="text-gray-600 text-sm italic"
                dangerouslySetInnerHTML={{ __html: dogMessage }}
              />
            </div>
          </div>
        </div>
      )}

      {isSleepyDogHovered && (
        <div className="fixed bottom-20 left-4 bg-white p-4 rounded-xl border-4 border-gray-500 shadow-[4px_4px_0px_0px_rgba(107,114,128,1)] z-50 max-w-sm">
          <div className="flex items-start">
            <div>
              <p className="font-bold text-gray-600 mb-2 text-sm">
                😴 Lazy Database Dog says:
              </p>
              <p
                className="text-gray-600 text-sm italic"
                dangerouslySetInnerHTML={{ __html: sleepyMessage }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Peeking Dogs */}
      <img
        src={HappyDogPic}
        alt="Happy Database Dog"
        className="absolute -bottom-2 -right-2 w-20 h-20 object-contain transform translate-x-1/4 translate-y-1/4 opacity-90 transition-all duration-500 hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer"
        onMouseEnter={() => setIsDogHovered(true)}
        onMouseLeave={() => setIsDogHovered(false)}
      />
      <img
        src={SleepyDogPic}
        alt="Sleepy Database Dog"
        className="absolute -bottom-2 -left-2 w-16 h-16 object-contain transform -translate-x-1/4 translate-y-1/4 opacity-80 transition-all duration-700 hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer"
        onMouseEnter={() => setIsSleepyDogHovered(true)}
        onMouseLeave={() => setIsSleepyDogHovered(false)}
      />
    </div>
  );
};

export default Home;

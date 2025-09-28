import { BgDecorations } from "@/components/home/BgDecorations";
import DashboardSign from "@/components/home/DashboardSign";
import { FileUpload } from "@/components/home/FileUpload";
import RecentOperations from "@/components/home/RecentOperations";
import Result from "@/components/home/Result";
import Stats from "@/components/home/Stats";
import StyledOperationSelect from "@/components/select/StyledOperationSelect";
import { NavbarContext } from "@/context/NavbarContext";
import {
  encodeValueWithType,
  getFileTypeFromFile,
} from "@/utils/fileTypeEncoder.js";
import {
  Delete,
  Get,
  IsDataLost,
  PrefixIterate,
  PrefixScan,
  Put,
  RangeIterate,
  RangeScan,
} from "@wails/main/App.js";
import React, { useContext, useEffect, useState } from "react";
import {
  FaArrowRight,
  FaDatabase,
  FaDog,
  FaList,
  FaRegSave,
  FaRegTrashAlt,
  FaSearch,
} from "react-icons/fa";
import { FaFont } from "react-icons/fa6";
import { MdPermMedia } from "react-icons/md";
import { Tooltip } from "react-tooltip";

const RokicaLeft = "../../pics/rokica_left.png";
const HyperRokica = "../../pics/rokica_hyper.png";
const SleepyCousin = "../../pics/rokica_sleepy.png";
const SleepingCousin = "../../pics/rokica_sleeping.png";

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

const dogErrorMessages = {
  GET: [
    "🐕 Woof! I sniffed everywhere but couldn't find that key. Maybe it's buried in someone else's yard?",
    "🔍 I'm a good dog, but even my nose couldn't track down that record!",
    "🦴 That key must be hidden better than my favorite bone!",
    "🐾 I searched high and low, but that data is playing hide-and-seek!",
    "🎾 Tried to fetch that record but it bounced right out of my paws!",
  ],
  PUT: [
    "🐕 Ruff! Couldn't bury that record properly. Maybe the database yard is full?",
    "💾 Something went wrong while trying to stash your treasure!",
    "🦴 Failed to hide that bone... I mean data... in the database!",
    "🐾 My paws slipped while trying to save that record!",
    "🎾 The data bounced right back instead of staying put!",
  ],
  DELETE: [
    "🗑️ Couldn't dig up that record to delete it. Maybe it's already gone?",
    "🐕 Woof! That key is being stubborn and won't come out of its hiding spot!",
    "🦴 Tried to unbury that record to delete it but my paws came up empty!",
    "🎾 That data is playing hard to get and won't be deleted!",
    "🐾 Scratched around but couldn't remove that pesky record!",
  ],
  SCAN: [
    "📋 My scanning nose got confused halfway through the search!",
    "🔍 The database trail went cold during the scan!",
    "🐕 Woof! Got distracted by a squirrel mid-scan... I mean, encountered an error!",
    "🦴 The scan bone broke before I could finish digging!",
    "🎾 Tried to scan but the data kept bouncing around!",
  ],
  ITERATE: [
    "🔄 My iterator got its leash tangled up!",
    "🐾 Tried to create an iterator but my paws got tied up!",
    "🎾 The iterator bounced away before I could catch it!",
    "🦴 Something chewed through my iterator connection!",
    "🐕 Woof! Iterator creation went to the dogs... literally!",
  ],
};

const dogNotFoundMessages = {
  GET: [
    "🐕 After a thorough sniff investigation, I can confirm: that key definitely doesn't exist!",
    "🔍 I've checked every corner of the database yard - that record is absolutely not there!",
    "🦴 My nose is never wrong! That key is as missing as my buried bones from last winter!",
    "🐾 I followed every data trail and I'm paw-sitive that record doesn't exist!",
    "🎾 Searched high, low, and everywhere in between - that key is nowhere to be hound!",
  ],
  DELETE: [
    "🗑️ Well, that's easy! Can't delete what was never there to begin with! Mission accomplished?",
    "🐕 Woof! Tried to dig up that record but the hole was already empty!",
    "🦴 That key was as gone as last week's buried bone - nothing to delete!",
    "🎾 Can't fetch what was never thrown! That record didn't exist anyway!",
    "🐾 Good news: that unwanted data was already not cluttering up the database!",
  ],
  SCAN: [
    "📋 Scanned the entire database neighborhood - no records match that pattern!",
    "🔍 My super-sniffer confirms: zero records found with that criteria!",
    "🐕 Woof! Searched every database tree, but no matching records were hiding there!",
    "🦴 Not even a single bone... I mean record... matches what you're looking for!",
    "🎾 The scan came back squeaky clean - no matching data anywhere!",
  ],
  ITERATE: [
    "🔄 Iterator created successfully, but it's got nothing to iterate over! Empty results!",
    "🐾 Ready to walk through the data, but the path is completely empty!",
    "🎾 Iterator is all set up, but there are no records to chase around!",
    "🦴 Created your iterator, but there are no bones... er, records... to find!",
    "🐕 Woof! Iterator is ready to go, but the search area is completely vacant!",
  ],
};

// TODO: The Results.jsx changes as we change the input. That is obviously bad.
// This will be fixed when we add concurrency. Then we won't rely on useStates for
// result/error/notFoundMessage/etc. but rather on the operations array only.
// We will just track the current operation.
// We should also add a unique ID to each operation, so we can track them better.

export const Home = () => {
  const [selectedOperation, setSelectedOperation] = useState("GET");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [valueTab, setValueTab] = useState("text"); // "text" or "media"
  const [uploadedFile, setUploadedFile] = useState(null);
  const [prefix, setPrefix] = useState("");
  const [minKey, setMinKey] = useState("");
  const [maxKey, setMaxKey] = useState("");
  const [pageNumber, setPageNumber] = useState(1);
  const [pageSize, setPageSize] = useState(5);
  const [result, setResult] = useState(null);
  const [notFoundMessage, setNotFoundMessage] = useState(null);
  const [error, setError] = useState(null);
  const [validationError, setValidationError] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [dataLost, setDataLost] = useState(false);
  const [dogMessage, setDogMessage] = useState("");
  const [sleepyMessage, setSleepyMessage] = useState("");
  const [isDogHovered, setIsDogHovered] = useState(false);
  const [isSleepyDogHovered, setIsSleepyDogHovered] = useState(false);
  const [operations, setOperations] = useState([]);
  const [activeOperationId, setActiveOperationId] = useState(null);
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

  const addOperation = (
    type,
    key,
    success,
    message,
    notFoundMessage = null,
    resultData = null,
    currentKey = null,
    ended = false,
    currentRecord = null,
  prefix = null,
  extra = null
  ) => {
    const operation = {
      id: Date.now(),
      type,
      key,
      success,
      message,
      notFoundMessage,
      resultData,
      currentKey,
      ended,
      currentRecord,
      prefix,
      timestamp: new Date().toLocaleTimeString(),
    };
  const opWithExtras = extra && typeof extra === "object" ? { ...operation, ...extra } : operation;
  setOperations((prev) => [opWithExtras, ...prev.slice(0, 14)]); // Keep only last 15 operations
  setActiveOperationId(operation.id);
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
        if (!key.trim()) {
          return "Please enter a key!";
        }
        if (valueTab === "text" && !value.trim()) {
          return "Please enter a value or switch to media tab to upload a file!";
        }
        if (valueTab === "media" && !uploadedFile) {
          return "Please upload a file or switch to text tab to enter a value!";
        }
        break;
      case "PREFIX_SCAN":
        if (pageSize < 1 || pageSize > 20) {
          return "Page size must be between 1 and 20!";
        }
        {
          const pn = Number(pageNumber);
          if (!Number.isFinite(pn) || pn < 1) {
            return "Page number must be at least 1!";
          }
        }
        break;
    }
    return null;
  };

  const handleGet = async () => {
    setError(null);
    setResult(null);
    setNotFoundMessage(null);
    setIsLoading(true);

    try {
      const record = await Get(key);
      if (record) {
        const resultText = `Found record: ${JSON.stringify(record, null, 2)}`;
        setResult(resultText);
        addOperation("GET", key, true, "Record found", null, resultText);
        setStats((prev) => ({ ...prev, gets: prev.gets + 1 }));
      } else {
        const notFoundMessage = getRandomDogNotFound("GET");
        setNotFoundMessage(notFoundMessage);
        addOperation("GET", key, false, null, notFoundMessage, notFoundMessage);
      }
    } catch (err) {
      const dogError = getRandomDogError("GET");
      setError(dogError);
      addOperation("GET", key, false, dogError);
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const fileToByteArray = async (file) => {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = function (event) {
        const arrayBuffer = event.target.result;
        const uint8Array = new Uint8Array(arrayBuffer);
        const byteString = Array.from(uint8Array).join(",");
        resolve(byteString);
      };
      reader.readAsArrayBuffer(file);
    });
  };

  const handlePut = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);
    setNotFoundMessage(null);

    try {
      let finalValue;
      let fileType = "";

      if (valueTab === "media" && uploadedFile) {
        // Handle file upload
        fileType = getFileTypeFromFile(uploadedFile);
        const byteArray = await fileToByteArray(uploadedFile);
        finalValue = encodeValueWithType(byteArray, fileType);
      } else {
        // Handle text input
        finalValue = encodeValueWithType(value, ""); // Empty string for text
      }

      await Put(key, finalValue);
      const resultText = `Successfully stored record with key: ${key}`;
      setResult(resultText);
      addOperation("PUT", key, true, "Record stored", null, resultText);
      setStats((prev) => ({ ...prev, puts: prev.puts + 1 }));

      // Clear inputs after successful put
      setKey("");
      setValue("");
      setUploadedFile(null);
      setValueTab("text");
    } catch (err) {
      console.error("Put operation failed:", err);
      const dogError = getRandomDogError("PUT");
      setError(dogError);
      addOperation("PUT", key, false, dogError, null, dogError);
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handleDelete = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);
    setNotFoundMessage(null);

    try {
      const result = await Delete(key);

      // The Go function returns a boolean indicating if the key existed
      if (result === true) {
        const resultText = `Successfully deleted record with key: ${key}`;
        setResult(resultText);
        addOperation("DELETE", key, true, "Record deleted", null, resultText);
        setStats((prev) => ({ ...prev, deletes: prev.deletes + 1 }));
      } else {
        const notFoundMessage = getRandomDogNotFound("DELETE");
        setNotFoundMessage(notFoundMessage);
        addOperation(
          "DELETE",
          key,
          false,
          null,
          notFoundMessage,
          notFoundMessage
        );
      }

      setKey(""); // Clear the key input after operation
    } catch (err) {
      console.error("Delete operation failed:", err);
      const dogError = getRandomDogError("DELETE");
      setError(dogError);
      addOperation("DELETE", key, false, dogError);
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePrefixScan = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);
    setNotFoundMessage(null);

    try {
      const pn = Number(pageNumber) || 1;
      const keys = await PrefixScan(prefix, pageSize, pn - 1);

      const isEmpty = !keys || keys.length === 0;
      const notFoundMsg = isEmpty
        ? getRandomDogNotFound("SCAN")
        : null;

      // If nothing found on initial scan: mark as notFound (yellow UI)
      if (isEmpty) {
        setNotFoundMessage(notFoundMsg);
      }

      addOperation(
        "PREFIX_SCAN",
        prefix,
        !isEmpty,
        isEmpty
          ? null
          : `Found ${keys.length} records on page ${pn}`,
        isEmpty ? notFoundMsg : null,
        null,
        null,
        false,
        null,
        prefix,
        { pageSize, currentPage: pn, keys, paginationError: false }
      );
      setStats((prev) => ({ ...prev, scans: prev.scans + 1 }));

      // Set result to trigger the table display and card coloring
      setResult(
        isEmpty
          ? "No records found for this prefix."
          : `Prefix scan completed: ${keys.length} records found`
      );
    } catch (err) {
      const dogError = getRandomDogError("SCAN");
      setError(dogError);
      addOperation("SCAN", prefix, false, dogError);
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePrefixScanPageChange = async (newPage, newPageSize = null) => {
    const currentOperation = operations.find(
      (op) => op.type === "PREFIX_SCAN" && op.prefix === prefix
    );
    if (!currentOperation) return;

    try {
      const effectivePageSize = newPageSize || currentOperation.pageSize;
      const keys = await PrefixScan(prefix, effectivePageSize, Math.max(0, newPage - 1));

  // Update the existing operation instead of creating a new one
      setOperations((prev) =>
        prev.map((op) => {
          if (op.id === currentOperation.id) {
    const hadResultsBefore = Array.isArray(currentOperation.keys) && currentOperation.keys.length > 0;
    const paginationError = hadResultsBefore && keys.length === 0;
            return {
              ...op,
              currentPage: newPage,
              pageSize: effectivePageSize,
              keys: keys,
              message: `Found ${keys.length} records on page ${newPage}`,
              timestamp: new Date().toLocaleTimeString(),
              paginationError,
            };
          }
          return op;
        })
      );

  // Pagination empties should NOT turn the whole card yellow
  setNotFoundMessage(null);
  setResult(`Prefix scan completed: ${keys.length} records found`);
    } catch (err) {
      console.error("Error changing page:", err);
    }
  };

  const handleRangeScan = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);
    setNotFoundMessage(null);

    try {
  const pn = Number(pageNumber) || 1;
  const records = await RangeScan(minKey, maxKey, pn, pageSize);
  const resultText = `Range scan results (page ${pn}):\n${JSON.stringify(
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
      const dogError = getRandomDogError("SCAN");
      setError(dogError);
      addOperation("SCAN", `${minKey}-${maxKey}`, false, dogError);
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  /**
   * Finds the lexicographically smaller string that is as close as possible
   * to the given string using the full UTF-8 character set.
   * If the input is empty, returns the empty string itself.
   *
   * @param {string} str The input string.
   * @returns {string} The lexicographically smaller string, or empty string if input is empty.
   */
  function findLexicographicallySmaller(str) {
    // If empty string, return empty string (can't get smaller)
    if (str.length === 0) {
      return "";
    }

    // Convert the string to a mutable array of characters.
    const arr = str.split("");
    const n = arr.length;

    // Try to find a position where we can decrement a character
    for (let i = n - 1; i >= 0; i--) {
      const currentCharCode = arr[i].charCodeAt(0);

      // If we can decrement this character (not at minimum UTF-8 value)
      if (currentCharCode > 0) {
        // Decrement the character
        arr[i] = String.fromCharCode(currentCharCode - 1);

        // Set all characters after this position to the maximum UTF-8 character
        // to get the lexicographically largest suffix, making the overall string
        // as close as possible to the original
        for (let j = i + 1; j < n; j++) {
          arr[j] = String.fromCharCode(0x10ffff); // Maximum Unicode code point
        }

        return arr.join("");
      }
      // If current character is at minimum (charCode 0), we continue to the next position
    }

    // If we get here, all characters were at minimum value (all char code 0)
    // The only string smaller would be a shorter string
    // Return the string with the last character removed, and set remaining chars to max
    if (n === 1) {
      return ""; // Single character at minimum becomes empty string
    }

    const result = new Array(n - 1);
    for (let i = 0; i < n - 1; i++) {
      result[i] = String.fromCharCode(0x10ffff);
    }

    return result.join("");
  }

  const handlePrefixIterate = async () => {
    setError(null);
    setResult(null);
    setIsLoading(true);
    setNotFoundMessage(null);

    try {
      const record = await PrefixIterate(
        prefix,
        findLexicographicallySmaller(prefix)
      );

      if (record) {
        const resultText = `Found record: ${JSON.stringify(record, null, 2)}`;
        setResult(resultText);
        addOperation(
          "PREFIX_ITERATE",
          prefix,
          true,
          "Iterator created",
          null,
          resultText,
          record.key,
          false,
          record,
          prefix
        );
      } else {
        const notFoundMessage = getRandomDogNotFound("ITERATE");
        setNotFoundMessage(notFoundMessage);
        addOperation(
          "PREFIX_ITERATE",
          prefix,
          false,
          null,
          notFoundMessage,
          notFoundMessage,
          prefix,
          true,
          null,
          prefix
        );
      }
      setStats((prev) => ({ ...prev, iterates: prev.iterates + 1 }));
    } catch (err) {
      const dogError = getRandomDogError("ITERATE");
      setError(dogError);
      addOperation(
        "PREFIX_ITERATE",
        prefix,
        false,
        dogError,
        null,
        dogError,
        prefix,
        true,
        null,
        prefix
      );
      setStats((prev) => ({ ...prev, errors: prev.errors + 1 }));
    } finally {
      setIsLoading(false);
    }
  };

  const handlePrefixIteratorNext = async (operation) => {
    if (operation.ended) return;

    try {
      const record = await PrefixIterate(operation.key, operation.currentKey);

      // Update the operation in the operations array
      setOperations((prev) =>
        prev.map((op) => {
          if (op.id === operation.id) {
            if (record) {
              const resultText = `Found record: ${JSON.stringify(
                record,
                null,
                2
              )}`;
              return {
                ...op,
                resultData: resultText,
                currentKey: record.key,
                currentRecord: record,
                success: true,
                notFoundMessage: null,
                message: "Next record found",
              };
            } else {
              return {
                ...op,
                ended: true,
                success: false,
                notFoundMessage: "🐾Iterator has reached the end",
                message: null,
                currentRecord: null,
              };
            }
          }
          return op;
        })
      );

      // Update the current result display
      if (record) {
        const resultText = `Found record: ${JSON.stringify(record, null, 2)}`;
        setResult(resultText);
        setError(null);
        setNotFoundMessage(null);
      } else {
        setResult(null);
        setError(null);
        setNotFoundMessage("🐾Iterator has reached the end");
      }
    } catch (err) {
      const dogError = getRandomDogError("ITERATE");
      setError(dogError);

      // Mark operation as ended due to error
      setOperations((prev) =>
        prev.map((op) => {
          if (op.id === operation.id) {
            return {
              ...op,
              ended: true,
              success: false,
              message: dogError,
            };
          }
          return op;
        })
      );
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
      const dogError = getRandomDogError("ITERATE");
      setError(dogError);
      addOperation("ITERATE", `${minKey}-${maxKey}`, false, dogError);
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

  const handleOperationClick = (operation) => {
    // Set the operation type and restore the input values
    setSelectedOperation(operation.type);
  setActiveOperationId(operation.id);

    // Clear any existing errors/validation
    setError(null);
    setValidationError(null);
    setNotFoundMessage(null);
    setResult(null);

    // Set the result based on what type of result this operation had
    if (operation.success && operation.resultData) {
      setResult(operation.resultData);
    } else if (operation.notFoundMessage) {
      setNotFoundMessage(operation.notFoundMessage);
    } else if (!operation.success && operation.message) {
      setError(operation.message);
    }

    // Restore input field values based on operation type
    if (operation.type === "GET" || operation.type === "DELETE") {
      setKey(operation.key);
    } else if (operation.type === "PUT") {
      setKey(operation.key);
      // Note: We don't have the original value stored, so we can't restore it
    } else if (operation.type === "PREFIX_SCAN") {
      setPrefix(operation.key);
      const count = Array.isArray(operation.keys) ? operation.keys.length : 0;
      setResult(`Prefix scan completed: ${count} records found`);
    } else if (operation.type === "PREFIX_ITERATE") {
      setPrefix(operation.key);
      // For iterators, we want to show the current state and provide next functionality
      if (operation.success && operation.resultData) {
        setResult(operation.resultData);
      } else if (operation.notFoundMessage) {
        setNotFoundMessage(operation.notFoundMessage);
      } else if (!operation.success && operation.message) {
        setError(operation.message);
      }
    } else if (
      operation.type === "RANGE_SCAN" ||
      operation.type === "RANGE_ITERATE"
    ) {
      const [min, max] = operation.key.split("-");
      setMinKey(min);
      setMaxKey(max);
    }
  };

  const getOperationIcon = () => {
    switch (selectedOperation) {
      case "GET":
        return <FaSearch />;
      case "PUT":
        return <FaRegSave />;
      case "DELETE":
        return <FaRegTrashAlt />;
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

  const getRandomDogError = (operation) => {
    const messages = dogErrorMessages[operation] || dogErrorMessages.GET;
    return messages[Math.floor(Math.random() * messages.length)];
  };

  const getRandomDogNotFound = (operation) => {
    const messages = dogNotFoundMessages[operation] || dogNotFoundMessages.GET;
    return messages[Math.floor(Math.random() * messages.length)];
  };

  const renderInputFieldsPUT = () => {
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

          {/* Tab Control */}
          <div className="mb-3 mt-0.5">
            <div className="flex border-b-2 border-sloth-brown pl-2 isolate">
              <button
                type="button"
                onClick={() => setValueTab("text")}
                className={`flex items-center justify-center gap-2 py-[0.45rem] px-4 font-bold text-sm transition-all duration-[230ms] !rounded-b-none ${
                  valueTab === "text"
                    ? "bg-sloth-yellow text-sloth-brown-dark border-2 border-sloth-brown border-b-sloth-yellow rounded-t-lg -mb-[2px] relative z-10"
                    : "border-2 border-b-0 border-sloth-brown/50 text-sloth-brown-dark bg-sloth-yellow-lite hover:bg-sloth-yellow-lite/50 active:bg-sloth-yellow-lite rounded-t-lg"
                }`}
                disabled={isLoading}
              >
                <FaFont className="text-sm" />
                Text
              </button>
              <button
                type="button"
                onClick={() => setValueTab("media")}
                className={`ml-1 flex items-center justify-center gap-2 py-[0.45rem] px-4 font-bold text-sm transition-all duration-[230ms] !rounded-b-none ${
                  valueTab === "media"
                    ? "bg-sloth-yellow text-sloth-brown-dark border-2 border-sloth-brown border-b-sloth-yellow rounded-t-lg -mb-[2px] relative z-10"
                    : "border-2 border-b-0 border-sloth-brown/50 text-sloth-brown-dark bg-sloth-yellow-lite hover:bg-sloth-yellow-lite/50 active:bg-sloth-yellow-lite rounded-t-lg"
                }`}
                disabled={isLoading}
              >
                <MdPermMedia className="text-sm" />
                Media
              </button>
            </div>
          </div>

          {/* Content based on active tab */}
          {valueTab === "text" ? (
            <textarea
              placeholder="Enter the value... woof!"
              value={value}
              onChange={(e) => {
                setValue(e.target.value);
                setUploadedFile(null);
              }}
              rows={4}
              className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200 resize-vertical"
              disabled={isLoading}
            />
          ) : (
            <div className="h-[10.5rem]">
              <FileUpload
                className="w-full h-full"
                file={uploadedFile}
                setFile={(file) => {
                  setUploadedFile(file);
                  setValue(undefined);
                }}
                handleClearExtra={() => {}}
              />
            </div>
          )}
        </div>
      </div>
    );
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
        return renderInputFieldsPUT();
      case "PREFIX_SCAN":
      case "PREFIX_ITERATE":
        return (
          <div className="grid grid-cols-1 gap-4">
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

            {selectedOperation === "PREFIX_SCAN" && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                    📄 Page Size
                  </label>
                  <StyledOperationSelect
                    value={pageSize}
                    onChange={(val) => setPageSize(Number(val))}
                    isDisabled={isLoading}
                    options={[
                      { value: 5, label: "5" },
                      { value: 10, label: "10" },
                      { value: 20, label: "20" },
                    ]}
                  />
                </div>
                <div>
                  <label className="block text-sm font-bold text-sloth-brown-dark mb-2">
                    🔢 Page Number
                  </label>
                  <input
                    type="text"
                    inputMode="numeric"
                    placeholder="1"
                    value={pageNumber}
                    onChange={(e) => {
                      const raw = e.target.value || "";
                      // Keep only digits
                      let v = raw.replace(/\D/g, "");
                      // Disallow leading zeros
                      v = v.replace(/^0+/, "");
                      // Allow empty while typing
                      setPageNumber(v);
                    }}
                    onBlur={() => {
                      const pn = Number(pageNumber);
                      if (!Number.isFinite(pn) || pn < 1) {
                        setPageNumber(1);
                      }
                    }}
                    className="w-full px-4 py-3 border-4 border-sloth-brown-dark rounded-lg text-sloth-brown-dark font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px] transition-all duration-200"
                    disabled={isLoading}
                  />
                </div>
              </div>
            )}
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
      <BgDecorations />

      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <DashboardSign dataLost={dataLost} />

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
                    setResult(null);
                    setError(null);
                    setNotFoundMessage(null);
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
            {(result || error || notFoundMessage) && !validationError && (
              <Result
                operation={selectedOperation}
                result={result}
                error={error}
                notFoundMessage={notFoundMessage}
                isSuccess={!error && !notFoundMessage && !!result}
                onIteratorNext={handlePrefixIteratorNext}
                operations={operations}
                onPrefixScanPageChange={handlePrefixScanPageChange}
                currentPrefix={prefix}
                activeOperationId={activeOperationId}
              />
            )}
          </div>

          {/* Side Panel */}
          <div className="space-y-6">
            {/* Stats Panel */}
            <Stats stats={stats} />

            {/* Recent Operations */}
            <RecentOperations
              operations={operations}
              onOperationClick={handleOperationClick}
              activeOperationId={activeOperationId}
            />
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

import React, { useState, useEffect } from "react";
import { useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { BgDecorations } from "@/components/home/BgDecorations";
import StyledOperationSelect from "@/components/select/StyledOperationSelect";
import { GetConfig, SaveConfig, CheckExistingData } from "@wails/main/App.js";
import {
  FaRegSave,
  FaCog,
  FaDatabase,
  FaMemory,
  FaExclamationTriangle,
  FaCheckCircle,
  FaDog,
  FaLock,
  FaShieldAlt,
} from "react-icons/fa";
import {
  MdStorage,
  MdCompress,
  MdSpeed,
  MdTune,
  MdLockOutline,
} from "react-icons/md";
import {
  BsGear,
  BsShieldCheck,
  BsExclamationTriangleFill,
} from "react-icons/bs";
import { Tooltip } from "react-tooltip";

const HyperRokica = "../../pics/rokica_hyper.png";
const SleepyCousin = "../../pics/rokica_sleepy.png";

// Dog configuration tips
const configTips = [
  "Woof! Remember - higher cache values mean happier <em>retriever</em> performance!",
  "Bark bark! LSM levels are like dog houses - more levels, more <em>paw-sible</em> storage!",
  "Good human! Compaction is like burying bones - <em>size-based</em> or <em>time-based</em>, both work!",
  "RUFF! Block sizes should be <em>just right</em> - not too big, not too small, like a perfect chew toy!",
  "Woof woof! WAL is like marking territory - it protects your data from getting <em>lost</em>!",
];

const lockedTips = [
  "Woof! Database is locked like a buried bone - can't dig it up without breaking it!",
  "Bark! Smart human locked the config to protect the precious data! No accidental <em>ruff</em> changes!",
  "Good dog knows: existing data means NO TOUCHING! Keep the database safe and sound!",
  "WOOF! Configuration locked tighter than a chew toy - and that's a GOOD thing!",
];

// Validation schema
const configSchema = yup.object().shape({
  lsm: yup.object().shape({
    max_levels: yup
      .number()
      .min(1, "Minimum 1 level required")
      .max(20, "Maximum 20 levels allowed")
      .required("Max levels is required"),
    max_tables_per_level: yup
      .number()
      .min(1, "Minimum 1 table per level")
      .max(100, "Maximum 100 tables per level")
      .required("Max tables per level is required"),
    max_memtables: yup
      .number()
      .min(1, "Minimum 1 memtable")
      .max(20, "Maximum 20 memtables")
      .required("Max memtables is required"),
    compaction_type: yup
      .string()
      .oneOf(["size", "time"], "Invalid compaction type")
      .required("Compaction type is required")
  }),
  cache: yup.object().shape({
    read_path_capacity: yup
      .number()
      .min(1, "Minimum capacity is 1")
      .required("Read path capacity is required"),
    block_capacity: yup
      .number()
      .min(1, "Minimum capacity is 1")
      .required("Block capacity is required"),
  }),
  wal: yup.object().shape({
    block_size: yup
      .number()
      .min(1024, "Minimum block size is 1024 bytes")
      .required("WAL block size is required"),
    log_size: yup
      .number()
      .min(1, "Minimum log size is 1")
      .required("Log size is required")
  }),
  sstable: yup.object().shape({
    compression_enabled: yup
      .boolean()
      .required("Compression setting is required"),
    block_size: yup
      .number()
      .min(1024, "Minimum block size is 1024 bytes")
      .required("SSTable block size is required"),
    use_separate_files: yup
      .boolean()
      .required("File separation setting is required"),
    sparse_step_index: yup
      .number()
      .min(1, "Minimum step index is 1")
      .required("Sparse step index is required"),
  }),
  memtable: yup.object().shape({
    capacity: yup
      .number()
      .min(1, "Minimum capacity is 1")
      .required("Memtable capacity is required"),
    memtable_type: yup
      .string()
      .oneOf(["btree", "skiplist", "hashmap"], "Invalid memtable type")
      .required("Memtable type is required"),
  }),
  bloom_filter: yup.object().shape({
    false_positive_rate: yup
      .number()
      .min(0.001, "Minimum false positive rate is 0.001")
      .max(0.5, "Maximum false positive rate is 0.5")
      .required("False positive rate is required"),
  }),
  block_manager: yup.object().shape({
    block_size: yup
      .number()
      .min(1024, "Minimum block size is 1024 bytes")
      .required("Block manager block size is required"),
    cache_size: yup
      .number()
      .min(1, "Minimum cache size is 1")
      .required("Cache size is required"),
  }),
  token_bucket: yup.object().shape({
    capacity: yup
      .number()
      .min(0, "Minimum capacity is 0")
      .max(1000, "Maximum capacity is 1000")
      .required("Token bucket capacity is required"),
    refill_interval: yup
      .number()
      .min(0, "Minimum interval is 0 seconds")
      .required("Refill interval is required"),
    refill_amount: yup
      .number()
      .min(0, "Minimum refill amount is 0")
      .required("Refill amount is required"),
  }),
  crc: yup.object().shape({
    size: yup
      .number()
      .min(1, "Minimum CRC size is 1")
      .required("CRC size is required"),
  }),
});

// Select options
const compactionTypeOptions = [
  { value: "size", label: "Size-based Compaction" },
  { value: "time", label: "Time-based Compaction" },
];

const memtableTypeOptions = [
  { value: "btree", label: "B-Tree" },
  { value: "skiplist", label: "Skip List" },
  { value: "hashmap", label: "HashMap" },
];

const booleanOptions = [
  { value: true, label: "Enabled" },
  { value: false, label: "Disabled" },
];

export const Config = () => {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [error, setError] = useState(null);
  const [isConfigLocked, setIsConfigLocked] = useState(false);
  const [lockReason, setLockReason] = useState("");
  const [dogTip, setDogTip] = useState("");
  const [isDogHovered, setIsDogHovered] = useState(false);

  const {
    register,
    handleSubmit,
    setValue,
    watch,
    formState: { errors, isValid },
  } = useForm({
    resolver: yupResolver(configSchema),
    mode: "onChange",
  });

  useEffect(() => {
    if (!isDogHovered) {
      setTimeout(
        () =>
          setDogTip(
            isConfigLocked
              ? "Woof! Configuration is safely locked!"
              : "Woof! Ready to configure!"
          ),
        160
      );
      return;
    }
    const tips = isConfigLocked ? lockedTips : configTips;
    setDogTip(tips[Math.floor(Math.random() * tips.length)]);
  }, [isDogHovered, isConfigLocked]);

  // Load configuration on mount
  useEffect(() => {
    const loadConfig = async () => {
      try {
        setLoading(true);

        // Check if there are existing database files
        const hasData = await CheckExistingData();

        if (hasData) {
          setIsConfigLocked(true);
          setLockReason(
            "Existing database files detected - configuration locked for data safety"
          );
        }

        const configData = await GetConfig();
        const parsedConfig = JSON.parse(configData);

        Object.keys(parsedConfig).forEach((section) => {
          Object.keys(parsedConfig[section]).forEach((key) => {
            setValue(`${section}.${key}`, parsedConfig[section][key]);
          });
        });
      } catch (err) {
        setError("Failed to load configuration: " + err.message);
      } finally {
        setLoading(false);
      }
    };

    loadConfig();
  }, [setValue]);

  const onSubmit = async (data) => {
    if (isConfigLocked) {
      setError("Configuration is locked due to existing database files!");
      return;
    }

    try {
      setSaving(true);
      setError(null);

      const currentConfigData = await GetConfig();
      const currentConfig = JSON.parse(currentConfigData);

      // Ensure backend-managed paths are preserved even though inputs are hidden
      const finalConfig = {
        ...data,
        lsm: {
          ...data.lsm,
          lsm_path: currentConfig?.lsm?.lsm_path || "lsm.db",
        },
      };

      const configJSON = JSON.stringify(finalConfig, null, 2);
      await SaveConfig(configJSON);

      setSaveSuccess(true);
      setTimeout(() => setSaveSuccess(false), 5000);
    } catch (err) {
      setError("Failed to save configuration: " + err.message);
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <div className="bg-sloth-yellow-lite/80 p-6 min-h-screen relative overflow-hidden select-none flex justify-center items-center">
        <BgDecorations />
        <div className="flex flex-col items-center gap-4">
          <div className="animate-spin rounded-full h-16 w-16 border-4 border-sloth-brown-dark border-t-transparent"></div>
          <div className="text-2xl font-bold text-sloth-brown-dark">
            Loading configuration...
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-sloth-yellow-lite/80 p-6 pt-[2.6rem] min-h-screen relative overflow-hidden select-none">
      <BgDecorations />

      {/* Lock Overlay - visual effect only */}
      {isConfigLocked && (
        <div className="fixed inset-0 bg-black/15 backdrop-blur-[1px] z-30 pointer-events-none">
          <div className="absolute inset-0 bg-gradient-to-br from-red-500/8 to-orange-500/8"></div>
        </div>
      )}

      <div className="max-w-5xl mx-auto space-y-8 relative z-40">
        {/* Header */}
        <div className="bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] relative overflow-hidden">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="relative">
                <FaCog className="text-3xl text-sloth-brown-dark" />
                {isConfigLocked && (
                  <FaLock className="absolute -top-1 -right-1 text-lg text-red-600 bg-white rounded-full p-1 border-2 border-red-600" />
                )}
              </div>
              <div>
                <h1 className="text-3xl font-bold text-sloth-brown-dark flex items-center gap-2">
                  Database Configuration
                  {isConfigLocked && (
                    <MdLockOutline className="text-2xl text-red-600" />
                  )}
                </h1>
                <p className="text-sloth-brown text-lg">
                  {isConfigLocked
                    ? "Configuration locked for data safety"
                    : "Fine-tune your HundDB settings"}
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2 text-sm">
              <div
                className={`w-3 h-3 rounded-full ${
                  isConfigLocked
                    ? "bg-red-500"
                    : isValid
                    ? "bg-green-500"
                    : "bg-red-500"
                }`}
              ></div>
              <span className="text-sloth-brown font-medium">
                {isConfigLocked ? "Locked" : isValid ? "Valid" : "Invalid"}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4 bg-sloth-tan bg-opacity-30 rounded-lg">
            <div className="text-center">
              <FaDatabase className="text-2xl text-sloth-brown-dark mx-auto mb-2" />
              <div className="font-semibold text-sloth-brown-dark">
                LSM Storage
              </div>
              <div className="text-sm text-sloth-brown">
                High-performance engine
              </div>
            </div>
            <div className="text-center">
              <MdSpeed className="text-2xl text-sloth-brown-dark mx-auto mb-2" />
              <div className="font-semibold text-sloth-brown-dark">
                Optimized Cache
              </div>
              <div className="text-sm text-sloth-brown">Memory-efficient</div>
            </div>
            <div className="text-center">
              <BsShieldCheck className="text-2xl text-sloth-brown-dark mx-auto mb-2" />
              <div className="font-semibold text-sloth-brown-dark">
                Data Integrity
              </div>
              <div className="text-sm text-sloth-brown">WAL protection</div>
            </div>
          </div>
        </div>

        {/* Lock Warning */}
        {isConfigLocked && (
          <div className="bg-red-50 border-4 border-red-600 rounded-xl p-6 mb-6 relative">
            <div className="absolute top-4 right-4">
              <FaShieldAlt className="text-3xl text-red-600" />
            </div>
            <div className="flex items-start gap-4">
              <BsExclamationTriangleFill className="text-3xl text-red-600 mt-1 flex-shrink-0" />
              <div className="space-y-3">
                <div>
                  <h3 className="text-2xl font-bold text-red-800 mb-2">
                    CONFIGURATION LOCKED
                  </h3>
                  <p className="text-red-700 font-semibold text-lg">
                    {lockReason}
                  </p>
                </div>
                <div className="bg-red-100 border-2 border-red-300 rounded-lg p-4">
                  <p className="text-red-800 font-medium mb-2">
                    <strong>REASON FOR LOCKING:</strong>
                  </p>
                  <ul className="text-red-700 space-y-1 text-sm">
                    <li>• Existing database files have been detected</li>
                    <li>• Changing configuration could damage existing data</li>
                    <li>• Lock protects the integrity of your data</li>
                  </ul>
                </div>
                <div className="bg-yellow-100 border-2 border-yellow-400 rounded-lg p-4">
                  <p className="text-yellow-800 font-medium mb-2">
                    <strong>TO ENABLE CHANGES:</strong>
                  </p>
                  <ul className="text-yellow-700 space-y-1 text-sm">
                    <li>• Make a backup of existing data</li>
                    <li>• Delete or move existing database files</li>
                    <li>• Or create a new database with a new configuration</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Alerts */}
        {error && (
          <div className="bg-red-100 border-4 border-red-600 rounded-lg p-4 mb-6">
            <div className="flex items-center gap-3">
              <FaExclamationTriangle className="text-2xl text-red-700" />
              <p className="text-red-700 font-semibold">{error}</p>
            </div>
          </div>
        )}

        {saveSuccess && (
          <div className="bg-green-100 border-4 border-green-600 rounded-lg p-4 mb-6">
            <div className="flex items-center gap-3">
              <FaCheckCircle className="text-2xl text-green-700" />
              <p className="text-green-700 font-semibold">
                Configuration saved successfully!
              </p>
            </div>
          </div>
        )}

        {/* Configuration Sections */}
        <div className={`space-y-6 ${isConfigLocked ? "opacity-60" : ""}`}>
          {/* LSM Configuration */}
          <ConfigSection
            title="LSM Tree Configuration"
            icon={<FaDatabase />}
            description="Log-Structured Merge Tree settings"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigInput
                label="Max Levels"
                name="lsm.max_levels"
                type="number"
                register={register}
                error={errors.lsm?.max_levels}
                description="Maximum LSM tree levels (1-20)"
                min={1}
                max={20}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Max Tables per Level"
                name="lsm.max_tables_per_level"
                type="number"
                register={register}
                error={errors.lsm?.max_tables_per_level}
                description="Maximum SSTables per level (1-100)"
                min={1}
                max={100}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Max Memtables"
                name="lsm.max_memtables"
                type="number"
                register={register}
                error={errors.lsm?.max_memtables}
                description="Maximum memtables (1-20)"
                min={1}
                max={20}
                disabled={isConfigLocked}
              />
              <ConfigSelect
                label="Compaction Type"
                name="lsm.compaction_type"
                options={compactionTypeOptions}
                setValue={setValue}
                watch={watch}
                error={errors.lsm?.compaction_type}
                disabled={isConfigLocked}
              />
              {/** LSM path is managed by the app; hidden from user but preserved on save */}
            </div>
          </ConfigSection>

          {/* Cache Configuration */}
          <ConfigSection
            title="Cache Configuration"
            icon={<FaMemory />}
            description="Memory cache settings"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigInput
                label="Read Path Capacity"
                name="cache.read_path_capacity"
                type="number"
                register={register}
                error={errors.cache?.read_path_capacity}
                description="Cache capacity for read operations"
                min={1}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Block Capacity"
                name="cache.block_capacity"
                type="number"
                register={register}
                error={errors.cache?.block_capacity}
                description="Cache capacity for blocks"
                min={1}
                disabled={isConfigLocked}
              />
            </div>
          </ConfigSection>

          {/* WAL Configuration */}
          <ConfigSection
            title="Write-Ahead Log (WAL)"
            icon={<MdStorage />}
            description="Write-ahead logging configuration"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigInput
                label="Block Size (bytes)"
                name="wal.block_size"
                type="number"
                register={register}
                error={errors.wal?.block_size}
                description="WAL block size (min: 1024)"
                min={1024}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Log Size"
                name="wal.log_size"
                type="number"
                register={register}
                error={errors.wal?.log_size}
                description="Maximum WAL log size"
                min={1}
                disabled={isConfigLocked}
              />
              {/** WAL path is managed by the app; hidden from user but preserved on save */}
            </div>
          </ConfigSection>

          {/* SSTable Configuration */}
          <ConfigSection
            title="SSTable Configuration"
            icon={<MdCompress />}
            description="Sorted String Table settings"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigSelect
                label="Compression Enabled"
                name="sstable.compression_enabled"
                options={booleanOptions}
                setValue={setValue}
                watch={watch}
                error={errors.sstable?.compression_enabled}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Block Size (bytes)"
                name="sstable.block_size"
                type="number"
                register={register}
                error={errors.sstable?.block_size}
                description="SSTable block size (min: 1024)"
                min={1024}
                disabled={isConfigLocked}
              />
              <ConfigSelect
                label="Use Separate Files"
                name="sstable.use_separate_files"
                options={booleanOptions}
                setValue={setValue}
                watch={watch}
                error={errors.sstable?.use_separate_files}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Sparse Step Index"
                name="sstable.sparse_step_index"
                type="number"
                register={register}
                error={errors.sstable?.sparse_step_index}
                description="Sparse index step size"
                min={1}
                disabled={isConfigLocked}
              />
            </div>
          </ConfigSection>

          {/* Memtable Configuration */}
          <ConfigSection
            title="Memtable Configuration"
            icon={<FaMemory />}
            description="In-memory table settings"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigInput
                label="Capacity"
                name="memtable.capacity"
                type="number"
                register={register}
                error={errors.memtable?.capacity}
                description="Memtable capacity"
                min={1}
                disabled={isConfigLocked}
              />
              <ConfigSelect
                label="Memtable Type"
                name="memtable.memtable_type"
                options={memtableTypeOptions}
                setValue={setValue}
                watch={watch}
                error={errors.memtable?.memtable_type}
                disabled={isConfigLocked}
              />
            </div>
          </ConfigSection>

          {/* Bloom Filter Configuration */}
          <ConfigSection
            title="Bloom Filter Configuration"
            icon={<BsGear />}
            description="Bloom filter settings"
            isLocked={isConfigLocked}
          >
            <ConfigInput
              label="False Positive Rate"
              name="bloom_filter.false_positive_rate"
              type="number"
              step="0.001"
              register={register}
              error={errors.bloom_filter?.false_positive_rate}
              description="False positive rate (0.001-0.5)"
              min={0.001}
              max={0.5}
              disabled={isConfigLocked}
            />
          </ConfigSection>

          {/* Block Manager Configuration */}
          <ConfigSection
            title="Block Manager Configuration"
            icon={<MdStorage />}
            description="Block storage management"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <ConfigInput
                label="Block Size (bytes)"
                name="block_manager.block_size"
                type="number"
                register={register}
                error={errors.block_manager?.block_size}
                description="Block size (min: 1024)"
                min={1024}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Cache Size"
                name="block_manager.cache_size"
                type="number"
                register={register}
                error={errors.block_manager?.cache_size}
                description="Block cache size"
                min={1}
                disabled={isConfigLocked}
              />
            </div>
          </ConfigSection>

          {/* Token Bucket Configuration */}
          <ConfigSection
            title="Token Bucket Configuration"
            icon={<MdTune />}
            description="Rate limiting settings"
            isLocked={isConfigLocked}
          >
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <ConfigInput
                label="Capacity"
                name="token_bucket.capacity"
                type="number"
                register={register}
                error={errors.token_bucket?.capacity}
                description="Max token capacity (1-1000)"
                min={1}
                max={1000}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Refill Interval (seconds)"
                name="token_bucket.refill_interval"
                type="number"
                register={register}
                error={errors.token_bucket?.refill_interval}
                description="Token refill interval"
                min={1}
                disabled={isConfigLocked}
              />
              <ConfigInput
                label="Refill Amount"
                name="token_bucket.refill_amount"
                type="number"
                register={register}
                error={errors.token_bucket?.refill_amount}
                description="Tokens added per interval"
                min={1}
                disabled={isConfigLocked}
              />
            </div>
          </ConfigSection>
        </div>

        {/* Save Button */}
        <div className="flex justify-center pt-8">
          <button
            onClick={handleSubmit(onSubmit)}
            disabled={saving || !isValid || isConfigLocked}
            className={`flex items-center gap-3 px-8 py-4 font-bold text-lg rounded-lg border-4 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] transition-all duration-200 ${
              isConfigLocked
                ? "bg-gray-400 text-gray-600 border-gray-600 cursor-not-allowed opacity-60"
                : "bg-sloth-brown text-sloth-yellow border-sloth-brown-dark hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] active:shadow-none active:translate-x-[4px] active:translate-y-[4px] disabled:opacity-50 disabled:cursor-not-allowed"
            }`}
          >
            {isConfigLocked ? (
              <>
                <FaLock />
                Configuration Locked
              </>
            ) : saving ? (
              <>
                <div className="animate-spin rounded-full h-5 w-5 border-2 border-sloth-yellow border-t-transparent"></div>
                Saving Configuration...
              </>
            ) : (
              <>
                <FaRegSave />
                Save Configuration
              </>
            )}
          </button>
        </div>

        {/* Dog Tips */}
        <div
          className={`bg-gradient-to-r from-sloth-yellow to-sloth-yellow-lite border-4 border-dashed rounded-xl p-6 max-w-full mx-auto md:max-w-[90%] ${
            isConfigLocked
              ? "border-red-400 from-red-100 to-red-50"
              : "border-sloth-brown"
          }`}
        >
          <div className="flex items-start gap-3">
            <div className="relative">
              <FaDog className="text-2xl text-sloth-brown mt-1 flex-shrink-0" />
              {isConfigLocked && (
                <FaLock className="absolute -top-1 -right-1 text-sm text-red-600 bg-white rounded-full p-0.5 border border-red-600" />
              )}
            </div>
            <div>
              <h4
                className={`text-lg font-bold mb-2 ${
                  isConfigLocked ? "text-red-700" : "text-sloth-brown-dark"
                }`}
              >
                {isConfigLocked
                  ? "Database Protection Mode"
                  : "Pro Configuration Tips"}
              </h4>
              <p
                className={`leading-relaxed mr-3 ${
                  isConfigLocked ? "text-red-600" : "text-sloth-brown"
                }`}
              >
                {isConfigLocked ? (
                  <strong>
                    Woof! Smart human - the config is locked to protect your
                    precious data bones! No accidental changes means no broken
                    databases. Good dog training!
                  </strong>
                ) : (
                  <>
                    <strong>Woof! From the pack:</strong> LSM levels are like dog
                    houses - more levels mean more storage but slower reads.
                    Cache settings are like treat storage - bigger cache means
                    faster <em>retrieval</em>!
                  </>
                )}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Peeking Dog */}
      <img
        src={isConfigLocked ? SleepyCousin : HyperRokica}
        alt="Rodney"
        className={`config-dog hidden sm:block absolute -bottom-2 -right-2 w-auto h-[7.5rem] hover:h-[9.25rem] -rotate-[8deg] hover:-rotate-3 object-contain transform translate-x-1/4 translate-y-1/4 opacity-95 transition-all duration-[465ms] hover:translate-x-0 hover:translate-y-0 hover:scale-110 hover:opacity-100 cursor-pointer ${
          isConfigLocked ? "grayscale" : ""
        }`}
        onMouseEnter={() => setIsDogHovered(true)}
        onMouseLeave={() => setIsDogHovered(false)}
      />

      <Tooltip
        anchorSelect=".config-dog"
        place="left-start"
        delayShow={350}
        offset={12}
        opacity={1}
        className={`!p-4 !rounded-xl !z-[9999] !max-w-sm border-2 shadow-[4px_4px_0px_0px_#6b5e4a] ${
          isConfigLocked
            ? "!bg-red-50 border-red-500"
            : "!bg-white border-sloth-brown"
        }`}
        border={isConfigLocked ? "3px solid #ef4444" : "3px solid #4b4436"}
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
            <p
              className={`font-semibold mb-2 text-lg ${
                isConfigLocked ? "text-red-700" : "text-sloth-brown"
              }`}
            >
              {isConfigLocked ? "Rodney says protectively:" : "Rodney says:"}
            </p>
            <p
              className={`italic ${
                isConfigLocked ? "text-red-600" : "text-gray-600"
              }`}
              dangerouslySetInnerHTML={{ __html: dogTip }}
            />
          </div>
        </div>
      </Tooltip>
    </div>
  );
};

// Config Section Component
const ConfigSection = ({ title, icon, description, children, isLocked }) => (
  <div
    className={`bg-sloth-yellow rounded-xl p-6 border-4 border-sloth-brown shadow-[6px_6px_0px_0px_rgba(107,94,74,1)] relative ${
      isLocked ? "opacity-75" : ""
    }`}
  >
    {isLocked && (
      <div className="absolute top-4 right-4 z-10">
        <FaLock className="text-xl text-red-600 bg-white rounded-full p-1 border-2 border-red-600" />
      </div>
    )}
    <div className="flex items-center gap-3 mb-4">
      <div
        className={`text-2xl ${
          isLocked ? "text-gray-500" : "text-sloth-brown-dark"
        }`}
      >
        {icon}
      </div>
      <div>
        <h2
          className={`text-2xl font-bold ${
            isLocked ? "text-gray-600" : "text-sloth-brown-dark"
          }`}
        >
          {title}
        </h2>
        <p className={isLocked ? "text-gray-500" : "text-sloth-brown"}>
          {description}
        </p>
      </div>
    </div>
    {children}
  </div>
);

// Config Input Component
const ConfigInput = ({
  label,
  name,
  type = "text",
  register,
  error,
  description,
  className = "",
  disabled = false,
  ...props
}) => (
  <div className={className}>
    <label
      className={`block text-sm font-bold mb-2 ${
        disabled ? "text-gray-500" : "text-sloth-brown-dark"
      }`}
    >
      {label}
    </label>
    <input
      {...register(name)}
      type={type}
      disabled={disabled}
      className={`w-full px-4 py-3 border-4 rounded-lg font-medium shadow-[3px_3px_0px_0px_rgba(139,119,95,1)] transition-all duration-200 ${
        disabled
          ? "border-gray-400 bg-gray-100 text-gray-500 cursor-not-allowed"
          : error
          ? "border-red-500 text-sloth-brown-dark placeholder-sloth-brown focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px]"
          : "border-sloth-brown-dark text-sloth-brown-dark placeholder-sloth-brown focus:shadow-[1px_1px_0px_0px_rgba(139,119,95,1)] focus:outline-none focus:translate-x-[1px] focus:translate-y-[1px]"
      }`}
      {...props}
    />
    {description && (
      <p
        className={`text-xs mt-1 ${
          disabled ? "text-gray-400" : "text-sloth-brown"
        }`}
      >
        {description}
      </p>
    )}
    {error && (
      <p className="text-red-500 text-sm mt-1 flex items-center gap-1">
        <FaExclamationTriangle className="text-xs" />
        {error.message}
      </p>
    )}
  </div>
);

// Config Select Component
const ConfigSelect = ({
  label,
  name,
  options,
  setValue,
  watch,
  error,
  description,
  disabled = false,
}) => {
  const currentValue = watch(name);

  const handleChange = (selectedValue) => {
    if (!disabled) {
      setValue(name, selectedValue, { shouldValidate: true });
    }
  };

  const selectedOption = options.find(
    (option) => option.value === currentValue
  );

  return (
    <div>
      <label
        className={`block text-sm font-bold mb-2 ${
          disabled ? "text-gray-500" : "text-sloth-brown-dark"
        }`}
      >
        {label}
      </label>
      <div
        className={`${
          error ? "border-4 border-red-500 rounded-lg" : ""
        } z-[100]`}
      >
        <StyledOperationSelect
          value={selectedOption?.value}
          onChange={handleChange}
          options={options}
          isSearchable={false}
          isDisabled={disabled}
        />
      </div>
      {description && (
        <p
          className={`text-xs mt-1 ${
            disabled ? "text-gray-400" : "text-sloth-brown"
          }`}
        >
          {description}
        </p>
      )}
      {error && (
        <p className="text-red-500 text-sm mt-1 flex items-center gap-1">
          <FaExclamationTriangle className="text-xs" />
          {error.message}
        </p>
      )}
    </div>
  );
};

export default Config;

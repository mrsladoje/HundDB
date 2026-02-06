# 🐕 HundDB

> **Sit... fetch... query!**  
> **Gooood database :)**

[![Built with](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![Wails](https://img.shields.io/badge/Desktop-Wails-DF4E3B)](https://wails.io)
[![React](https://img.shields.io/badge/Frontend-React-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🌟 What Is HundDB?

HundDB is a production-grade LSM-tree based key-value storage engine built from scratch in pure Go. It features a complete segmented WAL with low watermark management, multiple memtable implementations (B-Tree, Skip-List, HashMap), and sophisticated SSTables with five components: compressed data storage (via global key dictionary), regular index, sparse summary index, Bloom filters, and Merkle trees for integrity validation.

The read path uses multi-layered optimization: memtables (newest to oldest) → LRU cache → SSTables with Bloom filter prefiltering, boundary validation, and binary search on sparse then regular indexes. For prefix iteration and range scans, prefix-enhanced Bloom filters (inserting all prefixes up to 10 characters) enable skipping entire SSTables, dramatically reducing disk I/O.

The write path ensures durability through a segmented WAL with CRC-validated record fragmentation, then updates the active memtable. A concurrent flush pool (worker goroutines) persists full memtables as SSTables. The system supports both Size-Tiered Compaction (grouping similar-sized tables) and Leveled Compaction (sorted runs with overlap-based merging), both user-configurable.

The engine is wrapped in a cross-platform desktop app using **Wails** (Go-React integration) with a **neobrutalist design** built in React + Vite + Tailwind CSS. Because databases deserve more than terminal interfaces :)


## 🎥 See It In Action (YT▶️ link)

[![HundDB Demo](https://img.youtube.com/vi/gkG7mmT554E/maxresdefault.jpg)](https://www.youtube.com/watch?v=gkG7mmT554E)

*Click on the image above to watch the full demo!*

## 💪 What Makes HundDB Special

### Memory Management Mastery 🤹

We treat memory like it's precious—because it is. Every component is designed to:

- **Stream large datasets** rather than loading them entirely into memory
- **Use block-level caching** at multiple layers (LRU cache between memtables/SSTables, separate block cache in Block Manager)
- **Implement LRU eviction** strategically to minimize memory footprint
- **Avoid unnecessary allocations** in hot paths through careful buffer management

The compaction process exemplifies this: instead of loading entire SSTables into memory, we use iterators that stream data block-by-block, maintain only record hashes (32 bytes each) for Merkle tree construction, and write output incrementally. This allows us to compact multi-gigabyte SSTables with minimal RAM usage.

### Disk I/O Minimization: Five Layers of Defense ⚔️

Our read path has **five layers of defense** against unnecessary disk access:

1. **Memtable Check** (in-memory, instant)
2. **LRU Cache** (frequently accessed records stay hot)
3. **Bloom Filter** (probabilistic existence test with prefix support)
4. **Boundary Validation** (quick min/max key rejection)
5. **Block Manager Cache** (LRU cache for disk blocks)

Only after all these layers do we perform actual disk reads—and when we do, binary search on sparse indexes minimizes the number of blocks touched.

### Data Integrity Paranoia 💡

Every. Single. Byte. on disk gets a **CRC checksum**. We validate:

- WAL records on every read 
- SSTable blocks on every access (per-block CRCs)
- Full SSTable integrity via **Merkle trees** (user-triggered validation can detect exactly which records are corrupted)

> #### If cosmic radiation flips a bit, we'll know about it. 🤣🤣🤣🤣🤣🤣

### Advanced Query Optimization 🔋

For complex operations (prefix scans, range scans, iterators), we implement **lower bound search** instead of naive linear scanning. Combined with our prefix-enhanced Bloom filters, this means:

- **PREFIX_SCAN("user", page, size)** → Bloom filter checks prefixes "u", "us", "use", "user" (up to 10 chars), skips non-matching SSTables entirely, then uses lower bound search to jump directly to the first matching key
- **RANGE_SCAN("aaa", "zzz", page, size)** → No Bloom filter (ranges are hard to represent), but lower bound search jumps to range start, then sequential iteration with tombstone handling
- **PREFIX_ITERATE / RANGE_ITERATE** → Stateful iterators that maintain position across calls using the same lower bound search + sequential scan strategy

### Configuration-Driven Design ⚙️

Nearly every aspect of the engine is configurable through an external JSON file:

- **LSM Structure**: Number of levels, tables per level, compaction strategy (size-tiered vs. leveled), memtable count
- **Memtable Type**: Choose between B-Tree (balanced), Skip-List (probabilistic), or HashMap (O(1) but unordered)
- **SSTable Format**: Enable/disable compression, separate vs. single-file storage, sparse index density
- **Block Manager**: Block size (4KB/8KB/16KB), cache size
- **WAL**: Segment size, fragmentation behavior
- **Performance**: Cache sizes, Bloom filter false positive rates, token bucket rate limiting

This flexibility allows tuning the engine for different workloads—optimize for write throughput, read latency, space efficiency, or any combination.

## ⚡ Key Features

- **🗄️ Complete LSM-Tree Implementation** - Full read/write path with WAL, memtables, SSTables, and compaction
- **🎯 Multiple Memtable Types** - Choose between B-Tree, HashMap, or Skip-List (all implemented from scratch)
- **📊 Probabilistic Structures** - HyperLogLog, Count-Min-Sketch, SimHash, and Bloom Filters
- **🔍 Advanced Query Operations** - Prefix scan/iterate, range scan/iterate with lower bound search optimization
- **🔒 Data Integrity** - CRC checksums on ALL disk data, plus Merkle trees for validation
- **⚙️ Highly Configurable** - Support for both Size-Tiered and Leveled compaction strategies, configurable everything
- **💾 Memory Efficient** - Sophisticated caching, streaming, and iterators to never overload RAM
- **🚀 Optimized Reads** - Multi-layer SSTable search with Bloom filters (including prefix Bloom), binary searches on sparse and regular indexes, and boundary checks
- **🎨 Beautiful Desktop UI** - Cross-platform app with React + Tailwind neobrutalist design
- **🐾 Dog-Themed** - Because every serious database needs a good mascot

## 🏗️ Architecture Deep Dive

### Write Path: From Request to Disk 💾

```
User Request → Token Bucket → WAL → Memtable → SSTable Flush → Compaction
```

1. **Token Bucket Rate Limiting** - Prevents system overload (configurable capacity and refill rate)
2. **Write-Ahead Log (WAL)** - Segmented log ensuring durability:
   - Records written with **per-block CRC validation**
   - Supports **fragmentation** for records larger than block size
   - **Low watermark** tracking per memtable enables safe log truncation after flush
   - Graceful shutdown vs. crash recovery (metadata file tracks clean exit)
3. **Memtable** - In-memory structure (user's choice of B-Tree, HashMap, or Skip-List)
4. **Concurrent Flush Pool** - When all memtables are full, a worker pool flushes them in parallel while the system continues accepting writes to a fresh memtable
5. **SSTable Creation** - Flushed memtables become immutable SSTables on disk
6. **Compaction** - Background process maintains read performance:
   - **Size-Tiered**: Groups SSTables of similar size, merges when count exceeds threshold, cascades upward through levels
   - **Leveled**: Merges overlapping key ranges between levels, maintains sorted runs

### Read Path: The Art of Finding Data Fast 🔍

```
User Request → Memtable → Cache → SSTable (Bloom → Boundaries → Indexes → Data)
```

Our read path is a masterclass in disk I/O minimization:

1. **Check Memtables** - Newest to oldest, instant lookup
2. **Check LRU Cache** - Frequently accessed records stay in memory
3. **SSTable Search** - This is where the magic happens:
   - **Bloom Filter Check** - Probabilistic test (includes prefix checks for scan operations)
   - **Boundary Check** - Validate key is within [min, max] range
   - **Binary Search on Sparse Index** - Jump to approximate location (every Nth entry)
   - **Binary Search on Regular Index** - Pinpoint exact record location
   - **Block Manager Fetch** - Retrieve from disk (with block-level LRU cache)

For complex operations (**PREFIX_SCAN**, **RANGE_SCAN**, iterators), we use **lower bound search** to find the first key ≥ target, then iterate sequentially. Tombstone tracking across levels ensures deleted records don't appear in results.

### Write-Ahead Log (WAL): Durability Through Segmentation 🪄

The WAL is our guarantee against data loss:

- **Segmented Architecture**: Log split into fixed-size segments (user-configurable), each containing a maximum number of blocks
- **Record Fragmentation**: Records larger than a block are split into fragments (FIRST, MIDDLE, LAST, FULL markers)
- **CRC Validation**: Every block has a CRC header, verified on read to detect corruption
- **Low Watermark**: Per-memtable tracking of highest persisted log index enables safe deletion of old segments after flush
- **Recovery Process**: On startup, WAL replays records into memtables to restore unflushed data

The WAL's block-based design means we never load entire log segments into memory—we read and deserialize block-by-block.

### SSTable Structure: Five-Component Design 🤩

Each SSTable comprises:

```
SSTable
├── Data Component        - Actual key-value records (optionally compressed)
├── Index                - Points to exact record locations (binary searchable)
├── Summary (Sparse)     - Every Nth index entry (configurable density)
├── Bloom Filter         - Probabilistic existence check + prefix support
└── Merkle Tree          - Integrity validation (MD5 hashes of all records)
```

**Compression**: When enabled, keys are replaced with numeric IDs from a global dictionary shared across all SSTables, dramatically reducing storage for repetitive key patterns.

**File Modes**: User chooses between single-file (all components in one file with offset tracking) or separate-files (one file per component with size prefixes).

### Compaction: Two Strategies, One Goal 👓

Compaction prevents read amplification by merging SSTables:

**Size-Tiered Compaction:**
- Groups SSTables of similar size on each level
- When a level exceeds capacity (e.g., 4 tables), merges oldest group
- Merged result moves to next level (or stays if last level)
- Good for write-heavy workloads

**Leveled Compaction:**
- Maintains sorted runs across levels with minimal overlap
- When a level exceeds capacity, picks oldest table from level L
- Finds overlapping tables in level L+1 based on key ranges
- Merges all overlapping tables and places result in L+1
- Better read performance, more write amplification

Both strategies use **streaming merge-sort**: iterators over source SSTables, merge with tombstone resolution, write output incrementally block-by-block. Memory usage stays constant regardless of SSTable sizes.

### The Block Manager: Disk I/O Guardian 💂

All disk access goes through the Block Manager:

- **Fixed Block Sizes** - 4KB, 8KB, or 16KB (user-configurable)
- **CRC Validation** - Every block has a 4-byte CRC header, validated on read
- **Block-Level LRU Cache** - Frequently accessed blocks stay in memory
- **Streaming Support** - Read/write methods handle multi-block operations transparently
- **Concurrent Access** - Per-file RWMutex allows parallel reads, serialized writes

The Block Manager's abstraction means higher-level components (WAL, SSTable) never worry about block boundaries, CRCs, or caching—they just read/write arbitrary byte ranges.

## 🚀 Getting Started

### Prerequisites

- **Go 1.21+** - For the storage engine
- **Node.js 18+** - For the frontend build process
- **Wails CLI** - Cross-platform desktop framework

### Installation

Getting HundDB running is remarkably simple—we've invested significant engineering effort into making the build process painless. Here's the complete setup:

```bash
# Clone the repository
git clone https://github.com/mrsladoje/HundDB.git
cd HundDB

# Install Wails globally (if not already installed)
go install github.com/wailsapp/wails/v2/cmd/wails@latest

# Build the application (handles Go compilation, npm install, and bundling)
wails build

# For development mode with hot reload
wails dev
```

The `wails build` command orchestrates the entire build pipeline: Go compilation, npm dependency installation, frontend bundling, and cross-platform executable generation. It's almost suspiciously simple—but that simplicity comes from weeks of build system refinement.

**Fun fact:** Building a database from scratch is hard. Making it *look* easy to build is even harder. You're welcome. 🐕

## Configuration ⚙️

>Choose your spice! Read-heavy or write heavy?
>Our config can handle both! (or a s**t config that's neither in case you need exactly that.. 🤣🤣🤣🤣)

All engine parameters are configurable via `utils/config/app.json`. The system loads this configuration on startup, with automatic fallback to sensible defaults for any missing values:

### Configuration Options Explained 🙃

**LSM Tree Structure:**
- `max_levels`: Depth of LSM tree (typically 5-7)
- `max_tables_per_level`: Compaction trigger threshold
- `max_memtables`: Number of in-memory tables (higher = more write buffering)
- `compaction_type`: "size" (size-tiered) or "level" (leveled)

**Memtable:**
- `capacity`: Number of records per memtable
- `memtable_type`: "btree" (balanced, ordered), "skiplist" (probabilistic, ordered), "hashmap" (fast, unordered)

**SSTable:**
- `compression_enabled`: Use global key dictionary compression
- `use_separate_files`: true = one file per component, false = single file
- `sparse_step_index`: Density of sparse index (lower = more index entries = faster lookup, more memory)

**Block Manager:**
- `block_size`: 4096 (4KB), 8192 (8KB), or 16384 (16KB)
- `cache_size`: Number of blocks to keep in LRU cache

**Performance Tuning:**
- Increase `max_memtables` for write-heavy workloads
- Decrease `sparse_step_index` for read-heavy workloads (more memory usage)
- Increase `read_path_capacity` for working sets that fit in cache
- Use `compaction_type: "level"` for predictable read latency

The configuration system validates all parameters on load and provides clear error messages for invalid values. Change the config, restart HundDB, and your new settings take effect immediately.

## 📁 Project Structure

```
HundDB
├── lsm/                      # LSM-Tree Core Engine
│   ├── block_manager/        # Block-level disk abstraction with LRU cache
│   ├── cache/                # Read path cache layer
│   ├── lru_cache/            # Generic LRU implementation
│   ├── memtable/             # Three implementations (btree/hashmap/skiplist)
│   ├── sstable/              # Five-component SSTable with compaction
|   |   ├── bloom_filter/     # Bloom Filter for negative checks
|   |   └── merkle_tree/      # Merkle Tree for data integrity
│   └── wal/                  # Segmented write-ahead log
├── probabilistic/            # Independent probabilistic data structures
│   ├── count_min_sketch/
│   ├── hyperloglog/
│   ├── independent_bloom_filter/
│   └── sim_hash/
├── model/                    # Data models and records
├── token_bucket/             # Rate limiting
├── utils/                    # Configuration, CRC, compression, etc.
└── frontend/                 # React + Vite + Tailwind UI
```

## 🔮 Roadmap

HundDB is production-ready but still evolving! Planned features:

- [ ] **Worker Pool for inclming requests** - Concurrent request handling for improved throughput
- [ ] **Snapshot Isolation** - MVCC-based transaction support
- [ ] **Replication** - Multi-node deployment with leader election
- [ ] **Query Planner** - Cost-based optimization for complex queries
- [ ] **Metrics Dashboard** - Real-time performance monitoring and profiling

Check our [Issues](https://github.com/mrsladoje/HundDB/issues) tab to contribute or suggest features!

## 👥 Dreamteam

- **[mrsladoje](https://github.com/mrsladoje)** 
- **[Vukotije](https://github.com/Vukotije)** 
- **[0vertake](https://github.com/0vertake)** 
- **[nikolastevanovicc](https://github.com/nikolastevanovicc)** 
## 🐾 The Motto That Started It All

*"Dachshund Data: Short on time but long on information? Hund-reds of Tables - fetching data faster than you can say 'Hundewurst, bitte!'"*

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Prof. Milos Simic** and **TA Tamara Rankovic** for the incredibly detailed specification that pushed us to implement production-grade features and actually providing us with an interesting college project with so much learning potential
- The Go community for excellent documentation and libraries

---

<div align="center">

**[⭐ Star this repo](https://github.com/mrsladoje/HundDB) if you love dogs and databases!**

[🐕 Report Bug](https://github.com/mrsladoje/HundDB/issues) | [💡 Request Feature](https://github.com/mrsladoje/HundDB/issues) | [📖 Documentation](https://github.com/mrsladoje/HundDB/wiki)

*Built from scratch, optimized to perfection, themed with puppers* 🐾

</div>

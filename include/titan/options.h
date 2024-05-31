#pragma once

#include <map>
#include <unordered_map>

#include "rocksdb/advanced_cache.h"
#include "rocksdb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanDBOptions : public DBOptions {
  // The directory to store data specific to TitanDB alongside with
  // the base DB.
  //
  // Default: {dbname}/titandb
  std::string dirname;

  // Disable background GC
  //
  // Default: false
  bool disable_background_gc{false};

  // Max background GC thread
  //
  // Default: 1
  int32_t max_background_gc{1};

  // How often to schedule delete obsolete blob files periods.
  // If set zero, obsolete blob files won't be deleted.
  //
  // Default: 10
  uint32_t purge_obsolete_files_period_sec{10};  // 10s

  // If non-zero, dump titan internal stats to info log every
  // titan_stats_dump_period_sec.
  //
  // Default: 600 (10 min)
  uint32_t titan_stats_dump_period_sec{600};

  TitanDBOptions() = default;
  explicit TitanDBOptions(const DBOptions& options) : DBOptions(options) {}

  void Dump(Logger* logger) const;

  TitanDBOptions& operator=(const DBOptions& options) {
    *static_cast<DBOptions*>(this) = options;
    return *this;
  }
};

enum class TitanBlobRunMode {
  kNormal = 0,    // Titan process read/write as normal
  kReadOnly = 1,  // Titan stop writing value into blob log during flush
                  // and compaction. Existing values in blob log is still
                  // readable and garbage collected.
  kFallback = 2,  // On flush and compaction, Titan will convert blob
                  // index into real value, by reading from blob log,
                  // and store the value in SST file.
};

struct TitanOptionsHelper {
  static std::map<TitanBlobRunMode, std::string> blob_run_mode_to_string;
  static std::unordered_map<std::string, TitanBlobRunMode>
      blob_run_mode_string_map;
};

static auto& blob_run_mode_to_string =
    TitanOptionsHelper::blob_run_mode_to_string;
static auto& blob_run_mode_string_map =
    TitanOptionsHelper::blob_run_mode_string_map;

struct ImmutableTitanCFOptions;
struct MutableTitanCFOptions;

struct TitanCFOptions : public ColumnFamilyOptions {
  // The smallest value to store in blob files. Value smaller than
  // this threshold will be inlined in base DB.
  //
  // Default: 4096
  uint64_t min_blob_size{4096};

  // The compression algorithm used to compress data in blob files.
  //
  // Default: kNoCompression
  CompressionType blob_file_compression{kNoCompression};

  // The compression options. The `blob_file_compression.enabled` option is
  // ignored, we only use `blob_file_compression` above to determine wether the
  // blob file is compressed. We use this options mainly to configure the
  // compression dictionary.
  CompressionOptions blob_file_compression_options;

  // The desirable blob file size. This is not a hard limit but a wish.
  //
  // Default: 256MB
  uint64_t blob_file_target_size{256 << 20};

  // If non-NULL use the specified cache for blob records.
  //
  // Default: nullptr
  std::shared_ptr<Cache> blob_cache{nullptr};

  // Max batch size for GC.
  //
  // Default: 1GB
  uint64_t max_gc_batch_size{1 << 30};

  // Min batch size for GC.
  //
  // Default: 512MB
  uint64_t min_gc_batch_size{512 << 20};

  // The ratio of how much discardable size of a blob file can be GC.
  //
  // Default: 0.5
  double blob_file_discardable_ratio{0.5};

  // The blob file size less than this option will be mark GC.
  //
  // Default: 8MB
  uint64_t merge_small_file_threshold{8 << 20};

  // The mode used to process blob file.
  //
  // Default: kNormal
  TitanBlobRunMode blob_run_mode{TitanBlobRunMode::kNormal};

  // If set true, values in blob file will be merged to a new blob file while
  // their corresponding keys are compacted to last two level in LSM-Tree.
  //
  // With this feature enabled, Titan could get better scan performance, and
  // better write performance during GC, but will suffer around 1.1 space
  // amplification and 3 more write amplification if no GC needed (eg. uniformly
  // distributed keys) under default rocksdb setting.
  //
  // Requirement: level_compaction_dynamic_level_base = true
  // Default: false
  bool level_merge{false};

  // With level merge enabled, we expect there are no more than 10 sorted runs
  // of blob files in both of last two levels. But since last level blob files
  // won't be merged again, sorted runs in last level will increase infinitely.
  //
  // With this feature enabled, Titan will check sorted runs of compaction range
  // after each last level compaction and mark related blob files if there are
  // too many. These marked blob files will be merged to a new sorted run in
  // next compaction.
  //
  // Default: false
  bool range_merge{false};

  // Max sorted runs to trigger range merge. Decrease this value will increase
  // write amplification but get better short range scan performance.
  //
  // Default: 20
  int max_sorted_runs{20};

  // If set true, Titan will pass empty value in user compaction filter,
  // improves compaction performance by avoid fetching value from blob files.
  //
  // Default: false
  bool skip_value_in_compaction_filter{false};

  TitanCFOptions() = default;
  explicit TitanCFOptions(const ColumnFamilyOptions& options)
      : ColumnFamilyOptions(options) {}
  TitanCFOptions(const ColumnFamilyOptions&, const ImmutableTitanCFOptions&,
                 const MutableTitanCFOptions&);

  TitanCFOptions& operator=(const ColumnFamilyOptions& options) {
    *dynamic_cast<ColumnFamilyOptions*>(this) = options;
    return *this;
  }

  MemoryAllocator* memory_allocator() const {
    return blob_cache ? blob_cache->memory_allocator() : nullptr;
  }

  void Dump(Logger* logger) const;
  void UpdateMutableOptions(const MutableTitanCFOptions& new_options);
};

struct ImmutableTitanCFOptions {
  ImmutableTitanCFOptions() : ImmutableTitanCFOptions(TitanCFOptions()) {}

  explicit ImmutableTitanCFOptions(const TitanCFOptions& opts)
      : blob_file_target_size(opts.blob_file_target_size),
        blob_cache(opts.blob_cache),
        max_gc_batch_size(opts.max_gc_batch_size),
        min_gc_batch_size(opts.min_gc_batch_size),
        merge_small_file_threshold(opts.merge_small_file_threshold),
        level_merge(opts.level_merge),
        skip_value_in_compaction_filter(opts.skip_value_in_compaction_filter) {}

  uint64_t blob_file_target_size;

  std::shared_ptr<Cache> blob_cache;

  uint64_t max_gc_batch_size;

  uint64_t min_gc_batch_size;

  uint64_t merge_small_file_threshold;

  bool level_merge;

  bool skip_value_in_compaction_filter;
};

struct MutableTitanCFOptions {
  MutableTitanCFOptions() : MutableTitanCFOptions(TitanCFOptions()) {}

  explicit MutableTitanCFOptions(const TitanCFOptions& opts)
      : blob_run_mode(opts.blob_run_mode),
        min_blob_size(opts.min_blob_size),
        blob_file_compression(opts.blob_file_compression),
        blob_file_discardable_ratio(opts.blob_file_discardable_ratio) {}

  TitanBlobRunMode blob_run_mode;
  uint64_t min_blob_size;
  CompressionType blob_file_compression;
  double blob_file_discardable_ratio;
};

struct TitanOptions : public TitanDBOptions, public TitanCFOptions {
  TitanOptions() = default;
  explicit TitanOptions(const Options& options)
      : TitanDBOptions(options), TitanCFOptions(options) {}

  TitanOptions& operator=(const Options& options) {
    *static_cast<TitanDBOptions*>(this) = options;
    *static_cast<TitanCFOptions*>(this) = options;
    return *this;
  }

  operator Options() {
    Options options;
    *static_cast<DBOptions*>(&options) = *static_cast<DBOptions*>(this);
    *static_cast<ColumnFamilyOptions*>(&options) =
        *static_cast<ColumnFamilyOptions*>(this);
    return options;
  }
};

struct TitanReadOptions : public ReadOptions {
  // If true, it will just return keys without indexing value from blob files.
  // It is mainly used for the scan-delete operation after DeleteFilesInRange.
  // Cause DeleteFilesInRange may expose old blob index keys, returning key only
  // avoids referring to missing blob files.
  //
  // Default: false
  bool key_only{false};

  // If false, it will not abort when failed to get value from blob files and
  // return an empty value.
  //
  // Default: true
  bool abort_on_failure{true};

  TitanReadOptions() = default;
  explicit TitanReadOptions(const ReadOptions& options)
      : ReadOptions(options) {}

  TitanReadOptions& operator=(const ReadOptions& options) {
    *static_cast<ReadOptions*>(this) = options;
    return *this;
  }
};

}  // namespace titandb
}  // namespace rocksdb

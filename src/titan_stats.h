#pragma once

#include <array>
#include <atomic>
#include <map>
#include <string>
#include <unordered_map>

#include "logging/log_buffer.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/statistics.h"

#include "titan/options.h"

namespace rocksdb {
namespace titandb {

enum class InternalOpStatsType : int {
  COUNT = 0,
  BYTES_READ,
  BYTES_WRITTEN,
  IO_BYTES_READ,
  IO_BYTES_WRITTEN,
  INPUT_FILE_NUM,
  OUTPUT_FILE_NUM,
  GC_SAMPLING_MICROS,
  GC_READ_LSM_MICROS,
  // Update lsm and write callback
  GC_UPDATE_LSM_MICROS,
  INTERNAL_OP_STATS_ENUM_MAX,
};

enum class InternalOpType : int {
  FLUSH = 0,
  COMPACTION,
  GC,
  INTERNAL_OP_ENUM_MAX,
};

using InternalOpStats =
    std::array<std::atomic<uint64_t>,
               static_cast<size_t>(
                   InternalOpStatsType::INTERNAL_OP_STATS_ENUM_MAX)>;

// Titan internal stats does NOT optimize race
// condition by making thread local copies of
// data.
class TitanInternalStats {
 public:
  enum StatsType {
    LIVE_BLOB_SIZE = 0,
    NUM_LIVE_BLOB_FILE,
    NUM_OBSOLETE_BLOB_FILE,
    LIVE_BLOB_FILE_SIZE,
    OBSOLETE_BLOB_FILE_SIZE,
    INTERNAL_STATS_ENUM_MAX,
  };

  TitanInternalStats() { Clear(); }

  void Clear() {
    for (int stat = 0; stat < INTERNAL_STATS_ENUM_MAX; stat++) {
      stats_[stat].store(0, std::memory_order_relaxed);
    }
    for (int op = 0;
         op < static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX); op++) {
      assert(
          internal_op_stats_[op].size() ==
          static_cast<size_t>(InternalOpStatsType::INTERNAL_OP_STATS_ENUM_MAX));
      for (int stat = 0;
           stat <
           static_cast<int>(InternalOpStatsType::INTERNAL_OP_STATS_ENUM_MAX);
           stat++) {
        internal_op_stats_[op][stat].store(0, std::memory_order_relaxed);
      }
    }
  }

  void ResetStats(StatsType type) {
    stats_[type].store(0, std::memory_order_relaxed);
  }

  void AddStats(StatsType type, uint64_t value) {
    auto& v = stats_[type];
    v.fetch_add(value, std::memory_order_relaxed);
  }

  void SubStats(StatsType type, uint64_t value) {
    auto& v = stats_[type];
    v.fetch_sub(value, std::memory_order_relaxed);
  }

  bool GetIntProperty(const Slice& property, uint64_t* value) const {
    auto p = stats_type_string_map.find(property.ToString());
    if (p != stats_type_string_map.end()) {
      *value = stats_[p->second].load(std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  bool GetStringProperty(const Slice& property, std::string* value) const {
    uint64_t int_value;
    if (GetIntProperty(property, &int_value)) {
      *value = std::to_string(int_value);
      return true;
    }
    return false;
  }

  InternalOpStats* GetInternalOpStatsForType(InternalOpType type) {
    return &internal_op_stats_[static_cast<int>(type)];
  }

  void DumpAndResetInternalOpStats(LogBuffer* log_buffer);

 private:
  static const std::unordered_map<std::string, TitanInternalStats::StatsType>
      stats_type_string_map;
  static const std::array<
      std::string, static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX)>
      internal_op_names;
  std::array<std::atomic<uint64_t>, INTERNAL_STATS_ENUM_MAX> stats_;
  std::array<InternalOpStats,
             static_cast<size_t>(InternalOpType::INTERNAL_OP_ENUM_MAX)>
      internal_op_stats_;
};

class TitanStats {
 public:
  TitanStats(Statistics* stats) : stats_(stats) {}

  // TODO: Initialize corresponding internal stats struct for Column families
  // created after DB open.
  Status Initialize(std::map<uint32_t, TitanCFOptions> cf_options,
                    uint32_t default_cf) {
    for (auto& opts : cf_options) {
      internal_stats_[opts.first] = NewTitanInternalStats(opts.second);
    }
    default_cf_ = default_cf;
    return Status::OK();
  }

  Statistics* statistics() { return stats_; }

  TitanInternalStats* internal_stats(uint32_t cf_id) {
    auto p = internal_stats_.find(cf_id);
    if (p == internal_stats_.end()) {
      return nullptr;
    } else {
      return p->second.get();
    }
  }

  void DumpInternalOpStats(uint32_t cf_id, const std::string& cf_name);

 private:
  Statistics* stats_ = nullptr;
  uint32_t default_cf_ = 0;
  std::unordered_map<uint32_t, std::shared_ptr<TitanInternalStats>>
      internal_stats_;
  std::shared_ptr<TitanInternalStats> NewTitanInternalStats(
      TitanCFOptions& opts) {
    return std::make_shared<TitanInternalStats>();
  }
};

// Utility functions
inline Statistics* statistics(TitanStats* stats) {
  return (stats) ? stats->statistics() : nullptr;
}

inline void RecordTick(TitanStats* stats, uint32_t ticker_type,
                       uint64_t count = 1) {
  if (stats && stats->statistics()) {
    stats->statistics()->recordTick(ticker_type, count);
  }
}

inline void MeasureTime(TitanStats* stats, uint32_t histogram_type,
                        uint64_t time) {
  if (stats && stats->statistics()) {
    stats->statistics()->measureTime(histogram_type, time);
  }
}

inline void SetTickerCount(TitanStats* stats, uint32_t ticker_type,
                           uint64_t count) {
  if (stats && stats->statistics()) {
    stats->statistics()->setTickerCount(ticker_type, count);
  }
}

inline void ResetStats(TitanStats* stats, uint32_t cf_id,
                       TitanInternalStats::StatsType type) {
  if (stats) {
    auto p = stats->internal_stats(cf_id);
    if (p) {
      p->ResetStats(type);
    }
  }
}

inline void AddStats(TitanStats* stats, uint32_t cf_id,
                     TitanInternalStats::StatsType type, uint64_t value) {
  if (stats) {
    auto p = stats->internal_stats(cf_id);
    if (p) {
      p->AddStats(type, value);
    }
  }
}

inline void SubStats(TitanStats* stats, uint32_t cf_id,
                     TitanInternalStats::StatsType type, uint64_t value) {
  if (stats) {
    auto p = stats->internal_stats(cf_id);
    if (p) {
      p->SubStats(type, value);
    }
  }
}

inline uint64_t GetAndResetStats(InternalOpStats* stats,
                                 InternalOpStatsType type) {
  if (stats != nullptr) {
    return (*stats)[static_cast<int>(type)].exchange(0,
                                                     std::memory_order_relaxed);
  }
  return 0;
}

inline void AddStats(InternalOpStats* stats, InternalOpStatsType type,
                     uint64_t value = 1) {
  if (stats != nullptr) {
    (*stats)[static_cast<int>(type)].fetch_add(value,
                                               std::memory_order_relaxed);
  }
}

inline void SubStats(InternalOpStats* stats, InternalOpStatsType type,
                     uint64_t value = 1) {
  if (stats != nullptr) {
    (*stats)[static_cast<int>(type)].fetch_sub(value,
                                               std::memory_order_relaxed);
  }
}

// IOStatsContext helper

inline void SavePrevIOBytes(uint64_t* prev_bytes_read,
                            uint64_t* prev_bytes_written) {
  IOStatsContext* io_stats = get_iostats_context();
  if (io_stats != nullptr) {
    *prev_bytes_read = io_stats->bytes_read;
    *prev_bytes_written = io_stats->bytes_written;
  }
}

inline void UpdateIOBytes(uint64_t prev_bytes_read, uint64_t prev_bytes_written,
                          uint64_t* bytes_read, uint64_t* bytes_written) {
  IOStatsContext* io_stats = get_iostats_context();
  if (io_stats != nullptr) {
    *bytes_read += io_stats->bytes_read - prev_bytes_read;
    *bytes_written += io_stats->bytes_written - prev_bytes_written;
  }
}

class TitanStopWatch {
 public:
  TitanStopWatch(Env* env, uint64_t& stats)
      : env_(env), stats_(stats), start_(env_->NowMicros()) {}

  ~TitanStopWatch() { stats_ += env_->NowMicros() - start_; }

 private:
  Env* env_;
  uint64_t& stats_;
  uint64_t start_;
};

}  // namespace titandb
}  // namespace rocksdb

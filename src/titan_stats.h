#pragma once

#include "rocksdb/statistics.h"

#include <atomic>
#include <string>
#include <unordered_map>

namespace rocksdb {
namespace titandb {

// Titan internal stats does NOT optimize race
// condition by making thread local copies of
// data.
class TitanInternalStats {
 public:
  enum InternalStatsType {
    SIZE_BLOB_LIVE,
    NUM_BLOB_FILE,
    SIZE_BLOB_FILE,
    INTERNAL_STATS_ENUM_MAX,
  };
  void ResetStats(InternalStatsType type) {
    stats_[type].store(0, std::memory_order_relaxed);
  }
  void AddStats(InternalStatsType type, uint64_t value) {
    auto& v = stats_[type];
    v.fetch_add(value, std::memory_order_relaxed);
  }
  void SubStats(InternalStatsType type, uint64_t value) {
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
 private:
  static const std::unordered_map<std::string, TitanInternalStats::InternalStatsType>
      stats_type_string_map;
  std::atomic<uint64_t> stats_[INTERNAL_STATS_ENUM_MAX];
};

class TitanStats {
 public:
  TitanStats(Statistics* stats)
      : stats_(stats) {}
  Status Open(std::map<uint32_t, TitanCFOptions> cf_options) {
    for (auto& opts: cf_options) {
      internal_stats_[opts.first] = NewTitanInternalStats(opts.second);
    }
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
 private:
  Statistics* stats_;
  std::unordered_map<uint32_t, std::shared_ptr<TitanInternalStats>>
      internal_stats_;
  std::shared_ptr<TitanInternalStats> NewTitanInternalStats(TitanCFOptions& opts) {
    return std::make_shared<TitanInternalStats>();
  }
};

// Utility functions
inline Statistics* statistics(TitanStats* stats) {
  if (stats) {
    return stats->statistics();
  } else {
    return nullptr;
  }
}

}  // namespace titandb
}  // namespace rocksdb
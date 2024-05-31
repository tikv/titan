#include "titan_stats.h"

#include <functional>
#include <map>
#include <string>

#include "monitoring/statistics_impl.h"

#include "blob_file_set.h"
#include "blob_storage.h"
#include "titan/db.h"

namespace rocksdb {
namespace titandb {

std::shared_ptr<Statistics> CreateDBStatistics() {
  return rocksdb::CreateDBStatistics<TITAN_TICKER_ENUM_MAX,
                                     TITAN_HISTOGRAM_ENUM_MAX>();
}

static const std::string titandb_prefix = "rocksdb.titandb.";

static const std::string num_blob_files_at_level_prefix =
    "num-blob-files-at-level";
static const std::string live_blob_size = "live-blob-size";
static const std::string num_live_blob_file = "num-live-blob-file";
static const std::string num_obsolete_blob_file = "num-obsolete-blob-file";
static const std::string live_blob_file_size = "live-blob-file-size";
static const std::string obsolete_blob_file_size = "obsolete-blob-file-size";
static const std::string num_discardable_ratio_le0_file =
    "num-discardable-ratio-le0-file";
static const std::string num_discardable_ratio_le20_file =
    "num-discardable-ratio-le20-file";
static const std::string num_discardable_ratio_le50_file =
    "num-discardable-ratio-le50-file";
static const std::string num_discardable_ratio_le80_file =
    "num-discardable-ratio-le80-file";
static const std::string num_discardable_ratio_le100_file =
    "num-discardable-ratio-le100-file";

const std::string TitanDB::Properties::kNumBlobFilesAtLevelPrefix =
    titandb_prefix + num_blob_files_at_level_prefix;
const std::string TitanDB::Properties::kLiveBlobSize =
    titandb_prefix + live_blob_size;
const std::string TitanDB::Properties::kNumLiveBlobFile =
    titandb_prefix + num_live_blob_file;
const std::string TitanDB::Properties::kNumObsoleteBlobFile =
    titandb_prefix + num_obsolete_blob_file;
const std::string TitanDB::Properties::kLiveBlobFileSize =
    titandb_prefix + live_blob_file_size;
const std::string TitanDB::Properties::kObsoleteBlobFileSize =
    titandb_prefix + obsolete_blob_file_size;
const std::string TitanDB::Properties::kNumDiscardableRatioLE0File =
    titandb_prefix + num_discardable_ratio_le0_file;
const std::string TitanDB::Properties::kNumDiscardableRatioLE20File =
    titandb_prefix + num_discardable_ratio_le20_file;
const std::string TitanDB::Properties::kNumDiscardableRatioLE50File =
    titandb_prefix + num_discardable_ratio_le50_file;
const std::string TitanDB::Properties::kNumDiscardableRatioLE80File =
    titandb_prefix + num_discardable_ratio_le80_file;
const std::string TitanDB::Properties::kNumDiscardableRatioLE100File =
    titandb_prefix + num_discardable_ratio_le100_file;

const std::unordered_map<
    std::string, std::function<uint64_t(const TitanInternalStats*, Slice)>>
    TitanInternalStats::stats_type_string_map = {
        {TitanDB::Properties::kNumBlobFilesAtLevelPrefix,
         &TitanInternalStats::HandleNumBlobFilesAtLevel},
        {TitanDB::Properties::kLiveBlobSize,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::LIVE_BLOB_SIZE, std::placeholders::_2)},
        {TitanDB::Properties::kNumLiveBlobFile,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_LIVE_BLOB_FILE,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumObsoleteBlobFile,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_OBSOLETE_BLOB_FILE,
                   std::placeholders::_2)},
        {TitanDB::Properties::kLiveBlobFileSize,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::LIVE_BLOB_FILE_SIZE,
                   std::placeholders::_2)},
        {TitanDB::Properties::kObsoleteBlobFileSize,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumDiscardableRatioLE0File,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_DISCARDABLE_RATIO_LE0,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumDiscardableRatioLE20File,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_DISCARDABLE_RATIO_LE20,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumDiscardableRatioLE50File,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_DISCARDABLE_RATIO_LE50,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumDiscardableRatioLE80File,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_DISCARDABLE_RATIO_LE80,
                   std::placeholders::_2)},
        {TitanDB::Properties::kNumDiscardableRatioLE100File,
         std::bind(&TitanInternalStats::HandleStatsValue, std::placeholders::_1,
                   TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100,
                   std::placeholders::_2)},
};

const std::array<std::string,
                 static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX)>
    TitanInternalStats::internal_op_names = {{
        "Flush     ",
        "Compaction",
        "GC        ",
    }};

// Assumes that trailing numbers represent an optional argument. This requires
// property names to not end with numbers.
std::pair<Slice, Slice> GetPropertyNameAndArg(const Slice& property) {
  Slice name = property, arg = property;
  size_t sfx_len = 0;
  while (sfx_len < property.size() &&
         isdigit(property[property.size() - sfx_len - 1])) {
    ++sfx_len;
  }
  name.remove_suffix(sfx_len);
  arg.remove_prefix(property.size() - sfx_len);
  return {name, arg};
}

bool TitanInternalStats::GetIntProperty(const Slice& property,
                                        uint64_t* value) const {
  auto ppt = GetPropertyNameAndArg(property);
  auto p = stats_type_string_map.find(ppt.first.ToString());
  if (p != stats_type_string_map.end()) {
    *value = (p->second)(this, ppt.second);
    return true;
  }
  return false;
}

bool TitanInternalStats::GetStringProperty(const Slice& property,
                                           std::string* value) const {
  uint64_t int_value;
  if (GetIntProperty(property, &int_value)) {
    *value = std::to_string(int_value);
    return true;
  }
  return false;
}

uint64_t TitanInternalStats::HandleStatsValue(
    TitanInternalStats::StatsType type, Slice _arg) const {
  return stats_[type].load(std::memory_order_relaxed);
}

uint64_t TitanInternalStats::HandleNumBlobFilesAtLevel(Slice arg) const {
  auto s = arg.ToString();
  int level = ParseInt(s);
  return blob_storage_->NumBlobFilesAtLevel(level);
}

void TitanInternalStats::DumpAndResetInternalOpStats(LogBuffer* log_buffer) {
  constexpr double GB = 1.0 * 1024 * 1024 * 1024;
  constexpr double SECOND = 1.0 * 1000000;
  LogToBuffer(log_buffer,
              "OP           COUNT READ(GB)  WRITE(GB) IO_READ(GB) IO_WRITE(GB) "
              " FILE_IN FILE_OUT GC_READ(MICROS) GC_UPDATE(MICROS)");
  LogToBuffer(log_buffer,
              "----------------------------------------------------------------"
              "-----------------");
  for (int op = 0; op < static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX);
       op++) {
    LogToBuffer(
        log_buffer,
        "%s %5d %10.1f %10.1f  %10.1f   %10.1f %8d %8d %10.1f %10.1f %10.1f",
        internal_op_names[op].c_str(),
        GetAndResetStats(&internal_op_stats_[op], InternalOpStatsType::COUNT),
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::BYTES_READ) /
            GB,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::BYTES_WRITTEN) /
            GB,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::IO_BYTES_READ) /
            GB,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::IO_BYTES_WRITTEN) /
            GB,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::INPUT_FILE_NUM),
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::OUTPUT_FILE_NUM),
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::GC_READ_LSM_MICROS) /
            SECOND,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::GC_UPDATE_LSM_MICROS) /
            SECOND);
  }
}

void TitanStats::InitializeCF(uint32_t cf_id,
                              std::shared_ptr<BlobStorage> blob_storage) {
  internal_stats_[cf_id] = std::make_shared<TitanInternalStats>(blob_storage);
}

}  // namespace titandb
}  // namespace rocksdb

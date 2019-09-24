#include "titan_stats.h"
#include "titan/db.h"

#include <map>
#include <string>

namespace rocksdb {
namespace titandb {

static const std::string titandb_prefix = "rocksdb.titandb.";

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

const std::unordered_map<std::string, TitanInternalStats::StatsType>
    TitanInternalStats::stats_type_string_map = {
        {TitanDB::Properties::kLiveBlobSize,
         TitanInternalStats::LIVE_BLOB_SIZE},
        {TitanDB::Properties::kNumLiveBlobFile,
         TitanInternalStats::NUM_LIVE_BLOB_FILE},
        {TitanDB::Properties::kNumObsoleteBlobFile,
         TitanInternalStats::NUM_OBSOLETE_BLOB_FILE},
        {TitanDB::Properties::kLiveBlobFileSize,
         TitanInternalStats::LIVE_BLOB_FILE_SIZE},
        {TitanDB::Properties::kObsoleteBlobFileSize,
         TitanInternalStats::OBSOLETE_BLOB_FILE_SIZE},
        {TitanDB::Properties::kNumDiscardableRatioLE0File,
         TitanInternalStats::NUM_DISCARDABLE_RATIO_LE0},
        {TitanDB::Properties::kNumDiscardableRatioLE20File,
         TitanInternalStats::NUM_DISCARDABLE_RATIO_LE20},
        {TitanDB::Properties::kNumDiscardableRatioLE50File,
         TitanInternalStats::NUM_DISCARDABLE_RATIO_LE50},
        {TitanDB::Properties::kNumDiscardableRatioLE80File,
         TitanInternalStats::NUM_DISCARDABLE_RATIO_LE80},
        {TitanDB::Properties::kNumDiscardableRatioLE100File,
         TitanInternalStats::NUM_DISCARDABLE_RATIO_LE100},
};

const std::array<std::string,
                 static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX)>
    TitanInternalStats::internal_op_names = {{
        "Flush     ",
        "Compaction",
        "GC        ",
    }};

void TitanInternalStats::DumpAndResetInternalOpStats(LogBuffer* log_buffer) {
  constexpr double GB = 1.0 * 1024 * 1024 * 1024;
  constexpr double SECOND = 1.0 * 1000000;
  LogToBuffer(log_buffer,
              "OP           COUNT READ(GB)  WRITE(GB) IO_READ(GB) IO_WRITE(GB) "
              " FILE_IN FILE_OUT");
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
                         InternalOpStatsType::GC_SAMPLING_MICROS) /
            SECOND,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::GC_READ_LSM_MICROS) /
            SECOND,
        GetAndResetStats(&internal_op_stats_[op],
                         InternalOpStatsType::GC_UPDATE_LSM_MICROS) /
            SECOND);
  }
}

}  // namespace titandb
}  // namespace rocksdb

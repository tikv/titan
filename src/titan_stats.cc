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
};

const std::array<std::string, static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX)>
    TitanInternalStats::internal_op_names = {
  "Flush     ",
  "Compaction",
  "GC        ",
};

void TitanInternalStats::DumpAndResetInternalOpStats(LogBuffer* log_buffer) {
  constexpr double GB = 1.0 * 1024 * 1024 * 1024;
  LogToBuffer(log_buffer,
              "OP           COUNT READ(GB)  WRITE(GB) IO_READ(GB) IO_WRITE(GB) "
              " FILE_IN FILE_OUT");
  LogToBuffer(log_buffer,
              "----------------------------------------------------------------"
              "-----------------");
  for (int op = 0; op < static_cast<int>(InternalOpType::INTERNAL_OP_ENUM_MAX);
       op++) {
    LogToBuffer(
        log_buffer, "%s %5d %10.1f %10.1f  %10.1f   %10.1f %8d %8d",
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
                         InternalOpStatsType::OUTPUT_FILE_NUM));
  }
}

}  // namespace titandb
}  // namespace rocksdb

#include "titan_stats.h"
#include "titan/db.h"

#include <map>
#include <string>

namespace rocksdb {
namespace titandb {

static const std::string titandb_prefix = "rocksdb.titandb.";

static const std::string live_blob_size = "live-blob-size";
static const std::string num_blob_file = "num-blob-file";
static const std::string blob_file_size = "blob-file-size";

const std::string TitanDB::Properties::kLiveBlobSize =
    titandb_prefix + live_blob_size;
const std::string TitanDB::Properties::kNumBlobFile =
    titandb_prefix + num_blob_file;
const std::string TitanDB::Properties::kBlobFileSize =
    titandb_prefix + blob_file_size;

const std::unordered_map<std::string, TitanInternalStats::StatsType>
    TitanInternalStats::stats_type_string_map = {
        {TitanDB::Properties::kLiveBlobSize,
         TitanInternalStats::LIVE_BLOB_SIZE},
        {TitanDB::Properties::kNumBlobFile, TitanInternalStats::NUM_BLOB_FILE},
        {TitanDB::Properties::kBlobFileSize,
         TitanInternalStats::BLOB_FILE_SIZE},
};

}  // namespace titandb
}  // namespace rocksdb

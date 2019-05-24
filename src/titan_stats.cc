#include "titan_stats.h"
#include "titan/db.h"

#include <map>
#include <string>

namespace rocksdb {
namespace titandb {

static const std::string titandb_prefix = "rocksdb.titandb.";

static const std::string size_blob_live = "size-blob-live";
static const std::string num_blob_file = "num-blob-file";
static const std::string size_blob_file = "size-blob-file";

const std::string TitanDB::Properties::kSizeBlobLive =
    titandb_prefix + size_blob_live;
const std::string TitanDB::Properties::kNumBlobFile =
    titandb_prefix + num_blob_file;
const std::string TitanDB::Properties::kSizeBlobFile =
    titandb_prefix + size_blob_file;

const std::unordered_map<std::string, TitanInternalStats::StatsType>
    TitanInternalStats::stats_type_string_map = {
        {TitanDB::Properties::kSizeBlobLive,
         TitanInternalStats::SIZE_BLOB_LIVE},
        {TitanDB::Properties::kNumBlobFile, TitanInternalStats::NUM_BLOB_FILE},
        {TitanDB::Properties::kSizeBlobFile,
         TitanInternalStats::SIZE_BLOB_FILE},
};

}  // namespace titandb
}  // namespace rocksdb

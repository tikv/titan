#pragma once

#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "util/coding.h"

#include "blob_file_set.h"
#include "db_impl.h"

namespace rocksdb {
namespace titandb {

class BlobLiveBlocksCollectorFactory final
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "BlobLiveBlocksCollector"; }
};

class BlobLiveBlocksCollector final : public TablePropertiesCollector {
 public:
  const static std::string kPropertiesName;

  static bool Encode(const std::map<uint64_t, uint64_t>& blob_live_blocks,
                     std::string* result);
  static bool Decode(Slice* slice,
                     std::map<uint64_t, uint64_t>* blob_live_blocks);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobLiveBlocksCollector"; }

 private:
  std::map<uint64_t, uint64_t> blob_live_blocks_;
};

}  // namespace titandb
}  // namespace rocksdb

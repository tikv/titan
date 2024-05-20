#pragma once

#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "util/coding.h"

#include "blob_file_set.h"
#include "db_impl.h"

// BlobAlignedBlocksCollector is a TablePropertiesCollector that collects
// the mapping from file number to the number of aligned blocks in the file.
// This information is used by punch hole GC. This is not the same as the
// live_data_size. Because, to use punch hole GC, blobs have to be aligned to
// the file system block size (so that the file is still parsable after holes
// are punched). This is basically live_data_size plus the size of all the
// padding bytes divided by the file system block size.

namespace rocksdb {
namespace titandb {
class BlobAlignedBlocksCollectorFactory final
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "BlobAlignedBlocksCollector"; }

  std::shared_ptr<Logger> info_logger_;
};

class BlobAlignedBlocksCollector final : public TablePropertiesCollector {
 public:
  const static std::string kPropertiesName;

  static bool Encode(const std::map<uint64_t, uint64_t>& aligned_blocks,
                     std::string* result);
  static bool Decode(Slice* slice,
                     std::map<uint64_t, uint64_t>* aligned_blocks);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobAlignedBlocksCollector"; }

  BlobAlignedBlocksCollector() {}

 private:
  std::map<uint64_t, uint64_t> aligned_blocks_;
  std::shared_ptr<Logger> info_logger_;
};

}  // namespace titandb
}  // namespace rocksdb

#pragma once

#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "util/coding.h"

#include "blob_file_set.h"
#include "db_impl.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorFactory final
    : public TablePropertiesCollectorFactory {
 public:
  // If punch_hole_gc is enabled, then blob_file_set must be provided.
  // If blob_file_set is not provided, then punch_hole_gc will be considered
  // disabled, blob size will not align with block size.
  BlobFileSizeCollectorFactory(BlobFileSet* blob_file_set = nullptr)
      : blob_file_set_(blob_file_set) {}
  BlobFileSizeCollectorFactory(const BlobFileSizeCollectorFactory&) = delete;
  void operator=(const BlobFileSizeCollectorFactory&) = delete;
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "BlobFileSizeCollector"; }

 private:
  BlobFileSet* blob_file_set_;
};

class BlobFileSizeCollector final : public TablePropertiesCollector {
 public:
  const static std::string kPropertiesName;

  BlobFileSizeCollector(uint64_t default_block_size,
                        std::unordered_map<uint64_t, uint64_t> file_block_sizes)
      : default_block_size_(default_block_size),
        file_block_sizes_(file_block_sizes) {}

  static bool Encode(const std::map<uint64_t, uint64_t>& blob_files_size,
                     std::string* result);
  static bool Decode(Slice* slice,
                     std::map<uint64_t, uint64_t>* blob_files_size);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobFileSizeCollector"; }

 private:
  std::map<uint64_t, uint64_t> blob_files_size_;
  uint64_t default_block_size_;
  std::unordered_map<uint64_t, uint64_t> file_block_sizes_;
};

}  // namespace titandb
}  // namespace rocksdb

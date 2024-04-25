#include "blob_live_blocks_collector.h"

#include "base_db_listener.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobLiveBlocksCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /* context */) {
  return new BlobLiveBlocksCollector();
}

const std::string BlobLiveBlocksCollector::kPropertiesName =
    "TitanDB.blob_live_blocks";

bool BlobLiveBlocksCollector::Encode(
    const std::map<uint64_t, uint64_t>& blob_live_blocks, std::string* result) {
  PutVarint32(result, static_cast<uint32_t>(blob_live_blocks.size()));
  for (const auto& f_blocks : blob_live_blocks) {
    PutVarint64(result, f_blocks.first);
    PutVarint64(result, f_blocks.second);
  }
  return true;
}
bool BlobLiveBlocksCollector::Decode(
    Slice* slice, std::map<uint64_t, uint64_t>* blob_live_blocks) {
  uint32_t num = 0;
  if (!GetVarint32(slice, &num)) {
    return false;
  }
  uint64_t file_number;
  uint64_t size;
  for (uint32_t i = 0; i < num; ++i) {
    if (!GetVarint64(slice, &file_number)) {
      return false;
    }
    if (!GetVarint64(slice, &size)) {
      return false;
    }
    (*blob_live_blocks)[file_number] = size;
  }
  return true;
}

Status BlobLiveBlocksCollector::AddUserKey(const Slice& /* key */,
                                           const Slice& value, EntryType type,
                                           SequenceNumber /* seq */,
                                           uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) {
    return Status::OK();
  }

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&value));
  if (!s.ok()) {
    return s;
  }

  auto iter = blob_live_blocks_.find(index.file_number);
  if (iter == blob_live_blocks_.end()) {
    blob_live_blocks_[index.file_number] = index.blob_handle.size / 4096 + 1;
  } else {
    iter->second += index.blob_handle.size / 4096 + 1;
  }

  return Status::OK();
}

Status BlobLiveBlocksCollector::Finish(UserCollectedProperties* properties) {
  if (blob_live_blocks_.empty()) {
    return Status::OK();
  }

  std::string res;
  bool ok __attribute__((__unused__)) = Encode(blob_live_blocks_, &res);
  assert(ok);
  assert(!res.empty());
  properties->emplace(std::make_pair(kPropertiesName, res));
  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb

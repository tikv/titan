#pragma once

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

// Blob file overall format:
//
// [blob file header]
// [record head + record 1]
// [record head + record 2]
// ...
// [record head + record N]
// [blob file footer]

// Format of blob head (9 bytes):
//
//    +---------+---------+-------------+
//    |   crc   |  size   | compression |
//    +---------+---------+-------------+
//    | Fixed32 | Fixed32 |    char     |
//    +---------+---------+-------------+
//
const uint64_t kBlobHeaderSize = 8;
const uint64_t kRecordHeaderSize = 9;
const uint64_t kBlobFooterSize = BlockHandle::kMaxEncodedLength + 8 + 4;

// Format of blob record (not fixed size):
//
//    +--------------------+----------------------+
//    |        key         |        value         |
//    +--------------------+----------------------+
//    | Varint64 + key_len | Varint64 + value_len |
//    +--------------------+----------------------+
//
struct BlobRecord {
  Slice key;
  Slice value;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  size_t size() const { return key.size() + value.size(); }

  friend bool operator==(const BlobRecord& lhs, const BlobRecord& rhs);
};

class BlobEncoder {
 public:
  BlobEncoder(CompressionType compression)
      : compression_ctx_(compression),
        compression_info_(compression_opt_, compression_ctx_,
                          CompressionDict::GetEmptyDict(), compression,
                          0 /*sample_for_compression*/) {}

  void EncodeRecord(const BlobRecord& record);

  Slice GetHeader() const { return Slice(header_, sizeof(header_)); }
  Slice GetRecord() const { return record_; }

  size_t GetEncodedSize() const { return sizeof(header_) + record_.size(); }

 private:
  char header_[kRecordHeaderSize];
  Slice record_;
  std::string record_buffer_;
  std::string compressed_buffer_;
  CompressionOptions compression_opt_;
  CompressionContext compression_ctx_;
  CompressionInfo compression_info_;
};

class BlobDecoder {
 public:
  Status DecodeHeader(Slice* src);
  Status DecodeRecord(Slice* src, BlobRecord* record, OwnedSlice* buffer);

  size_t GetRecordSize() const { return record_size_; }

 private:
  uint32_t crc_{0};
  uint32_t header_crc_{0};
  uint32_t record_size_{0};
  CompressionType compression_{kNoCompression};
};

// Format of blob handle (not fixed size):
//
//    +----------+----------+
//    |  offset  |   size   |
//    +----------+----------+
//    | Varint64 | Varint64 |
//    +----------+----------+
//
struct BlobHandle {
  uint64_t offset{0};
  uint64_t size{0};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobHandle& lhs, const BlobHandle& rhs);
};

// Format of blob index (not fixed size):
//
//    +------+-------------+------------------------------------+
//    | type | file number |            blob handle             |
//    +------+-------------+------------------------------------+
//    | char |  Varint64   | Varint64(offsest) + Varint64(size) |
//    +------+-------------+------------------------------------+
//
// It is stored in LSM-Tree as the value of key, then Titan can use this blob
// index to locate actual value from blob file.
struct BlobIndex {
  enum Type : unsigned char {
    kBlobRecord = 1,
  };
  uint64_t file_number{0};
  BlobHandle blob_handle;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobIndex& lhs, const BlobIndex& rhs);
};

// Format of blob file meta (not fixed size):
//
//    +-------------+-----------+--------------+------------+
//    | file number | file size | file entries | file level |
//    +-------------+-----------+--------------+------------+
//    |  Varint64   | Varint64  |   Varint64   |  Varint32  |
//    +-------------+-----------+--------------+------------+
//    +--------------------+--------------------+
//    |    smallest key    |    largest key     |
//    +--------------------+--------------------+
//    | Varint32 + key_len | Varint32 + key_len |
//    +--------------------+--------------------+
//
// The blob file meta is stored in Titan's manifest for quick constructing of
// meta infomations of all the blob files in memory.
//
// Legacy format:
//
//    +-------------+-----------+
//    | file number | file size |
//    +-------------+-----------+
//    |  Varint64   | Varint64  |
//    +-------------+-----------+
//
class BlobFileMeta {
 public:
  enum class FileEvent {
    kInit,
    kFlushCompleted,
    kCompactionCompleted,
    kGCCompleted,
    kGCBegin,
    kGCOutput,
    kFlushOrCompactionOutput,
    kDbRestart,
    kDelete,
    kNeedMerge,
    kReset,  // reset file to normal for test
  };

  enum class FileState {
    kInit,  // file never at this state
    kNormal,
    kPendingLSM,  // waiting keys adding to LSM
    kBeingGC,     // being gced
    kPendingGC,   // output of gc, waiting gc finish and keys adding to LSM
    kObsolete,    // already gced, but wait to be physical deleted
    kToMerge,     // need merge to new blob file in next compaction
  };

  BlobFileMeta() = default;

  BlobFileMeta(uint64_t _file_number, uint64_t _file_size,
               uint64_t _file_entries, uint32_t _file_level,
               const std::string& _smallest_key,
               const std::string& _largest_key)
      : file_number_(_file_number),
        file_size_(_file_size),
        file_entries_(_file_entries),
        file_level_(_file_level),
        smallest_key_(_smallest_key),
        largest_key_(_largest_key) {}

  friend bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs);

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);
  Status DecodeFromLegacy(Slice* src);

  uint64_t file_number() const { return file_number_; }
  uint64_t file_size() const { return file_size_; }
  uint64_t file_entries() const { return file_entries_; }
  uint32_t file_level() const { return file_level_; }
  const std::string& smallest_key() const { return smallest_key_; }
  const std::string& largest_key() const { return largest_key_; }

  FileState file_state() const { return state_; }
  bool is_obsolete() const { return state_ == FileState::kObsolete; }
  uint64_t discardable_size() const { return discardable_size_; }

  bool gc_mark() const { return gc_mark_; }
  void set_gc_mark(bool mark) { gc_mark_ = mark; }

  void FileStateTransit(const FileEvent& event);

  void AddDiscardableSize(uint64_t _discardable_size);
  double GetDiscardableRatio() const;
  bool NoLiveData() {
    return discardable_size_ == file_size_ - kBlobHeaderSize - kBlobFooterSize;
  }
  TitanInternalStats::StatsType GetDiscardableRatioLevel() const;

 private:
  // Persistent field
  uint64_t file_number_{0};
  uint64_t file_size_{0};
  uint64_t file_entries_;
  // Target level of compaction/flush which generates this blob file
  uint32_t file_level_;
  // Empty `smallest_key_` and `largest_key_` means smallest key is unknown,
  // and can only happen when the file is from legacy version.
  std::string smallest_key_;
  std::string largest_key_;

  // Not persistent field
  FileState state_{FileState::kInit};

  uint64_t discardable_size_{0};
  // gc_mark is set to true when this file is recovered from re-opening the DB
  // that means this file needs to be checked for GC
  bool gc_mark_{false};
};

// Format of blob file header (8 bytes):
//
//    +--------------+---------+
//    | magic number | version |
//    +--------------+---------+
//    |   Fixed32    | Fixed32 |
//    +--------------+---------+
//
// The header is mean to be compatible with header of BlobDB blob files, except
// we use a different magic number.
struct BlobFileHeader {
  // The first 32bits from $(echo titandb/blob | sha1sum).
  static const uint32_t kHeaderMagicNumber = 0x2be0a614ul;
  static const uint32_t kVersion1 = 1;
  static const uint64_t kEncodedLength = 4 + 4;

  uint32_t version = kVersion1;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);
};

// Format of blob file footer (BlockHandle::kMaxEncodedLength + 12):
//
//    +---------------------+-------------+--------------+----------+
//    |  meta index handle  |   padding   | magic number | checksum |
//    +---------------------+-------------+--------------+----------+
//    | Varint64 + Varint64 | padding_len |   Fixed64    | Fixed32  |
//    +---------------------+-------------+--------------+----------+
//
// To make the blob file footer fixed size,
// the padding_len is `BlockHandle::kMaxEncodedLength - meta_handle_len`
struct BlobFileFooter {
  // The first 64bits from $(echo titandb/blob | sha1sum).
  static const uint64_t kFooterMagicNumber{0x2be0a6148e39edc6ull};
  static const uint64_t kEncodedLength{kBlobFooterSize};

  BlockHandle meta_index_handle{BlockHandle::NullBlockHandle()};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs);
};

// A convenient template to decode a const slice.
template <typename T>
Status DecodeInto(const Slice& src, T* target) {
  auto tmp = src;
  auto s = target->DecodeFrom(&tmp);
  if (s.ok() && !tmp.empty()) {
    s = Status::Corruption(Slice());
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb

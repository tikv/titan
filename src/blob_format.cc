#include "blob_format.h"

#include "test_util/sync_point.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

namespace {

bool GetChar(Slice* src, unsigned char* value) {
  if (src->size() < 1) return false;
  *value = *src->data();
  src->remove_prefix(1);
  return true;
}

}  // namespace

void BlobRecord::EncodeTo(std::string* dst) const {
  PutLengthPrefixedSlice(dst, key);
  PutLengthPrefixedSlice(dst, value);
}

Status BlobRecord::DecodeFrom(Slice* src) {
  if (!GetLengthPrefixedSlice(src, &key) ||
      !GetLengthPrefixedSlice(src, &value)) {
    return Status::Corruption("BlobRecord");
  }
  return Status::OK();
}

bool operator==(const BlobRecord& lhs, const BlobRecord& rhs) {
  return lhs.key == rhs.key && lhs.value == rhs.value;
}

void BlobEncoder::EncodeRecord(const BlobRecord& record) {
  record_buffer_.clear();
  compressed_buffer_.clear();

  CompressionType compression;
  record.EncodeTo(&record_buffer_);
  record_ = Compress(compression_info_, record_buffer_, &compressed_buffer_,
                     &compression);

  assert(record_.size() < std::numeric_limits<uint32_t>::max());
  EncodeFixed32(header_ + 4, static_cast<uint32_t>(record_.size()));
  header_[8] = compression;

  uint32_t crc = crc32c::Value(header_ + 4, sizeof(header_) - 4);
  crc = crc32c::Extend(crc, record_.data(), record_.size());
  EncodeFixed32(header_, crc);
}

Status BlobDecoder::DecodeHeader(Slice* src) {
  if (!GetFixed32(src, &crc_)) {
    return Status::Corruption("BlobHeader");
  }
  header_crc_ = crc32c::Value(src->data(), kBlobHeaderSize - 4);

  unsigned char compression;
  if (!GetFixed32(src, &record_size_) || !GetChar(src, &compression)) {
    return Status::Corruption("BlobHeader");
  }
  compression_ = static_cast<CompressionType>(compression);

  return Status::OK();
}

Status BlobDecoder::DecodeRecord(Slice* src, BlobRecord* record,
                                 OwnedSlice* buffer) {
  TEST_SYNC_POINT_CALLBACK("BlobDecoder::DecodeRecord", &crc_);

  Slice input(src->data(), record_size_);
  src->remove_prefix(record_size_);
  uint32_t crc = crc32c::Extend(header_crc_, input.data(), input.size());
  if (crc != crc_) {
    return Status::Corruption("BlobRecord", "checksum mismatch");
  }

  if (compression_ == kNoCompression) {
    return DecodeInto(input, record);
  }
  UncompressionContext ctx(compression_);
  UncompressionInfo info(ctx, UncompressionDict::GetEmptyDict(), compression_);
  Status s = Uncompress(info, input, buffer);
  if (!s.ok()) {
    return s;
  }
  return DecodeInto(*buffer, record);
}

void BlobHandle::EncodeTo(std::string* dst) const {
  PutVarint64(dst, offset);
  PutVarint64(dst, size);
}

Status BlobHandle::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &offset) || !GetVarint64(src, &size)) {
    return Status::Corruption("BlobHandle");
  }
  return Status::OK();
}

bool operator==(const BlobHandle& lhs, const BlobHandle& rhs) {
  return lhs.offset == rhs.offset && lhs.size == rhs.size;
}

void BlobIndex::EncodeTo(std::string* dst) const {
  dst->push_back(kBlobRecord);
  PutVarint64(dst, file_number);
  blob_handle.EncodeTo(dst);
}

Status BlobIndex::DecodeFrom(Slice* src) {
  unsigned char type;
  if (!GetChar(src, &type) || type != kBlobRecord ||
      !GetVarint64(src, &file_number)) {
    return Status::Corruption("BlobIndex");
  }
  Status s = blob_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobIndex", s.ToString());
  }
  return s;
}

bool operator==(const BlobIndex& lhs, const BlobIndex& rhs) {
  return (lhs.file_number == rhs.file_number &&
          lhs.blob_handle == rhs.blob_handle);
}

void BlobFileMeta::EncodeTo(std::string* dst) const {
  PutVarint64(dst, file_number_);
  PutVarint64(dst, file_size_);
  PutLengthPrefixedSlice(dst, smallest_key_);
  PutLengthPrefixedSlice(dst, largest_key_);
}

Status BlobFileMeta::DecodeFromLegacy(Slice* src) {
  if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_)) {
    return Status::Corruption("BlobFileMeta decode failed");
  }
  assert(smallest_key_.empty());
  assert(largest_key_.empty());
  return Status::OK();
}

Status BlobFileMeta::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &file_number_) || !GetVarint64(src, &file_size_)) {
    return Status::Corruption("BlobFileMeta decode failed");
  }
  Slice str;
  if (GetLengthPrefixedSlice(src, &str) && str.size() != 0) {
    smallest_key_.assign(str.data(), str.size());
  } else {
    return Status::Corruption("BlobSmallestKey Decode failed");
  }
  if (GetLengthPrefixedSlice(src, &str) && str.size() != 0) {
    largest_key_.assign(str.data(), str.size());
  } else {
    return Status::Corruption("BlobLargestKey decode failed");
  }
  return Status::OK();
}

bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs) {
  return (lhs.file_number_ == rhs.file_number_ &&
          lhs.file_size_ == rhs.file_size_);
}

void BlobFileMeta::FileStateTransit(const FileEvent& event) {
  switch (event) {
    case FileEvent::kFlushCompleted:
      // blob file maybe generated by flush or gc, because gc will rewrite valid
      // keys to memtable. If it's generated by gc, we will leave gc to change
      // its file state. If it's generated by flush, we need to change it to
      // normal state after flush completed.
      assert(state_ == FileState::kPendingLSM ||
             state_ == FileState::kPendingGC || state_ == FileState::kNormal ||
             state_ == FileState::kBeingGC);
      if (state_ == FileState::kPendingLSM) state_ = FileState::kNormal;
      break;
    case FileEvent::kGCCompleted:
      // file is marked obsoleted during gc
      if (state_ == FileState::kObsolete) {
        break;
      }
      assert(state_ == FileState::kPendingGC || state_ == FileState::kBeingGC);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kCompactionCompleted:
      assert(state_ == FileState::kPendingLSM);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kGCBegin:
      assert(state_ == FileState::kNormal);
      state_ = FileState::kBeingGC;
      break;
    case FileEvent::kGCOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingGC;
      break;
    case FileEvent::kFlushOrCompactionOutput:
      assert(state_ == FileState::kInit);
      state_ = FileState::kPendingLSM;
      break;
    case FileEvent::kDbRestart:
      assert(state_ == FileState::kInit);
      state_ = FileState::kNormal;
      break;
    case FileEvent::kDelete:
      assert(state_ != FileState::kObsolete);
      state_ = FileState::kObsolete;
      break;
    default:
      assert(false);
  }
}

void BlobFileMeta::AddDiscardableSize(uint64_t _discardable_size) {
  assert(_discardable_size < file_size_);
  discardable_size_ += _discardable_size;
  assert(discardable_size_ < file_size_);
}

double BlobFileMeta::GetDiscardableRatio() const {
  return static_cast<double>(discardable_size_) /
         static_cast<double>(file_size_);
}

void BlobFileHeader::EncodeTo(std::string* dst) const {
  PutFixed32(dst, kHeaderMagicNumber);
  PutFixed32(dst, version);
}

Status BlobFileHeader::DecodeFrom(Slice* src) {
  uint32_t magic_number = 0;
  if (!GetFixed32(src, &magic_number) || magic_number != kHeaderMagicNumber) {
    return Status::Corruption(
        "Blob file header magic number missing or mismatched.");
  }
  if (!GetFixed32(src, &version) || version != kVersion1) {
    return Status::Corruption("Blob file header version missing or invalid.");
  }
  return Status::OK();
}

void BlobFileFooter::EncodeTo(std::string* dst) const {
  auto size = dst->size();
  meta_index_handle.EncodeTo(dst);
  // Add padding to make a fixed size footer.
  dst->resize(size + kEncodedLength - 12);
  PutFixed64(dst, kFooterMagicNumber);
  Slice encoded(dst->data() + size, dst->size() - size);
  PutFixed32(dst, crc32c::Value(encoded.data(), encoded.size()));
}

Status BlobFileFooter::DecodeFrom(Slice* src) {
  auto data = src->data();
  Status s = meta_index_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobFileFooter", s.ToString());
  }
  // Remove padding.
  src->remove_prefix(data + kEncodedLength - 12 - src->data());
  uint64_t magic_number = 0;
  if (!GetFixed64(src, &magic_number) || magic_number != kFooterMagicNumber) {
    return Status::Corruption("BlobFileFooter", "magic number");
  }
  Slice decoded(data, src->data() - data);
  uint32_t checksum = 0;
  if (!GetFixed32(src, &checksum) ||
      crc32c::Value(decoded.data(), decoded.size()) != checksum) {
    return Status::Corruption("BlobFileFooter", "checksum");
  }
  return Status::OK();
}

bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs) {
  return (lhs.meta_index_handle.offset() == rhs.meta_index_handle.offset() &&
          lhs.meta_index_handle.size() == rhs.meta_index_handle.size());
}

}  // namespace titandb
}  // namespace rocksdb

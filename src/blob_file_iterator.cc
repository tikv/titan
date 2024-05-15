#include "iostream"

#include "blob_file_iterator.h"

#include "table/block_based/block_based_table_reader.h"
#include "util/crc32c.h"

#include "blob_file_reader.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

BlobFileIterator::BlobFileIterator(
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_name,
    uint64_t file_size, const TitanCFOptions& titan_cf_options)
    : file_(std::move(file)),
      file_number_(file_name),
      file_size_(file_size),
      titan_cf_options_(titan_cf_options) {}

BlobFileIterator::~BlobFileIterator() {}

bool BlobFileIterator::Init() {
  Slice slice;
  char header_buf[BlobFileHeader::kMaxEncodedLength];
  // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
  // is only used for GC, we always set for_compaction to true.
  status_ =
      file_->Read(IOOptions(), 0, BlobFileHeader::kMaxEncodedLength, &slice,
                  header_buf, nullptr /*aligned_buf*/, true /*for_compaction*/);
  if (!status_.ok()) {
    return false;
  }
  BlobFileHeader blob_file_header;
  status_ = DecodeInto(slice, &blob_file_header, true /*ignore_extra_bytes*/);
  if (!status_.ok()) {
    return false;
  }

  header_size_ = blob_file_header.size();

  char footer_buf[BlobFileFooter::kEncodedLength];
  // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
  // is only used for GC, we always set for_compaction to true.
  status_ =
      file_->Read(IOOptions(), file_size_ - BlobFileFooter::kEncodedLength,
                  BlobFileFooter::kEncodedLength, &slice, footer_buf,
                  nullptr /*aligned_buf*/, true /*for_compaction*/);
  if (!status_.ok()) return false;
  BlobFileFooter blob_file_footer;
  status_ = blob_file_footer.DecodeFrom(&slice);
  end_of_blob_record_ = file_size_ - BlobFileFooter::kEncodedLength;
  if (!blob_file_footer.meta_index_handle.IsNull()) {
    end_of_blob_record_ -= (blob_file_footer.meta_index_handle.size() +
                            BlockBasedTable::kBlockTrailerSize);
  }

  alignment_size_ = blob_file_footer.alignment_size;

  if (blob_file_header.flags & BlobFileHeader::kHasUncompressionDictionary) {
    status_ = InitUncompressionDict(blob_file_footer, file_.get(),
                                    &uncompression_dict_,
                                    titan_cf_options_.memory_allocator());
    if (!status_.ok()) {
      return false;
    }
    decoder_.SetUncompressionDict(uncompression_dict_.get());
    // the layout of blob file is like:
    // |  ....   |
    // | records |
    // | compression dict + kBlockTrailerSize(5) |
    // | metaindex block(40) + kBlockTrailerSize(5) |
    // | footer(kEncodedLength: 32) |
    end_of_blob_record_ -= (uncompression_dict_->GetRawDict().size() +
                            BlockBasedTable::kBlockTrailerSize);
  }

  assert(end_of_blob_record_ > BlobFileHeader::kMinEncodedLength);
  init_ = true;
  return true;
}

void BlobFileIterator::SeekToFirst() {
  if (!init_ && !Init()) return;
  status_ = Status::OK();
  iterate_offset_ = header_size_;
  if (alignment_size_ != 0) {
    AdjustOffsetToNextAlignment();
  }
  PrefetchAndGet();
}

bool BlobFileIterator::Valid() const { return valid_ && status().ok(); }

void BlobFileIterator::Next() {
  assert(init_);
  PrefetchAndGet();
}

Slice BlobFileIterator::key() const { return cur_blob_record_.key; }

Slice BlobFileIterator::value() const { return cur_blob_record_.value; }

void BlobFileIterator::IterateForPrev(uint64_t offset) {
  if (!init_ && !Init()) return;

  status_ = Status::OK();

  if (offset >= end_of_blob_record_) {
    iterate_offset_ = offset;
    status_ = Status::InvalidArgument("Out of bound");
    return;
  }

  uint64_t total_length = 0;
  FixedSlice<kRecordHeaderSize> header_buffer;
  iterate_offset_ = header_size_;
  for (; iterate_offset_ < offset;) {
    // With for_compaction=true, rate_limiter is enabled. Since
    // BlobFileIterator is only used for GC, we always set for_compaction to
    // true.
    status_ = file_->Read(IOOptions(), iterate_offset_, kRecordHeaderSize,
                          &header_buffer, header_buffer.get(),
                          nullptr /*aligned_buf*/, true /*for_compaction*/);
    if (!status_.ok()) return;
    status_ = decoder_.DecodeHeader(&header_buffer);
    if (!status_.ok()) return;
    total_length = kRecordHeaderSize + decoder_.GetRecordSize();
    iterate_offset_ += total_length;
    uint64_t padding = 0;
    if (alignment_size_ != 0) {
      padding = alignment_size_ - (iterate_offset_ % alignment_size_);
    }
    iterate_offset_ += padding;
    total_length += padding;
  }

  if (iterate_offset_ > offset) iterate_offset_ -= total_length;
  valid_ = false;
}

void BlobFileIterator::AdjustOffsetToNextAlignment() {
  if (alignment_size_ == 0) return;
  uint64_t remainder = iterate_offset_ % alignment_size_;
  if (remainder != 0) {
    iterate_offset_ += alignment_size_ - remainder;
  }
}

bool BlobFileIterator::GetBlobRecord() {
  FixedSlice<kRecordHeaderSize> header_buffer;
  // With for_compaction=true, rate_limiter is enabled. Since
  // BlobFileIterator is only used for GC, we always set for_compaction to
  // true.
  status_ = file_->Read(IOOptions(), iterate_offset_, kRecordHeaderSize,
                        &header_buffer, header_buffer.get(),
                        nullptr /*aligned_buf*/, true /*for_compaction*/);
  if (!status_.ok()) return false;
  // If the header buffer is all zero, it means the record is deleted (punch
  // hole).
  bool deleted = true;
  for (size_t i = 0; i < kRecordHeaderSize; i++) {
    if (header_buffer[i] != 0) {
      deleted = false;
      break;
    }
  }
  if (deleted) {
    iterate_offset_ += alignment_size_;
    AdjustOffsetToNextAlignment();
    return false;
  }

  status_ = decoder_.DecodeHeader(&header_buffer);
  if (!status_.ok()) return false;
  Slice record_slice;
  auto record_size = decoder_.GetRecordSize();
  buffer_.resize(record_size);
  // With for_compaction=true, rate_limiter is enabled. Since BlobFileIterator
  // is only used for GC, we always set for_compaction to true.
  status_ = file_->Read(IOOptions(), iterate_offset_ + kRecordHeaderSize,
                        record_size, &record_slice, buffer_.data(),
                        nullptr /*aligned_buf*/, true /*for_compaction*/);
  if (status_.ok()) {
    status_ =
        decoder_.DecodeRecord(&record_slice, &cur_blob_record_, &uncompressed_,
                              titan_cf_options_.memory_allocator());
  }
  if (!status_.ok()) return false;

  cur_record_offset_ = iterate_offset_;
  cur_record_size_ = kRecordHeaderSize + record_size;
  iterate_offset_ += cur_record_size_;
  // align to next record
  AdjustOffsetToNextAlignment();
  valid_ = true;
  return true;
}

void BlobFileIterator::PrefetchAndGet() {
  while (iterate_offset_ < end_of_blob_record_) {
    // TODO: maybe reduce read ahead when encountering punch holes. e.g. just
    // read header.
    if (readahead_begin_offset_ > iterate_offset_ ||
        readahead_end_offset_ < iterate_offset_) {
      // alignment
      readahead_begin_offset_ =
          iterate_offset_ - (iterate_offset_ & (kDefaultPageSize - 1));
      readahead_end_offset_ = readahead_begin_offset_;
      readahead_size_ = kMinReadaheadSize;
    }
    auto min_blob_size =
        iterate_offset_ + kRecordHeaderSize + titan_cf_options_.min_blob_size;
    if (readahead_end_offset_ <= min_blob_size) {
      while (readahead_end_offset_ + readahead_size_ <= min_blob_size &&
             readahead_size_ < kMaxReadaheadSize)
        readahead_size_ <<= 1;
      status_ = file_->Prefetch(readahead_end_offset_, readahead_size_);
      if (!status_.ok()) return;
      readahead_end_offset_ += readahead_size_;
      readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ << 1);
    }

    bool live = GetBlobRecord();

    if (readahead_end_offset_ < iterate_offset_) {
      readahead_end_offset_ = iterate_offset_;
    }

    // If the record is valid (not punch-holed), we can return. Otherwise,
    // continue iterating until we find a valid record.
    if (live) return;
    iterate_offset_ += alignment_size_;
  }
  valid_ = false;
}

BlobFileMergeIterator::BlobFileMergeIterator(
    std::vector<std::unique_ptr<BlobFileIterator>>&& blob_file_iterators,
    const Comparator* comparator)
    : blob_file_iterators_(std::move(blob_file_iterators)),
      min_heap_(BlobFileIterComparator(comparator)) {}

bool BlobFileMergeIterator::Valid() const {
  if (current_ == nullptr) return false;
  if (!status().ok()) return false;
  return current_->Valid() && current_->status().ok();
}

void BlobFileMergeIterator::SeekToFirst() {
  for (auto& iter : blob_file_iterators_) {
    iter->SeekToFirst();
    if (iter->status().ok() && iter->Valid()) min_heap_.push(iter.get());
  }
  if (!min_heap_.empty()) {
    current_ = min_heap_.top();
    min_heap_.pop();
  } else {
    status_ = Status::Aborted("No iterator is valid");
  }
}

void BlobFileMergeIterator::Next() {
  assert(Valid());
  current_->Next();
  if (current_->status().ok() && current_->Valid()) min_heap_.push(current_);
  if (!min_heap_.empty()) {
    current_ = min_heap_.top();
    min_heap_.pop();
  } else {
    current_ = nullptr;
  }
}

Slice BlobFileMergeIterator::key() const {
  assert(current_ != nullptr);
  return current_->key();
}

Slice BlobFileMergeIterator::value() const {
  assert(current_ != nullptr);
  return current_->value();
}

}  // namespace titandb
}  // namespace rocksdb

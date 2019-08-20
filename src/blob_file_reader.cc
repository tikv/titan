#include "blob_file_reader.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "file/filename.h"
#include "util/crc32c.h"
#include "util/string_util.h"
#include "test_util/sync_point.h"

#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                         const TitanDBOptions& db_options,
                         const EnvOptions& env_options, Env* env,
                         std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<RandomAccessFile> file;
  auto file_name = BlobFileName(db_options.dirname, file_number);
  Status s = env->NewRandomAccessFile(file_name, &file, env_options);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  // Currently only `BlobGCJob` will call `NewBlobFileReader()`. We set
  // `for_compaction=true` in this case to enable rate limiter.
  result->reset(new RandomAccessFileReader(
      std::move(file), file_name, nullptr /*env*/, nullptr /*stats*/,
      0 /*hist_type*/, nullptr /*file_read_hist*/, env_options.rate_limiter,
      true /*for compaction*/));
  return s;
}

const uint64_t kMaxReadaheadSize = 256 << 10;

namespace {

void GenerateCachePrefix(std::string* dst, Cache* cc, RandomAccessFile* file) {
  char buffer[kMaxVarint64Length * 3 + 1];
  auto size = file->GetUniqueId(buffer, sizeof(buffer));
  if (size == 0) {
    auto end = EncodeVarint64(buffer, cc->NewId());
    size = end - buffer;
  }
  dst->assign(buffer, size);
}

void EncodeBlobCache(std::string* dst, const Slice& prefix, uint64_t offset) {
  dst->assign(prefix.data(), prefix.size());
  PutVarint64(dst, offset);
}

}  // namespace

Status BlobFileReader::Open(const TitanCFOptions& options,
                            std::unique_ptr<RandomAccessFileReader> file,
                            uint64_t file_size,
                            std::unique_ptr<BlobFileReader>* result,
                            TitanStats* stats) {
  if (file_size < BlobFileFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be a blob file");
  }

  FixedSlice<BlobFileFooter::kEncodedLength> buffer;
  Status s = file->Read(file_size - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength, &buffer, buffer.get());
  if (!s.ok()) {
    return s;
  }

  BlobFileFooter footer;
  s = DecodeInto(buffer, &footer);
  if (!s.ok()) {
    return s;
  }

  auto reader = new BlobFileReader(options, std::move(file), stats);
  reader->footer_ = footer;
  result->reset(reader);
  return Status::OK();
}

BlobFileReader::BlobFileReader(const TitanCFOptions& options,
                               std::unique_ptr<RandomAccessFileReader> file,
                               TitanStats* stats)
    : options_(options),
      file_(std::move(file)),
      cache_(options.blob_cache),
      stats_(stats) {
  if (cache_) {
    GenerateCachePrefix(&cache_prefix_, cache_.get(), file_->file());
  }
}

Status BlobFileReader::Get(const ReadOptions& /*options*/,
                           const BlobHandle& handle, BlobRecord* record,
                           PinnableSlice* buffer) {
  TEST_SYNC_POINT("BlobFileReader::Get");

  std::string cache_key;
  Cache::Handle* cache_handle = nullptr;
  if (cache_) {
    EncodeBlobCache(&cache_key, cache_prefix_, handle.offset);
    cache_handle = cache_->Lookup(cache_key);
    if (cache_handle) {
      RecordTick(stats_, BLOCK_CACHE_DATA_HIT);
      RecordTick(stats_, BLOCK_CACHE_HIT);
      auto blob = reinterpret_cast<OwnedSlice*>(cache_->Value(cache_handle));
      buffer->PinSlice(*blob, UnrefCacheHandle, cache_.get(), cache_handle);
      return DecodeInto(*blob, record);
    }
  }
  RecordTick(stats_, BLOCK_CACHE_DATA_MISS);
  RecordTick(stats_, BLOCK_CACHE_MISS);

  OwnedSlice blob;
  Status s = ReadRecord(handle, record, &blob);
  if (!s.ok()) {
    return s;
  }

  if (cache_) {
    auto cache_value = new OwnedSlice(std::move(blob));
    auto cache_size = cache_value->size() + sizeof(*cache_value);
    cache_->Insert(cache_key, cache_value, cache_size,
                   &DeleteCacheValue<OwnedSlice>, &cache_handle);
    buffer->PinSlice(*cache_value, UnrefCacheHandle, cache_.get(),
                     cache_handle);
  } else {
    buffer->PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
  }

  return Status::OK();
}

Status BlobFileReader::ReadRecord(const BlobHandle& handle, BlobRecord* record,
                                  OwnedSlice* buffer) {
  Slice blob;
  CacheAllocationPtr ubuf(new char[handle.size]);
  Status s = file_->Read(handle.offset, handle.size, &blob, ubuf.get());
  if (!s.ok()) {
    return s;
  }
  if (handle.size != static_cast<uint64_t>(blob.size())) {
    return Status::Corruption(
        "ReadRecord actual size: " + ToString(blob.size()) +
        " not equal to blob size " + ToString(handle.size));
  }

  BlobDecoder decoder;
  s = decoder.DecodeHeader(&blob);
  if (!s.ok()) {
    return s;
  }
  buffer->reset(std::move(ubuf), blob);
  s = decoder.DecodeRecord(&blob, record, buffer);
  return s;
}

Status BlobFilePrefetcher::Get(const ReadOptions& options,
                               const BlobHandle& handle, BlobRecord* record,
                               PinnableSlice* buffer) {
  if (handle.offset == last_offset_) {
    last_offset_ = handle.offset + handle.size;
    if (handle.offset + handle.size > readahead_limit_) {
      readahead_size_ = std::max(handle.size, readahead_size_);
      reader_->file_->Prefetch(handle.offset, readahead_size_);
      readahead_limit_ = handle.offset + readahead_size_;
      readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
    }
  } else {
    last_offset_ = handle.offset + handle.size;
    readahead_size_ = 0;
    readahead_limit_ = 0;
  }

  return reader_->Get(options, handle, record, buffer);
}

}  // namespace titandb
}  // namespace rocksdb

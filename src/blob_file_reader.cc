#include "blob_file_reader.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include "file/filename.h"
#include "file/readahead_raf.h"
#include "table/block_based/block.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/string_util.h"

#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                         const TitanDBOptions& db_options,
                         const EnvOptions& env_options, Env* env,
                         std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<FSRandomAccessFile> file;
  auto file_name = BlobFileName(db_options.dirname, file_number);
  Status s = env->GetFileSystem()->NewRandomAccessFile(
      file_name, FileOptions(env_options), &file, nullptr /*dbg*/);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  result->reset(new RandomAccessFileReader(
      std::move(file), file_name, nullptr /*clock*/, nullptr /*io_tracer*/,
      nullptr /*stats*/, 0 /*hist_type*/, nullptr /*file_read_hist*/,
      env_options.rate_limiter));
  return s;
}

const uint64_t kMaxReadaheadSize = 256 << 10;

namespace {

void GenerateCachePrefix(std::string* dst, Cache* cc,
                         FSRandomAccessFile* file) {
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

// Seek to the specified meta block.
// Return true if it successfully seeks to that block.
Status SeekToMetaBlock(InternalIterator* meta_iter,
                       const std::string& block_name, bool* is_found,
                       BlockHandle* block_handle = nullptr) {
  if (block_handle != nullptr) {
    *block_handle = BlockHandle::NullBlockHandle();
  }
  *is_found = false;
  meta_iter->Seek(block_name);
  if (meta_iter->status().ok() && meta_iter->Valid() &&
      meta_iter->key() == block_name) {
    *is_found = true;
    if (block_handle) {
      Slice v = meta_iter->value();
      return block_handle->DecodeFrom(&v);
    }
  }
  return meta_iter->status();
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

  BlobFileHeader header;
  Status s = ReadHeader(file, &header);
  if (!s.ok()) {
    return s;
  }

  FixedSlice<BlobFileFooter::kEncodedLength> buffer;
  s = file->Read(IOOptions(), file_size - BlobFileFooter::kEncodedLength,
                 BlobFileFooter::kEncodedLength, &buffer, buffer.get(),
                 nullptr /*aligned_buf*/);
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
  if (header.flags & BlobFileHeader::kHasUncompressionDictionary) {
    s = InitUncompressionDict(footer, reader->file_.get(),
                              &reader->uncompression_dict_);
    if (!s.ok()) {
      return s;
    }
  }
  result->reset(reader);
  return Status::OK();
}

Status BlobFileReader::ReadHeader(std::unique_ptr<RandomAccessFileReader>& file,
                                  BlobFileHeader* header) {
  FixedSlice<BlobFileHeader::kMaxEncodedLength> buffer;
  Status s = file->Read(IOOptions(), 0, BlobFileHeader::kMaxEncodedLength,
                        &buffer, buffer.get(), nullptr /*aligned_buf*/);
  if (!s.ok()) return s;

  s = DecodeInto(buffer, header, true /*ignore_extra_bytes*/);

  return s;
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
      RecordTick(statistics(stats_), TITAN_BLOB_CACHE_HIT);
      auto blob = reinterpret_cast<OwnedSlice*>(cache_->Value(cache_handle));
      buffer->PinSlice(*blob, UnrefCacheHandle, cache_.get(), cache_handle);
      return DecodeInto(*blob, record);
    }
  }
  RecordTick(statistics(stats_), TITAN_BLOB_CACHE_MISS);

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
  Status s = file_->Read(IOOptions(), handle.offset, handle.size, &blob,
                         ubuf.get(), nullptr /*aligned_buf*/);
  if (!s.ok()) {
    return s;
  }
  if (handle.size != static_cast<uint64_t>(blob.size())) {
    return Status::Corruption(
        "ReadRecord actual size: " + ToString(blob.size()) +
        " not equal to blob size " + ToString(handle.size));
  }

  BlobDecoder decoder(uncompression_dict_ == nullptr
                          ? &UncompressionDict::GetEmptyDict()
                          : uncompression_dict_.get());
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

Status InitUncompressionDict(
    const BlobFileFooter& footer, RandomAccessFileReader* file,
    std::unique_ptr<UncompressionDict>* uncompression_dict) {
  // TODO: Cache the compression dictionary in either block cache or blob cache.
#if ZSTD_VERSION_NUMBER < 10103
  return Status::NotSupported("the version of libztsd is too low");
#endif
  // 1. read meta index block
  // 2. read dictionary
  // 3. reset the dictionary
  assert(footer.meta_index_handle.size() > 0);
  BlockHandle meta_index_handle = footer.meta_index_handle;
  Slice blob;
  CacheAllocationPtr ubuf(new char[meta_index_handle.size()]);
  Status s = file->Read(IOOptions(), meta_index_handle.offset(),
                        meta_index_handle.size(), &blob, ubuf.get(),
                        nullptr /*aligned_buf*/);
  if (!s.ok()) {
    return s;
  }
  BlockContents meta_block_content(std::move(ubuf), meta_index_handle.size());

  std::unique_ptr<Block> meta(
      new Block(std::move(meta_block_content), kDisableGlobalSequenceNumber));

  std::unique_ptr<InternalIterator> meta_iter(meta->NewDataIterator(
      BytewiseComparator(), kDisableGlobalSequenceNumber));

  bool dict_is_found = false;
  BlockHandle dict_block;
  s = SeekToMetaBlock(meta_iter.get(), kCompressionDictBlockName,
                      &dict_is_found, &dict_block);
  if (!s.ok()) {
    return s;
  }

  if (!dict_is_found) {
    return Status::NotFound("uncompression dict");
  }

  Slice dict_slice;
  CacheAllocationPtr dict_buf(new char[dict_block.size()]);
  s = file->Read(IOOptions(), dict_block.offset(), dict_block.size(),
                 &dict_slice, dict_buf.get(), nullptr /*aligned_buf*/);
  if (!s.ok()) {
    return s;
  }

  std::string dict_str(dict_buf.get(), dict_buf.get() + dict_block.size());
  uncompression_dict->reset(new UncompressionDict(dict_str, true));

  return s;
}

}  // namespace titandb
}  // namespace rocksdb

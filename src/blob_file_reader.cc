#include "blob_file_reader.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include "file/filename.h"
#include "file/readahead_raf.h"
#include "rocksdb/cache.h"
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
                              &reader->uncompression_dict_,
                              options.memory_allocator());
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
                               TitanStats* _stats)
    : options_(options), file_(std::move(file)) {}

Status BlobFileReader::Get(const ReadOptions& _options,
                           const BlobHandle& handle, BlobRecord* record,
                           OwnedSlice* buffer) {
  TEST_SYNC_POINT("BlobFileReader::Get");
  Slice blob;
  CacheAllocationPtr ubuf =
      AllocateBlock(handle.size, options_.memory_allocator());
  Status s = file_->Read(IOOptions(), handle.offset, handle.size, &blob,
                         ubuf.get(), nullptr /*aligned_buf*/);
  if (!s.ok()) {
    return s;
  }
  if (handle.size != static_cast<uint64_t>(blob.size())) {
    return Status::Corruption(
        "ReadRecord actual size: " + std::to_string(blob.size()) +
        " not equal to blob size " + std::to_string(handle.size));
  }

  BlobDecoder decoder(uncompression_dict_ == nullptr
                          ? &UncompressionDict::GetEmptyDict()
                          : uncompression_dict_.get());
  s = decoder.DecodeHeader(&blob);
  if (!s.ok()) {
    return s;
  }
  buffer->reset(std::move(ubuf), blob);
  s = decoder.DecodeRecord(&blob, record, buffer, options_.memory_allocator());
  return s;
}

Status BlobFilePrefetcher::Get(const ReadOptions& options,
                               const BlobHandle& handle, BlobRecord* record,
                               OwnedSlice* buffer) {
  if (handle.offset == last_offset_) {
    last_offset_ = handle.offset + handle.size;
    if (handle.offset + handle.size > readahead_limit_) {
      readahead_size_ = std::max(handle.size, readahead_size_);
      IOOptions io_options;
      io_options.rate_limiter_priority = Env::IOPriority::IO_HIGH;
      reader_->file_->Prefetch(io_options, handle.offset, readahead_size_);
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
    std::unique_ptr<UncompressionDict>* uncompression_dict,
    MemoryAllocator* memory_allocator) {
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

  CacheAllocationPtr ubuf =
      AllocateBlock(meta_index_handle.size(), memory_allocator);
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
  CacheAllocationPtr dict_buf =
      AllocateBlock(dict_block.size(), memory_allocator);
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

#pragma once

#include "file/writable_file_writer.h"
#include "options/db_options.h"
#include "rocksdb/cache.h"
#include "util/compression.h"

#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

// A slice pointed to an owned buffer.
class OwnedSlice : public Slice {
 public:
  void reset(CacheAllocationPtr _data, size_t _size) {
    data_ = _data.get();
    size_ = _size;
    buffer_ = std::move(_data);
  }

  void reset(CacheAllocationPtr buffer, const Slice& s) {
    data_ = s.data();
    size_ = s.size();
    buffer_ = std::move(buffer);
  }

  char* release() {
    data_ = nullptr;
    size_ = 0;
    return buffer_.release();
  }

  static void CleanupFunc(void* buffer, void*) {
    delete[] reinterpret_cast<char*>(buffer);
  }

 private:
  CacheAllocationPtr buffer_;
};

// A slice pointed to a fixed size buffer.
template <size_t T>
class FixedSlice : public Slice {
 public:
  FixedSlice() : Slice(buffer_, T) {}

  char* get() { return buffer_; }

 private:
  char buffer_[T];
};

// Compresses the input data according to the compression context.
// Returns a slice with the output data and sets "*type" to the output
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(const CompressionInfo& info, const Slice& input,
               std::string* output, CompressionType* type);

// Uncompresses the input data according to the uncompression type.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(const UncompressionInfo& info, const Slice& input,
                  OwnedSlice* output, MemoryAllocator* allocator = nullptr);

void UnrefCacheHandle(void* cache, void* handle);

template <class T>
void DeleteCacheValue(void* value, MemoryAllocator*) {
  delete reinterpret_cast<T*>(value);
}

const Cache::CacheItemHelper kBlobValueCacheItemHelper(
    CacheEntryRole::kBlobValue, &DeleteCacheValue<OwnedSlice>);

Status SyncTitanManifest(TitanStats* stats,
                         const ImmutableDBOptions* db_options,
                         WritableFileWriter* file);

}  // namespace titandb
}  // namespace rocksdb

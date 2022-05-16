#include "util.h"

#include "util/compression.h"
#include "util/stop_watch.h"

namespace rocksdb {
namespace titandb {

// See util/compression.h.
const uint32_t kCompressionFormat = 2;

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

Slice Compress(const CompressionInfo& info, const Slice& input,
               std::string* output, CompressionType* type) {
  *type = info.type();
  if (info.type() == kNoCompression) {
    return input;
  }
  if (!CompressData(input, info, kCompressionFormat, output)) {
    // Compression method is not supported, or not good compression
    // ratio, so just fall back to uncompressed form.
    *type = kNoCompression;
    return input;
  }
  return *output;
}

Status Uncompress(const UncompressionInfo& info, const Slice& input,
                  OwnedSlice* output) {
  assert(info.type() != kNoCompression);
  size_t usize = 0;
  CacheAllocationPtr ubuf = UncompressData(info, input.data(), input.size(),
                                           &usize, kCompressionFormat);
  if (!ubuf.get()) {
    return Status::Corruption("Corrupted compressed blob");
  }
  output->reset(std::move(ubuf), usize);
  return Status::OK();
}

void UnrefCacheHandle(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

Status SyncTitanManifest(TitanStats* stats,
                         const ImmutableDBOptions* db_options,
                         WritableFileWriter* file) {
  StopWatch sw(db_options->clock, statistics(stats),
               TITAN_MANIFEST_FILE_SYNC_MICROS);
  return file->Sync(db_options->use_fsync);
}

}  // namespace titandb
}  // namespace rocksdb

#include "blob_file_cache.h"

#include "file/filename.h"
#include "rocksdb/advanced_cache.h"

#include "util.h"

namespace rocksdb {
namespace titandb {

namespace {

const Cache::CacheItemHelper kBlobFileReaderCacheItemHelper(
    CacheEntryRole::kBlockBasedTableReader, &DeleteCacheValue<BlobFileReader>);

Slice EncodeFileNumber(const uint64_t* number) {
  return Slice(reinterpret_cast<const char*>(number), sizeof(*number));
}

}  // namespace

BlobFileCache::BlobFileCache(const TitanDBOptions& db_options,
                             const TitanCFOptions& cf_options,
                             std::shared_ptr<Cache> cache, TitanStats* stats)
    : env_(db_options.env),
      env_options_(db_options),
      db_options_(db_options),
      cf_options_(cf_options),
      cache_(cache),
      stats_(stats) {}

Status BlobFileCache::Get(const ReadOptions& options, uint64_t file_number,
                          const BlobHandle& handle, BlobRecord* record,
                          OwnedSlice* buffer) {
  Cache::Handle* cache_handle = nullptr;
  Status s = GetBlobFileReaderHandle(file_number, &cache_handle);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(cache_handle));
  s = reader->Get(options, handle, record, buffer);
  cache_->Release(cache_handle);
  return s;
}

Status BlobFileCache::NewPrefetcher(
    uint64_t file_number, std::unique_ptr<BlobFilePrefetcher>* result) {
  Cache::Handle* cache_handle = nullptr;
  Status s = GetBlobFileReaderHandle(file_number, &cache_handle);
  if (!s.ok()) return s;

  auto reader = reinterpret_cast<BlobFileReader*>(cache_->Value(cache_handle));
  auto prefetcher = new BlobFilePrefetcher(reader);
  prefetcher->RegisterCleanup(&UnrefCacheHandle, cache_.get(), cache_handle);
  result->reset(prefetcher);
  return s;
}

void BlobFileCache::Evict(uint64_t file_number) {
  cache_->Erase(EncodeFileNumber(&file_number));
}

Status BlobFileCache::GetBlobFileReaderHandle(uint64_t file_number,
                                              Cache::Handle** handle) {
  Status s;
  Slice cache_key = EncodeFileNumber(&file_number);
  *handle = cache_->Lookup(cache_key);
  if (*handle) {
    // TODO: add file reader cache hit/miss metrics
    return s;
  }
  std::unique_ptr<RandomAccessFileReader> file;
  uint64_t file_size;
  {
    std::unique_ptr<FSRandomAccessFile> f;
    auto file_name = BlobFileName(db_options_.dirname, file_number);
    auto fs = env_->GetFileSystem();

    s = fs->GetFileSize(file_name, IOOptions(), &file_size, nullptr);
    if (!s.ok()) return s;
    s = fs->NewRandomAccessFile(file_name, FileOptions(env_options_), &f,
                                nullptr /*dbg*/);
    if (!s.ok()) return s;
    if (db_options_.advise_random_on_open) {
      f->Hint(FSRandomAccessFile::kRandom);
    }
    file.reset(new RandomAccessFileReader(std::move(f), file_name));
  }

  std::unique_ptr<BlobFileReader> reader;
  s = BlobFileReader::Open(cf_options_, std::move(file), file_size, &reader,
                           stats_);
  if (!s.ok()) return s;

  cache_->Insert(cache_key, reader.release(), &kBlobFileReaderCacheItemHelper,
                 1, handle);
  return s;
}

}  // namespace titandb
}  // namespace rocksdb

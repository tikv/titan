#pragma once

#include <cstdint>
#include <queue>

#include "blob_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "titan/options.h"
#include "util.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace titandb {

// Used by GC job for iterate through blob file.
class BlobFileIterator {
 public:
  const uint64_t kMinReadaheadSize = 4 << 10;
  const uint64_t kMaxReadaheadSize = 256 << 10;

  BlobFileIterator(std::unique_ptr<RandomAccessFileReader>&& file,
                   uint64_t file_name, uint64_t file_size,
                   const TitanCFOptions& titan_cf_options);
  ~BlobFileIterator();

  bool Init();
  bool Valid() const;
  void SeekToFirst();
  void Next();
  Slice key() const;
  Slice value() const;
  Status status() const { return status_; }
  uint64_t header_size() const { return header_size_; }

  void IterateForPrev(uint64_t);

  BlobIndex GetBlobIndex() {
    BlobIndex blob_index;
    blob_index.file_number = file_number_;
    blob_index.blob_handle.offset = cur_record_offset_;
    blob_index.blob_handle.size = cur_record_size_;
    return blob_index;
  }

 private:
  // Blob file info
  const std::unique_ptr<RandomAccessFileReader> file_;
  const uint64_t file_number_;
  const uint64_t file_size_;
  TitanCFOptions titan_cf_options_;

  bool init_{false};
  uint64_t end_of_blob_record_{0};

  // Iterator status
  Status status_;
  bool valid_{false};

  BlobDecoder decoder_;
  uint64_t iterate_offset_{0};
  std::vector<char> buffer_;
  OwnedSlice uncompressed_;
  BlobRecord cur_blob_record_;
  uint64_t cur_record_offset_;
  uint64_t cur_record_size_;
  uint64_t header_size_;

  uint64_t readahead_begin_offset_{0};
  uint64_t readahead_end_offset_{0};
  uint64_t readahead_size_{kMinReadaheadSize};

  void PrefetchAndGet();
  void GetBlobRecord();
};

class BlobFileMergeIterator {
 public:
  explicit BlobFileMergeIterator(
      std::vector<std::unique_ptr<BlobFileIterator>>&&, const Comparator*);

  ~BlobFileMergeIterator() = default;

  bool Valid() const;
  void SeekToFirst();
  void Next();
  Slice key() const;
  Slice value() const;
  Status status() const {
    if (current_ != nullptr && !current_->status().ok())
      return current_->status();
    return status_;
  }

  BlobIndex GetBlobIndex() { return current_->GetBlobIndex(); }

 private:
  class BlobFileIterComparator {
   public:
    // The default constructor is not supposed to be used.
    // It is only to make std::priority_queue can compile.
    BlobFileIterComparator() : comparator_(nullptr){};
    explicit BlobFileIterComparator(const Comparator* comparator)
        : comparator_(comparator){};
    // Smaller value get Higher priority
    bool operator()(const BlobFileIterator* iter1,
                    const BlobFileIterator* iter2) {
      assert(comparator_ != nullptr);
      return comparator_->Compare(iter1->key(), iter2->key()) > 0;
    }

   private:
    const Comparator* comparator_;
  };

  Status status_;
  std::vector<std::unique_ptr<BlobFileIterator>> blob_file_iterators_;
  std::priority_queue<BlobFileIterator*, std::vector<BlobFileIterator*>,
                      BlobFileIterComparator>
      min_heap_;
  BlobFileIterator* current_ = nullptr;
};

}  // namespace titandb
}  // namespace rocksdb

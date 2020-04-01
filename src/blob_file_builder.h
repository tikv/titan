#pragma once

#include "blob_format.h"
#include "titan/options.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace titandb {

// Blob file format:
//
// <begin>
// [blob record 1]
// [blob record 2]
// ...
// [blob record N]
// [meta block 1]
// [meta block 2]
// ...
// [meta block K]
// [meta index block]
// [footer]
// <end>
//
// 1. The sequence of blob records in the file are stored in sorted
// order. These records come one after another at the beginning of the
// file, and are compressed according to the compression options.
//
// 2. After the blob records we store a bunch of meta blocks, and a
// meta index block with block handles pointed to the meta blocks. The
// meta block and the meta index block are formatted the same as the
// BlockBasedTable.

class BlobFileBuilder {
 public:
  // Constructs a builder that will store the contents of the file it
  // is building in "*file". Does not close the file. It is up to the
  // caller to sync and close the file after calling Finish().
  BlobFileBuilder(const TitanDBOptions& db_options,
                  const TitanCFOptions& cf_options, WritableFileWriter* file);

  // Adds the record to the file and points the handle to it.
  void Add(const BlobRecord& record, BlobHandle* handle);

  // Returns non-ok iff some error has been detected.
  Status status() const { return status_; }

  // Finishes building the table.
  // REQUIRES: Finish(), Abandon() have not been called.
  Status Finish();

  // Abandons building the table. If the caller is not going to call
  // Finish(), it must call Abandon() before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called.
  void Abandon();

  // Number of calls to Add() so far.
  uint64_t NumEntries();

  const std::string& GetSmallestKey() { return smallest_key_; }
  const std::string& GetLargestKey() { return largest_key_; }

  uint64_t live_data_size() const { return live_data_size_; }

 private:
  bool ok() const { return status().ok(); }

  TitanCFOptions cf_options_;
  WritableFileWriter* file_;

  Status status_;
  BlobEncoder encoder_;

  uint64_t num_entries_ = 0;
  std::string smallest_key_;
  std::string largest_key_;
  uint64_t live_data_size_ = 0;
};

}  // namespace titandb
}  // namespace rocksdb

#pragma once

#include "blob_format.h"
#include "table/meta_blocks.h"
#include "titan/options.h"
#include "util/compression.h"
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

  // Enter unbuffered state, only be called after collecting enough samples
  // for compression dictionary
  void EnterUnbuffered();

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
  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  enum class BuilderState {
    kBuffered,
    kUnbuffered,
  };
  BuilderState builder_state_;

  bool ok() const { return status().ok(); }
  void WriteRawBlock(const Slice& block, BlockHandle* handle);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder,
                                 BlockHandle* handle);
  void FlushSampleRecords();
  void WriteEncoderData(BlobHandle* handle);

  TitanCFOptions cf_options_;
  WritableFileWriter* file_;

  Status status_;
  BlobEncoder encoder_;

  // following 3 may be refactored in to Rep
  std::vector<std::string> sample_records_;
  uint64_t sample_str_len_ = 0;
  std::unique_ptr<CompressionDict> compression_dict_;

  uint64_t num_entries_ = 0;
  std::string smallest_key_;
  std::string largest_key_;
  uint64_t live_data_size_ = 0;
};

}  // namespace titandb
}  // namespace rocksdb

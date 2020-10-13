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
  class BlobRecordContext {
   public:
    std::string key;  // original internal key
    BlobIndex original_blob_index;
    BlobIndex new_blob_index;
    class SmallKVContetx {
     public:
      SmallKVContetx() : is_small(false) {}
      bool is_small;
      std::string value;
    } small_kv_ctx;
  };
  typedef autovector<std::unique_ptr<BlobRecordContext>> OutContexts;

  // Constructs a builder that will store the contents of the file it
  // is building in "*file". Does not close the file. It is up to the
  // caller to sync and close the file after calling Finish().
  BlobFileBuilder(const TitanDBOptions& db_options,
                  const TitanCFOptions& cf_options, WritableFileWriter* file);

  // SetFileWriter can only be called when current writer is null.
  void SetFileWriter(WritableFileWriter* file);

  // If file_ is not null, return true.
  bool HasFileWriter();

  // Tries to add the record to the file
  // Notice:
  // 1. When the record is a small KV pair, it will be write back to
  //    `out_ctx` immediately, otherwise, it will be write into the blob file.
  // 2. The `out_ctx` might be empty when builder is in `kBuffered` state.
  // 3. Caller should set `ctx.new_blob_index.file_number` before pass it in,
  //    the file builder will only change the `blob_handle` of it
  void Add(const BlobRecord& record, std::unique_ptr<BlobRecordContext> ctx,
           OutContexts* out_ctx);

  // Returns non-ok iff some error has been detected.
  Status status() const { return status_; }

  // Finishes building the table, and return status.
  // This method will return modify output contexts when it is called in
  // `kBuffered` state.
  // REQUIRES: Finish(), Abandon() have not been called.
  Status Finish(OutContexts* out_ctx);

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
  // Enter unbuffered state, only be called after collecting enough samples
  // for compression dictionary. It will modify `out_ctx` of the buffered
  // records
  void EnterUnbuffered(OutContexts* out_ctx);
  void WriteHeader();
  void WriteRawBlock(const Slice& block, BlockHandle* handle);
  void WriteCompressionDictBlock(MetaIndexBuilder* meta_index_builder,
                                 BlockHandle* handle);
  void FlushSampleRecords(OutContexts* out_ctx);
  void WriteEncoderData(BlobHandle* handle);

  TitanCFOptions cf_options_;
  WritableFileWriter* file_;

  Status status_;
  BlobEncoder encoder_;

  // following 3 may be refactored in to Rep
  std::vector<std::string> sample_records_;
  uint64_t sample_str_len_ = 0;
  std::unique_ptr<CompressionDict> compression_dict_;

  OutContexts cached_contexts_;

  uint64_t num_entries_ = 0;
  std::string smallest_key_;
  std::string largest_key_;
  uint64_t live_data_size_ = 0;
};

}  // namespace titandb
}  // namespace rocksdb

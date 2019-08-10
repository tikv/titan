#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <env/io_posix.h>

#include <utility>
#include "dig_hole_job.h"
#include "blob_file_reader.h"
#include "blob_file_iterator.h"
namespace rocksdb {
namespace titandb {
DigHoleJob::DigHoleJob(TitanDBOptions titan_db_options,
                       const EnvOptions &env_options,
                       Env *env,
                       TitanCFOptions titan_cf_options,
                       std::function<bool()> IsShutingDown,
                       std::function<Status(const Slice &, const BlobIndex &, bool *)> DiscardEntry) :
    db_options_(std::move(titan_db_options)),
    env_options_(env_options),
    env_(env),
    titan_cf_options_(std::move(titan_cf_options)) {
  this->IsShutingDown_ = std::move(IsShutingDown);
  this->DiscardEntry_ = std::move(DiscardEntry);
}
Status DigHoleJob::Exec(const std::vector<BlobFileMeta *> &inputs) {
  for (BlobFileMeta *input : inputs) {
    Status s = Exec(input);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
Status DigHoleJob::Exec(BlobFileMeta *input) {
  Status s;
  //open file
  std::unique_ptr<PosixRandomRWFile> file;// TODO(@DorianZheng) set read ahead size
  s = OpenBlobFile(input->file_number(), 0, db_options_, env_options_, env_, &file);
  if (!s.ok()) {
    return s;
  }
  auto record_iter = new BlobFileIterator(
      std::move(file), input->file_number(), input->file_size(),
      titan_cf_options_);
  //pre size
  uint64_t before_size = 0, after_size = 0;
  record_iter->GetFileRealSize(&before_size);
  //for each block-clusters
  for (record_iter->SeekToFirst(); record_iter->Valid();) {
    if (IsShutingDown_()) {
      s = Status::ShutdownInProgress();
      return s;
    }
    //read first record in block-cluster and get hole size
    BlobIndex blob_index = record_iter->GetBlobIndex();
    BlobHandle blob_handle = blob_index.blob_handle;
    uint64_t record_end = blob_handle.offset + blob_handle.size;
    const uint64_t hole_start = blob_handle.offset;
    const uint64_t hole_end = (record_end + block_size_ - 1) / block_size_ * block_size_;
    bool enableHole = true;

    while (record_end <= hole_end) {
      //judge
      if (enableHole) {
        bool discardable = false;
        s = DiscardEntry_(record_iter->key(), blob_index, &discardable);
        if (!s.ok()) {
          return s;
        }
        if (!discardable) {//There is valid record.
          enableHole = false;
        }
      }
      //read next record
      record_iter->Next();
      if (!record_iter->Valid()) {
        break;
      } else {
        blob_index = record_iter->GetBlobIndex();
        blob_handle = record_iter->GetBlobIndex().blob_handle;
        record_end = blob_handle.offset + blob_handle.size;
      }
    }

    if (enableHole && record_iter->status().ok()) {
      record_iter->PunchHole(hole_start, hole_end - hole_start);
    }
  }

  if (!record_iter->status().ok()) {
    return record_iter->status();
  }

  // TODO(@lhy1024) post
  record_iter->GetFileRealSize(&after_size);
  // input->finish(after_size, before_size-after_size);
  return s;
}
}
}
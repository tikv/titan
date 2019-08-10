#pragma once

#include <titan/options.h>
#include "blob_format.h"
#include "blob_gc_job.h"
namespace rocksdb {
namespace titandb {
class DigHoleJob {
 public:
  DigHoleJob() = delete;
  DigHoleJob(const DigHoleJob &) = delete;
  void operator=(const DigHoleJob &) = delete;
  DigHoleJob(TitanDBOptions titan_db_options,
             const EnvOptions &env_options,
             Env *env_,
             TitanCFOptions titan_cf_options,
             std::function<bool()> IsShutingDown,
             std::function<Status(const Slice &, const BlobIndex &, bool *)> DiscardEntry);
  friend class DigHoleTest;
  Status Exec(const std::vector<BlobFileMeta *> &inputs);
 private:
  std::function<bool()> IsShutingDown_;
  std::function<Status(const Slice &, const BlobIndex &, bool *)> DiscardEntry_;
  Status Exec(BlobFileMeta *input);
  TitanDBOptions db_options_;
  EnvOptions env_options_;
  Env *env_;
  TitanCFOptions titan_cf_options_;
  const uint64_t block_size_ = 4096;
};
}
}
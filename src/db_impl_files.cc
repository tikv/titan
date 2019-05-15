#include "db_impl.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::PurgeObsoleteFiles() {
  Status s;
  std::vector<std::string> candidate_files;
  auto oldest_sequence = GetOldestSnapshotSequence();
  {
    MutexLock l(&mutex_);
    vset_->GetObsoleteFiles(&candidate_files, oldest_sequence);
  }

  // dedup state.inputs so we don't try to delete the same
  // file twice
  std::sort(candidate_files.begin(), candidate_files.end());
  candidate_files.erase(
      std::unique(candidate_files.begin(), candidate_files.end()),
      candidate_files.end());

  for (const auto& candidate_file : candidate_files) {
    ROCKS_LOG_INFO(db_options_.info_log, "Titan deleting obsolete file [%s]",
                   candidate_file.c_str());
    s = env_->DeleteFile(candidate_file);
    if (!s.ok()) {
      fprintf(stderr, "Titan deleting file [%s] failed, status:%s",
              candidate_file.c_str(), s.ToString().c_str());
      abort();
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb

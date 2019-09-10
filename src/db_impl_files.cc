#include "db_impl.h"

namespace rocksdb {
namespace titandb {

Status TitanDBImpl::PurgeObsoleteFilesImpl() {
  Status s;
  std::vector<std::string> candidate_files;
  auto oldest_sequence = GetOldestSnapshotSequence();
  {
    MutexLock l(&mutex_);
    blob_file_set_->GetObsoleteFiles(&candidate_files, oldest_sequence);
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
    Status delete_status = env_->DeleteFile(candidate_file);
    if (!s.ok()) {
      // Move on despite error deleting the file.
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Titan deleting file [%s] failed, status:%s",
                      candidate_file.c_str(), s.ToString().c_str());
      s = delete_status;
    }
  }
  return s;
}

void TitanDBImpl::PurgeObsoleteFiles() {
  Status s __attribute__((__unused__)) = PurgeObsoleteFilesImpl();
  assert(s.ok());
}

Status TitanDBImpl::TEST_PurgeObsoleteFiles() {
  return PurgeObsoleteFilesImpl();
}

}  // namespace titandb
}  // namespace rocksdb

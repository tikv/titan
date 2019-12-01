#include "base_db_listener.h"

#include "db_impl.h"

namespace rocksdb {
namespace titandb {

BaseDbListener::BaseDbListener(TitanDBImpl* db) : db_impl_(db) {
  assert(db_impl_ != nullptr);
}

BaseDbListener::~BaseDbListener() {}

void BaseDbListener::OnFlushCompleted(DB* /*db*/,
                                      const FlushJobInfo& flush_job_info) {
  if (db_impl_->initialized()) {
    db_impl_->OnFlushCompleted(flush_job_info);
  }
}

void BaseDbListener::OnCompactionCompleted(
    DB* /* db */, const CompactionJobInfo& compaction_job_info) {
  if (db_impl_->initialized()) {
    db_impl_->OnCompactionCompleted(compaction_job_info);
  }
}

}  // namespace titandb
}  // namespace rocksdb

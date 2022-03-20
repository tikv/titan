#pragma once

#include "rocksdb/listener.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl;

class BaseDbListener final : public EventListener {
 public:
  BaseDbListener(TitanDBImpl* db);
  ~BaseDbListener();

  void OnFlushCompleted(DB* db, const FlushJobInfo& flush_job_info) override;

  void OnCompactionCompleted(
      DB* db, const CompactionJobInfo& compaction_job_info) override;

 private:
  rocksdb::titandb::TitanDBImpl* db_impl_;
};

}  // namespace titandb
}  // namespace rocksdb

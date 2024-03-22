#include "blob_gc.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<std::shared_ptr<BlobFileMeta>>&& blob_files,
               TitanCFOptions&& _titan_cf_options, bool need_trigger_next,
               uint64_t cf_id, bool punch_hole)
    : inputs_(blob_files),
      titan_cf_options_(std::move(_titan_cf_options)),
      trigger_next_(need_trigger_next),
      cf_id_(cf_id),
      use_punch_hole_(punch_hole) {
  MarkFilesBeingGC();
}

BlobGC::~BlobGC() {
  // Release snapshot requires db pointer, so we can't release it internally.
  // In case the caller forgets to release the snapshot, we assert here, prefer
  // to crash in the runtime than leak.
  assert(snapshot_ == nullptr);
}

void BlobGC::SetColumnFamily(ColumnFamilyHandle* cfh) { cfh_ = cfh; }

ColumnFamilyData* BlobGC::GetColumnFamilyData() {
  auto* cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh_);
  return cfhi->cfd();
}

void BlobGC::AddOutputFile(BlobFileMeta* blob_file) {
  outputs_.push_back(blob_file);
}

void BlobGC::MarkFilesBeingGC() {
  for (auto& f : inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
  }
}

void BlobGC::ReleaseGcFiles() {
  for (auto& f : inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }

  for (auto& f : outputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }
}

void BlobGC::ReleaseSnapshot(DB* db) {
  if (snapshot_ != nullptr) {
    db->ReleaseSnapshot(snapshot_);
    snapshot_ = nullptr;
  }
}

}  // namespace titandb
}  // namespace rocksdb

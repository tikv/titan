#include "blob_gc.h"
#include "unordered_set"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<BlobFileMeta*>&& gc_blob_files,
               std::vector<BlobFileMeta*>&& fs_blob_files,
               TitanCFOptions&& _titan_cf_options, bool need_trigger_next)
    : gc_inputs_(std::move(gc_blob_files)),
      fs_inputs_(std::move(fs_blob_files)),
      titan_cf_options_(std::move(_titan_cf_options)),
      trigger_next_(need_trigger_next) {
  MarkFilesBeingGC();
}

BlobGC::~BlobGC() {}

void BlobGC::SetColumnFamily(ColumnFamilyHandle* cfh) { cfh_ = cfh; }

ColumnFamilyData* BlobGC::GetColumnFamilyData() {
  auto* cfhi = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh_);
  return cfhi->cfd();
}

void BlobGC::AddOutputFile(BlobFileMeta* blob_file) {
  blob_file->FileStateTransit(BlobFileMeta::FileEvent::kGCOutput);
  outputs_.push_back(blob_file);
}

void BlobGC::MarkFilesBeingGC() {
  std::unordered_set<uint64_t> gc_input_marks;
  for (auto& f : gc_inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
    gc_input_marks.insert(f->file_number());
  }
  for (auto& f : fs_inputs_) {
    if (gc_input_marks.find(f->file_number()) != gc_input_marks.end()) {
      continue;
    }
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCBegin);
  }
}

void BlobGC::ReleaseGcFiles() {
  std::unordered_set<uint64_t> gc_input_marks;
  for (auto& f : gc_inputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
    gc_input_marks.insert(f->file_number());
  }
  for (auto& f : fs_inputs_) {
    if (gc_input_marks.find(f->file_number()) != gc_input_marks.end()) {
      continue;
    }
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }

  for (auto& f : outputs_) {
    f->FileStateTransit(BlobFileMeta::FileEvent::kGCCompleted);
  }
}

}  // namespace titandb
}  // namespace rocksdb

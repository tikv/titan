#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <memory>
#include <unordered_map>

#include "blob_file_reader.h"
#include "blob_format.h"
#include "db/db_iter.h"
#include "rocksdb/env.h"
#include "titan_logging.h"
#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
   TitanDBIterator(const TitanReadOptions &options, BlobStorage *storage,
                   std::shared_ptr<ManagedSnapshot> snap,
                   std::unique_ptr<ArenaWrappedDBIter> iter, SystemClock *clock,
                   TitanStats *stats, Logger *info_log);

   ~TitanDBIterator();

   bool Valid() const override;

   Status status() const override;
   void SeekToFirst() override;

   void SeekToLast() override;

   void Seek(const Slice &target) override;

   void SeekForPrev(const Slice &target) override;

   void Next() override;

   void Prev() override;

   Slice key() const override;

   Slice value() const override;

   bool seqno(SequenceNumber *number) const override;

 private:
   bool ShouldGetBlobValue();

   void GetBlobValue(bool forward);

   void GetBlobValueImpl(const BlobIndex &index);

   Status status_;
   BlobRecord record_;
   PinnableSlice buffer_;

   TitanReadOptions options_;
   BlobStorage *storage_;
   std::shared_ptr<ManagedSnapshot> snap_;
   std::unique_ptr<ArenaWrappedDBIter> iter_;
   std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>> files_;

   SystemClock *clock_;
   TitanStats *stats_;
   Logger *info_log_;
};

}  // namespace titandb
}  // namespace rocksdb

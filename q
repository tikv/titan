[1mdiff --git a/src/compaction_filter.h b/src/compaction_filter.h[m
[1mindex e3d6947..f5bc00a 100644[m
[1m--- a/src/compaction_filter.h[m
[1m+++ b/src/compaction_filter.h[m
[36m@@ -3,11 +3,10 @@[m
 #include <string>[m
 #include <utility>[m
 [m
[31m-#include "rocksdb/compaction_filter.h"[m
[31m-#include "util/mutexlock.h"[m
[31m-[m
 #include "db_impl.h"[m
[32m+[m[32m#include "rocksdb/compaction_filter.h"[m
 #include "titan_logging.h"[m
[32m+[m[32m#include "util/mutexlock.h"[m
 [m
 namespace rocksdb {[m
 namespace titandb {[m
[36m@@ -37,8 +36,8 @@[m [mclass TitanCompactionFilter final : public CompactionFilter {[m
 [m
   Decision FilterV3(int level, const Slice &key, SequenceNumber seqno,[m
                     ValueType value_type, const Slice &value,[m
[31m-                    std::string *new_value, std::string *skip_until) const[m
[31m-      override {[m
[32m+[m[32m                    std::string *new_value,[m
[32m+[m[32m                    std::string *skip_until) const override {[m
     Status s;[m
     Slice user_key = key;[m
 [m
[36m@@ -163,9 +162,9 @@[m [mclass TitanCompactionFilterFactory final : public CompactionFilterFactory {[m
     std::shared_ptr<BlobStorage> blob_storage;[m
     {[m
       MutexLock l(&titan_db_impl_->mutex_);[m
[31m-      blob_storage =[m
[31m-          titan_db_impl_->blob_file_set_->GetBlobStorage([m
[31m-                                              context.column_family_id).lock();[m
[32m+[m[32m      blob_storage = titan_db_impl_->blob_file_set_[m
[32m+[m[32m                         ->GetBlobStorage(context.column_family_id)[m
[32m+[m[32m                         .lock();[m
     }[m
     if (blob_storage == nullptr) {[m
       assert(false);[m

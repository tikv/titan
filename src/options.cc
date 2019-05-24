#include "titan/options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "rocksdb/convenience.h"

namespace rocksdb {
namespace titandb {

TitanCFOptions::TitanCFOptions(const ImmutableTitanCFOptions& immutable_opts,
                               const MutableTitanCFOptions& mutable_opts)
    : min_blob_size(immutable_opts.min_blob_size),
      blob_file_compression(immutable_opts.blob_file_compression),
      blob_file_target_size(immutable_opts.blob_file_target_size),
      blob_cache(immutable_opts.blob_cache),
      max_gc_batch_size(immutable_opts.max_gc_batch_size),
      min_gc_batch_size(immutable_opts.min_gc_batch_size),
      blob_file_discardable_ratio(immutable_opts.blob_file_discardable_ratio),
      sample_file_size_ratio(immutable_opts.sample_file_size_ratio),
      merge_small_file_threshold(immutable_opts.merge_small_file_threshold),
      blob_run_mode(mutable_opts.blob_run_mode) {}

std::string TitanCFOptions::ToString() const {
  char buf[256];
  std::string str;
  std::string res = "[titandb]\n";
  snprintf(buf, sizeof(buf), "min_blob_size = %" PRIu64 "\n", min_blob_size);
  res += buf;
  GetStringFromCompressionType(&str, blob_file_compression);
  snprintf(buf, sizeof(buf), "blob_file_compression = %s\n", str.c_str());
  res += buf;
  snprintf(buf, sizeof(buf), "blob_file_target_size = %" PRIu64 "\n",
           blob_file_target_size);
  res += buf;
  return res;
}

std::unordered_map<std::string, TitanBlobRunMode>
    TitanOptionsHelper::blob_run_mode_string_map = {
        {"normal", TitanBlobRunMode::kNormal},
        {"read-only", TitanBlobRunMode::kReadOnly},
        {"fallback", TitanBlobRunMode::kFallback}};

}  // namespace titandb
}  // namespace rocksdb

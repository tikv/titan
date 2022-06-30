#include "titan/options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <cinttypes>

#include "options/options_helper.h"
#include "rocksdb/convenience.h"

#include "titan_logging.h"

namespace rocksdb {
namespace titandb {

void TitanDBOptions::Dump(Logger* logger) const {
  TITAN_LOG_HEADER(logger, "TitanDBOptions.dirname                    : %s",
                   dirname.c_str());
  TITAN_LOG_HEADER(logger, "TitanDBOptions.disable_background_gc      : %d",
                   static_cast<int>(disable_background_gc));
  TITAN_LOG_HEADER(logger,
                   "TitanDBOptions.max_background_gc          : %" PRIi32,
                   max_background_gc);
  TITAN_LOG_HEADER(logger,
                   "TitanDBOptions.purge_obsolete_files_period_sec: %" PRIu32,
                   purge_obsolete_files_period_sec);
  TITAN_LOG_HEADER(logger,
                   "TitanDBOptions.titan_stats_dump_period_sec: %" PRIu32,
                   titan_stats_dump_period_sec);
}

TitanCFOptions::TitanCFOptions(const ColumnFamilyOptions& cf_opts,
                               const ImmutableTitanCFOptions& immutable_opts,
                               const MutableTitanCFOptions& mutable_opts)
    : ColumnFamilyOptions(cf_opts),
      min_blob_size(immutable_opts.min_blob_size),
      blob_file_compression(immutable_opts.blob_file_compression),
      blob_file_target_size(immutable_opts.blob_file_target_size),
      blob_cache(immutable_opts.blob_cache),
      max_gc_batch_size(immutable_opts.max_gc_batch_size),
      min_gc_batch_size(immutable_opts.min_gc_batch_size),
      blob_file_discardable_ratio(immutable_opts.blob_file_discardable_ratio),
      merge_small_file_threshold(immutable_opts.merge_small_file_threshold),
      blob_run_mode(mutable_opts.blob_run_mode),
      skip_value_in_compaction_filter(
          immutable_opts.skip_value_in_compaction_filter) {}

void TitanCFOptions::Dump(Logger* logger) const {
  TITAN_LOG_HEADER(logger,
                   "TitanCFOptions.min_blob_size                : %" PRIu64,
                   min_blob_size);
  std::string compression_str = "unknown";
  for (auto& compression_type : compression_type_string_map) {
    if (compression_type.second == blob_file_compression) {
      compression_str = compression_type.first;
      break;
    }
  }
  TITAN_LOG_HEADER(logger, "TitanCFOptions.blob_file_compression        : %s",
                   compression_str.c_str());
  TITAN_LOG_HEADER(logger, "TItanCFOptions.blob_file_compression_options: ");
  TITAN_LOG_HEADER(logger, "    window_bits : %d",
                   blob_file_compression_options.window_bits);
  TITAN_LOG_HEADER(logger, "    level : %d",
                   blob_file_compression_options.level);
  TITAN_LOG_HEADER(logger, "    strategy : %d",
                   blob_file_compression_options.strategy);
  TITAN_LOG_HEADER(logger, "    max_dict_bytes : %" PRIu32,
                   blob_file_compression_options.max_dict_bytes);
  TITAN_LOG_HEADER(logger, "    zstd_max_train_bytes : %" PRIu32,
                   blob_file_compression_options.zstd_max_train_bytes);
  TITAN_LOG_HEADER(logger,
                   "TitanCFOptions.blob_file_target_size        : %" PRIu64,
                   blob_file_target_size);
  TITAN_LOG_HEADER(logger, "TitanCFOptions.blob_cache                   : %p",
                   blob_cache.get());
  if (blob_cache != nullptr) {
    TITAN_LOG_HEADER(logger, "%s", blob_cache->GetPrintableOptions().c_str());
  }
  TITAN_LOG_HEADER(logger,
                   "TitanCFOptions.max_gc_batch_size            : %" PRIu64,
                   max_gc_batch_size);
  TITAN_LOG_HEADER(logger,
                   "TitanCFOptions.min_gc_batch_size            : %" PRIu64,
                   min_gc_batch_size);
  TITAN_LOG_HEADER(logger, "TitanCFOptions.blob_file_discardable_ratio  : %lf",
                   blob_file_discardable_ratio);
  TITAN_LOG_HEADER(logger,
                   "TitanCFOptions.merge_small_file_threshold   : %" PRIu64,
                   merge_small_file_threshold);
  std::string blob_run_mode_str = "unknown";
  if (blob_run_mode_to_string.count(blob_run_mode) > 0) {
    blob_run_mode_str = blob_run_mode_to_string.at(blob_run_mode);
  }
  TITAN_LOG_HEADER(logger, "TitanCFOptions.blob_run_mode                : %s",
                   blob_run_mode_str.c_str());
}

std::map<TitanBlobRunMode, std::string>
    TitanOptionsHelper::blob_run_mode_to_string = {
        {TitanBlobRunMode::kNormal, "kNormal"},
        {TitanBlobRunMode::kReadOnly, "kReadOnly"},
        {TitanBlobRunMode::kFallback, "kFallback"}};

std::unordered_map<std::string, TitanBlobRunMode>
    TitanOptionsHelper::blob_run_mode_string_map = {
        {"kNormal", TitanBlobRunMode::kNormal},
        {"kReadOnly", TitanBlobRunMode::kReadOnly},
        {"kFallback", TitanBlobRunMode::kFallback}};

}  // namespace titandb
}  // namespace rocksdb

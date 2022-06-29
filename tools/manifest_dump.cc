// Copyright 2021-present TiKV Project Authors. Licensed under Apache-2.0.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run Titan tools.\n");
  return 1;
}
#else

#include <memory>

#include "file/sequence_file_reader.h"
#include "rocksdb/env.h"
#include "util/gflags_compat.h"

#include "edit_collector.h"
#include "version_edit.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(path, "", "Path for Titan manifest file.");
DEFINE_bool(ignore_tail_err, true,
            "Ignore error encounter towards the tail of manifest.");
DEFINE_bool(verbose, false, "Output each manifest record.");
DEFINE_bool(with_keys, false, "Output blob file boundary keys");

#define handle_error(s, location)                                           \
  if (!s.ok()) {                                                            \
    fprintf(stderr, "error when %s: %s\n", location, s.ToString().c_str()); \
    return 1;                                                               \
  }

namespace rocksdb {
namespace titandb {

int manifest_dump() {
  if (FLAGS_path.empty()) {
    fprintf(stderr, "Manifest file path not given.\n");
    return 1;
  }
  Env* env = Env::Default();
  Status s;

  // Open manifest file.
  std::unique_ptr<SequentialFileReader> file_reader;
  std::unique_ptr<FSSequentialFile> file;
  s = env->GetFileSystem()->NewSequentialFile(FLAGS_path, FileOptions(), &file,
                                              nullptr /*dbg*/);
  handle_error(s, "open manifest file");
  file_reader.reset(new SequentialFileReader(std::move(file), FLAGS_path));

  // Open log reader.
  LogReporter reporter;
  reporter.status = &s;
  log::Reader log_reader(nullptr, std::move(file_reader), &reporter,
                         true /*checksum*/, 0 /*log_num*/);
  Slice record;
  std::string scratch;

  // Loop through log records.
  EditCollector edit_collector;
  while (log_reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = DecodeInto(record, &edit);
    handle_error(s, "parse version edit");
    if (FLAGS_verbose) {
      edit.Dump(FLAGS_with_keys);
      fprintf(stdout, "\n");
    }
    s = edit_collector.AddEdit(edit);
    handle_error(s, "colllect version edit");
  }
  if (!FLAGS_ignore_tail_err) {
    handle_error(s, "parse manifest record");
  }
  edit_collector.Dump(FLAGS_with_keys);
  return 0;
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);
  return rocksdb::titandb::manifest_dump();
}

#endif  // GFLAGS

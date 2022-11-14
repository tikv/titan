// Copyright 2021-present TiKV Project Authors. Licensed under Apache-2.0.

#include "file/filename.h"
#include "util/gflags_compat.h"

#include "blob_file_iterator.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(path, "", "Path of blob file.");
DEFINE_bool(dump, false, "");

#define handle_error(s, location)                                           \
  if (!s.ok()) {                                                            \
    fprintf(stderr, "error when %s: %s\n", location, s.ToString().c_str()); \
    return 1;                                                               \
  }

namespace rocksdb {
namespace titandb {

int blob_file_dump() {
  Env* env = Env::Default();
  Status s;

  std::string file_name = FLAGS_path;
  uint64_t file_size = 0;
  s = env->GetFileSize(file_name, &file_size);
  handle_error(s, "getting file size");

  std::unique_ptr<RandomAccessFileReader> file;
  std::unique_ptr<FSRandomAccessFile> f;
  s = env->GetFileSystem()->NewRandomAccessFile(file_name, FileOptions(), &f,
                                                nullptr /*dbg*/);
  handle_error(s, "open file");
  file.reset(new RandomAccessFileReader(std::move(f), file_name));

  std::unique_ptr<BlobFileIterator> iter(new BlobFileIterator(
      std::move(file), 1 /*fake file number*/, file_size, TitanCFOptions()));

  iter->SeekToFirst();
  while (iter->Valid()) {
    handle_error(iter->status(), "status");
    if (FLAGS_dump) {
      std::string key = iter->key().ToString(true);
      std::string value = iter->value().ToString(true);
      fprintf(stdout, "%s: %s\n", key.c_str(), value.c_str());
    }
    iter->Next();
  }
  handle_error(iter->status(), "reading blob file");
  return 0;
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);
  return rocksdb::titandb::blob_file_dump();
}

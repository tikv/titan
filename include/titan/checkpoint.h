#pragma once

#include "db.h"

namespace rocksdb {
namespace titandb {

class Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  static Status Create(TitanDB* db, Checkpoint** checkpoint_ptr);

  // Builds an openable snapshot of TitanDB.
  // base_checkpoint_dir: checkpoint directory of base DB
  // titan_checkpoint_dir: checkpoint directory of TitanDB, if not specified,
  // default value is {base_checkpoint_dir}/titandb.
  // The specified directory should contain absolute path and not exist, it
  // will be created by the API.
  // When a checkpoint is created:
  // (1) SST and blob files are hard linked if the output directory is on the
  //     same filesystem as the database, and copied otherwise.
  // (2) MANIFEST file specific to TitanDB will be regenerated based on all
  //     existing blob files.
  // (3) other required files are always copied.
  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families. The
  // default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
  // Flush will always trigger if it is 2PC.
  virtual Status CreateCheckpoint(const std::string& base_checkpoint_dir,
                                  const std::string& titan_checkpoint_dir = "",
                                  uint64_t log_size_for_flush = 0);

  virtual ~Checkpoint() {}
};

}  // namespace titandb
}  // namespace rocksdb

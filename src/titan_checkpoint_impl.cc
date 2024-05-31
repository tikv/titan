#include "titan_checkpoint_impl.h"

#include <cinttypes>

#include "db/log_writer.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "port/port.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/transaction_log.h"
#include "test_util/sync_point.h"
#include "utilities/checkpoint/checkpoint_impl.h"

#include "titan_logging.h"
#include "util.h"
#include "version_edit.h"

namespace rocksdb {
namespace titandb {

Status Checkpoint::Create(TitanDB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new TitanCheckpointImpl(db);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(
    const std::string& /*base_checkpoint_dir*/,
    const std::string& /*titan_checkpoint_dir = ""*/,
    uint64_t /*log_size_for_flush = 0*/) {
  return Status::NotSupported("TitanDB doesn't support this operation");
}

void TitanCheckpointImpl::CleanStagingDirectory(
    const std::string& full_private_path, Logger* info_log) {
  std::vector<std::string> subchildren;
  Status s = db_->GetEnv()->FileExists(full_private_path);
  if (s.IsNotFound()) {
    return;
  }
  TITAN_LOG_INFO(info_log, "File exists %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());
  s = db_->GetEnv()->GetChildren(full_private_path, &subchildren);
  if (s.ok()) {
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      if (subchild == "titandb") {
        CleanStagingDirectory(subchild_path, info_log);
        TITAN_LOG_INFO(info_log, "Clean titandb directory %s",
                       subchild_path.c_str());
      } else {
        s = db_->GetEnv()->DeleteFile(subchild_path);
        TITAN_LOG_INFO(info_log, "Delete file %s -- %s", subchild_path.c_str(),
                       s.ToString().c_str());
      }
    }
  }
  // Finally delete the private dir
  s = db_->GetEnv()->DeleteDir(full_private_path);
  TITAN_LOG_INFO(info_log, "Delete dir %s -- %s", full_private_path.c_str(),
                 s.ToString().c_str());
}

Status TitanCheckpointImpl::CreateTitanManifest(
    const std::string& file_name, std::vector<VersionEdit>* edits) {
  Status s;
  FileSystem* fs = db_->GetFileSystem();
  bool use_fsync = db_->GetDBOptions().use_fsync;
  const FileOptions file_options;
  std::unique_ptr<WritableFileWriter> file;

  {
    std::unique_ptr<FSWritableFile> f;
    s = fs->NewWritableFile(file_name, file_options, &f, nullptr /*dbg*/);
    if (!s.ok()) return s;
    file.reset(new WritableFileWriter(std::move(f), file_name, file_options));
  }
  std::unique_ptr<log::Writer> manifest;
  manifest.reset(new log::Writer(std::move(file), 0, false));

  for (auto& edit : *edits) {
    std::string record;
    edit.EncodeTo(&record);
    s = manifest->AddRecord(record);
    if (!s.ok()) return s;
  }

  return manifest->file()->Sync(use_fsync);
}

// Builds an openable checkpoint of TitanDB
Status TitanCheckpointImpl::CreateCheckpoint(
    const std::string& base_checkpoint_dir,
    const std::string& titan_checkpoint_dir, uint64_t log_size_for_flush) {
  TitanDBOptions titandb_options = db_->GetTitanDBOptions();
  std::string full_private_path;
  std::string checkpoint_dir = titan_checkpoint_dir;
  if (checkpoint_dir.empty()) {
    checkpoint_dir = base_checkpoint_dir + "/titandb";
  }
  // Check TitanDB checkpoint directory
  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("TitanDB checkpoint directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  TITAN_LOG_INFO(titandb_options.info_log,
                 "Started the TitanDB checkpoint process -- creating checkpoint"
                 "in directory %s",
                 checkpoint_dir.c_str());

  size_t final_nonslash_idx = checkpoint_dir.find_last_not_of('/');
  if (final_nonslash_idx == std::string::npos) {
    // npos means it's only slashes or empty. Non-empty means it's the root
    // directory, but it shouldn't be because we verified above the directory
    // doesn't exist.
    assert(checkpoint_dir.empty());
    return Status::InvalidArgument("Invalid TitanDB checkpoint directory name");
  }

  // Disable file deletions
  s = db_->DisableFileDeletions();
  const bool disabled_file_deletions = s.ok();
  if (s.ok()) {
    // Create base DB checkpoint
    auto base_db_checkpoint = new rocksdb::CheckpointImpl(db_);
    s = base_db_checkpoint->CreateCheckpoint(base_checkpoint_dir,
                                             log_size_for_flush,
                                             nullptr /*sequence_number_ptr*/);
    delete base_db_checkpoint;
    base_db_checkpoint = nullptr;

    if (s.ok()) {
      full_private_path =
          checkpoint_dir.substr(0, final_nonslash_idx + 1) + ".tmp";
      TITAN_LOG_INFO(
          titandb_options.info_log,
          "TitanDB checkpoint process -- using temporary directory %s",
          full_private_path.c_str());
      CleanStagingDirectory(full_private_path, titandb_options.info_log.get());
      s = db_->GetEnv()->CreateDir(full_private_path);
    }
  }

  if (s.ok()) {
    // Create TitanDB checkpoint
    s = CreateCustomCheckpoint(
        titandb_options,
        [&](const std::string& src_dirname, const std::string& fname,
            FileType) {
          TITAN_LOG_INFO(titandb_options.info_log, "Hard Linking %s",
                         fname.c_str());
          return db_->GetEnv()->LinkFile(src_dirname + fname,
                                         full_private_path + fname);
        } /* link_file_cb */,
        [&](const std::string& src_dirname, const std::string& fname,
            uint64_t size_limit_bytes, FileType) {
          TITAN_LOG_INFO(titandb_options.info_log, "Copying %s", fname.c_str());
          return CopyFile(db_->GetFileSystem(), src_dirname + fname,
                          full_private_path + fname, size_limit_bytes,
                          titandb_options.use_fsync, nullptr,
                          Temperature::kUnknown);
        } /* copy_file_cb */,
        [&](const std::string& fname, const std::string& contents, FileType) {
          TITAN_LOG_INFO(titandb_options.info_log, "Creating %s",
                         fname.c_str());
          return CreateFile(db_->GetFileSystem(), full_private_path + fname,
                            contents, titandb_options.use_fsync);
        } /* create_file_cb */,
        log_size_for_flush, full_private_path);
  }

  if (disabled_file_deletions) {
    // We copied all the files, enable file deletions
    Status ss = db_->EnableFileDeletions(false);
    assert(ss.ok());
  }

  if (s.ok()) {
    // Move tmp private backup to real TitanDB checkpoint directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    std::unique_ptr<Directory> checkpoint_directory;
    s = db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (s.ok() && checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (s.ok()) {
    // Here we know that we succeeded and installed the new checkpoint
    TITAN_LOG_INFO(titandb_options.info_log,
                   "TitanDB checkpoint DONE. All is good");
  } else {
    // Clean all the files we might have created
    TITAN_LOG_INFO(titandb_options.info_log, "TitanDB checkpoint failed -- %s",
                   s.ToString().c_str());
    CleanStagingDirectory(full_private_path, titandb_options.info_log.get());
  }
  return s;
}

Status TitanCheckpointImpl::CreateCustomCheckpoint(
    const TitanDBOptions& titandb_options,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname, FileType type)>
        link_file_cb,
    std::function<Status(const std::string& src_dirname,
                         const std::string& src_fname,
                         uint64_t size_limit_bytes, FileType type)>
        copy_file_cb,
    std::function<Status(const std::string& fname, const std::string& contents,
                         FileType type)>
        create_file_cb,
    uint64_t log_size_for_flush, const std::string full_private_path) {
  Status s;
  std::vector<std::string> titandb_files;
  std::vector<VersionEdit> version_edits;
  bool same_fs = true;

  // This will return files prefixed with "/"
  s = db_->GetAllTitanFiles(titandb_files, &version_edits);

  TEST_SYNC_POINT(
      "TitanCheckpointImpl::CreateCustomCheckpoint::AfterGetAllTitanFiles");
  TEST_SYNC_POINT(
      "TitanCheckpointImpl::CreateCustomCheckpoint:BeforeTitanDBCheckpoint1");
  TEST_SYNC_POINT(
      "TitanCheckpointImpl::CreateCustomCheckpoint::BeforeTitanDBCheckpoint2");

  if (!s.ok()) {
    return s;
  }

  // Copy/Hard link files
  std::string manifest_fname, current_fname;
  for (auto& live_file : titandb_files) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_file, &number, &type);

    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }

    // We should only get blob, manifest and current files here
    assert(type == kBlobFile || type == kDescriptorFile ||
           type == kCurrentFile);
    assert(live_file.size() > 0 && live_file[0] == '/');
    if (type == kCurrentFile) {
      current_fname = live_file;
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_file;
    }
    std::string src_fname = live_file;

    // Rules:
    // * If it's kBlobFile, then it's shared
    // * If it's kDescriptorFile, craft the manifest based on all blob file
    // * If it's kCurrentFile, craft the current file manually to ensure
    //   it's consistent with the manifest number. This is necessary because
    //   current file contents can change during checkpoint creation.
    // * Always copy if cross-device link.
    if (type == kBlobFile && same_fs) {
      s = link_file_cb(titandb_options.dirname, src_fname, type);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if (type != kBlobFile || !same_fs) {
      if (type == kDescriptorFile) {
        s = CreateTitanManifest(full_private_path + src_fname, &version_edits);
      } else {
        s = copy_file_cb(titandb_options.dirname, src_fname, 0, type);
      }
    }
  }
  // Write manifest name to CURRENT file
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
    create_file_cb(current_fname, manifest_fname + "\n", kCurrentFile);
  }

  return s;
}

}  // namespace titandb
}  // namespace rocksdb

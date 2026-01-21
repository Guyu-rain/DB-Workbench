#include "path_utils.h"

#include <cstdlib>

namespace dbms_paths {

std::filesystem::path DataDirPath() {
  if (const char* env = std::getenv("DBMS_DATA_DIR"); env && *env) {
    return std::filesystem::path(env);
  }
  return std::filesystem::current_path() / "data";
}

bool EnsureDataDir(std::string& err) {
  try {
    auto dir = DataDirPath();
    if (std::filesystem::exists(dir)) return true;
    if (!std::filesystem::create_directories(dir)) {
      err = "Failed to create data directory: " + dir.string();
      return false;
    }
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    err = std::string("Filesystem error: ") + e.what();
    return false;
  }
}

bool EnsureDbDir(const std::string& db_name, std::string& err) {
  try {
    if (!EnsureDataDir(err)) return false;
    auto dir = DbDirPath(db_name);
    if (!std::filesystem::exists(dir)) {
      if (!std::filesystem::create_directories(dir)) {
        err = "Failed to create database directory: " + dir.string();
        return false;
      }
    }
    auto idx_dir = IndexDirPath(db_name);
    if (!std::filesystem::exists(idx_dir)) {
      if (!std::filesystem::create_directories(idx_dir)) {
        err = "Failed to create index directory: " + idx_dir.string();
        return false;
      }
    }
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    err = std::string("Filesystem error: ") + e.what();
    return false;
  }
}

bool EnsureIndexDirFromDat(const std::string& dat_path, std::string& err) {
  try {
    auto dir = IndexDirFromDat(dat_path);
    if (!std::filesystem::exists(dir)) {
      if (!std::filesystem::create_directories(dir)) {
        err = "Failed to create index directory: " + dir.string();
        return false;
      }
    }
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    err = std::string("Filesystem error: ") + e.what();
    return false;
  }
}

std::filesystem::path DbDirPath(const std::string& db_name) {
  return DataDirPath() / db_name;
}

std::filesystem::path IndexDirPath(const std::string& db_name) {
  return DbDirPath(db_name) / "index";
}

std::filesystem::path IndexDirFromDat(const std::string& dat_path) {
  std::filesystem::path dat = dat_path;
  return dat.parent_path() / "index";
}

std::string DbfPath(const std::string& db_name) {
  return (DbDirPath(db_name) / (db_name + ".dbf")).string();
}

std::string DatPath(const std::string& db_name) {
  return (DbDirPath(db_name) / (db_name + ".dat")).string();
}

std::string WalPath(const std::string& db_name) {
  return (DbDirPath(db_name) / (db_name + ".wal")).string();
}

std::filesystem::path BackupRootPath() {
  return DataDirPath() / "backups";
}

bool EnsureBackupRoot(std::string& err) {
  try {
    if (!EnsureDataDir(err)) return false;
    auto dir = BackupRootPath();
    if (!std::filesystem::exists(dir)) {
      if (!std::filesystem::create_directories(dir)) {
        err = "Failed to create backup root directory: " + dir.string();
        return false;
      }
    }
    return true;
  } catch (const std::filesystem::filesystem_error& e) {
    err = std::string("Filesystem error: ") + e.what();
    return false;
  }
}

std::filesystem::path BackupDbDirPath(const std::string& db_name) {
  return BackupRootPath() / db_name;
}

std::filesystem::path BackupPath(const std::string& db_name, const std::string& backup_name) {
  return BackupDbDirPath(db_name) / backup_name;
}

std::string IndexPathFromDat(const std::string& dat_path, const std::string& table_name, const std::string& index_name) {
  auto dir = IndexDirFromDat(dat_path);
  std::string file = table_name + "." + index_name + ".idx";
  return (dir / file).string();
}

}  // namespace dbms_paths

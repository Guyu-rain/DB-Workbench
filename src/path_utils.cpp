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

std::string DbfPath(const std::string& db_name) {
  return (DataDirPath() / (db_name + ".dbf")).string();
}

std::string DatPath(const std::string& db_name) {
  return (DataDirPath() / (db_name + ".dat")).string();
}

std::string WalPath(const std::string& db_name) {
  return (DataDirPath() / (db_name + ".wal")).string();
}

}  // namespace dbms_paths

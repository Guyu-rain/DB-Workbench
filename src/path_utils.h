#pragma once

#include <filesystem>
#include <string>

namespace dbms_paths {

std::filesystem::path DataDirPath();
bool EnsureDataDir(std::string& err);
bool EnsureDbDir(const std::string& db_name, std::string& err);
bool EnsureIndexDirFromDat(const std::string& dat_path, std::string& err);
std::filesystem::path DbDirPath(const std::string& db_name);
std::filesystem::path IndexDirPath(const std::string& db_name);
std::filesystem::path IndexDirFromDat(const std::string& dat_path);
std::string DbfPath(const std::string& db_name);
std::string DatPath(const std::string& db_name);
std::string WalPath(const std::string& db_name);
std::string IndexPathFromDat(const std::string& dat_path, const std::string& table_name, const std::string& index_name);

}  // namespace dbms_paths

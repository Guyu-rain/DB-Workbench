#pragma once

#include <filesystem>
#include <string>

namespace dbms_paths {

std::filesystem::path DataDirPath();
bool EnsureDataDir(std::string& err);
std::string DbfPath(const std::string& db_name);
std::string DatPath(const std::string& db_name);
std::string WalPath(const std::string& db_name);

}  // namespace dbms_paths

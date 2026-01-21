#pragma once
#include <string>
#include <vector>
#include "db_types.h"
#include "storage_engine.h"
#include "txn/txn_types.h"

class LogManager;
class LockManager;

class DMLService {
 public:
  explicit DMLService(StorageEngine& engine) : engine_(engine) {}

  bool Insert(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const std::vector<Record>& records, std::string& err,
              Txn* txn = nullptr, LogManager* log = nullptr, LockManager* lock_manager = nullptr);
  bool Delete(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const std::vector<Condition>& conditions,
              ReferentialAction action, bool actionSpecified, std::string& err,
              Txn* txn = nullptr, LogManager* log = nullptr, LockManager* lock_manager = nullptr);
  bool Update(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const std::vector<Condition>& conditions,
              const std::vector<std::pair<std::string, std::string>>& assignments, std::string& err,
              Txn* txn = nullptr, LogManager* log = nullptr, LockManager* lock_manager = nullptr);
  
  bool Match(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conditions) const;

 private:
  StorageEngine& engine_;
};

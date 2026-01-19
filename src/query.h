#pragma once
#include <string>
#include <vector>
#include "db_types.h"
#include "storage_engine.h"
#include "txn/txn_types.h"

class LockManager;

class QueryService {
 public:
  explicit QueryService(StorageEngine& engine) : engine_(engine) {}

  bool Select(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const QueryPlan& plan,
              std::vector<Record>& out, std::string& err, Txn* txn = nullptr, LockManager* lock_manager = nullptr);

 private:
  StorageEngine& engine_;
  bool MatchConditions(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conds, const std::string& datPath, const std::string& dbfPath);
  Record Project(const TableSchema& schema, const Record& rec, const std::vector<std::string>& projection) const;
  
  // Helper to execute subquery
  bool ExecuteSubQuery(const std::string& datPath, const std::string& dbfPath, const QueryPlan& plan, std::vector<Record>& out, std::string& err);
};

#pragma once
#include <string>
#include <vector>
#include <map>
#include "txn_types.h"

class LogManager {
 public:
  explicit LogManager(const std::string& db_name = "");

  void SetDbName(const std::string& db_name);
  const std::string& WalPath() const { return wal_path_; }

  LSN Append(LogRecord& rec, std::string& err);
  bool Flush(LSN lsn, std::string& err);
  bool GetRecord(LSN lsn, LogRecord& out) const;
  bool ReadAll(std::vector<LogRecord>& out, std::string& err) const;
  LSN NextLsn() const { return next_lsn_; }
  void SetNextLsn(LSN next) { next_lsn_ = next; }

 private:
  std::string wal_path_;
  LSN next_lsn_ = 1;
  std::map<LSN, LogRecord> cache_;
};

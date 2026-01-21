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
  bool TruncateWithBackup(std::string& err);
  LSN NextLsn() const { return next_lsn_; }
  void SetNextLsn(LSN next) { next_lsn_ = next; }

 private:
  std::string wal_path_;
  LSN next_lsn_ = 1;
  std::map<LSN, LogRecord> cache_;
};

struct CheckpointMeta {
  uint32_t version = 1;
  uint64_t checkpoint_lsn = 0;
  uint64_t timestamp_sec = 0;
};

bool EncodeCheckpointMeta(LogRecord& rec, const CheckpointMeta& meta);
bool DecodeCheckpointMeta(const LogRecord& rec, CheckpointMeta& meta);

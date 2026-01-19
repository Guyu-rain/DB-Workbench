#pragma once
#include <cstdint>
#include <string>
#include <vector>

using TxnId = uint64_t;
using LSN = uint64_t;

struct RID {
  std::string table_name;
  uint64_t file_offset = 0;
};

enum class TxnState {
  ACTIVE,
  COMMITTED,
  ABORTED
};

enum class IsolationLevel {
  READ_COMMITTED
};

enum class LogType {
  BEGIN,
  INSERT,
  UPDATE,
  DELETE,
  COMMIT,
  ABORT
};

struct LogRecord {
  LSN lsn = 0;
  TxnId txn_id = 0;
  LogType type = LogType::BEGIN;
  RID rid;
  std::vector<uint8_t> before;
  std::vector<uint8_t> after;
};

struct Savepoint {
  std::string name;
  size_t undo_chain_size = 0;
};

struct Txn {
  TxnId id = 0;
  TxnState state = TxnState::ACTIVE;
  std::vector<LSN> undo_chain;
  std::vector<Savepoint> savepoints;
  std::string db_name;
  std::vector<std::string> touched_tables;
};

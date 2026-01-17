#pragma once
#include <string>
#include "txn_types.h"

class StorageEngine;
class LogManager;

class TxnManager {
 public:
  TxnManager(StorageEngine& engine, LogManager& log);

  Txn* Begin(const std::string& db_name, std::string& err);
  bool Commit(Txn* txn, std::string& err);
  bool Rollback(Txn* txn, std::string& err);

  bool Savepoint(Txn* txn, const std::string& name, std::string& err);
  bool RollbackTo(Txn* txn, const std::string& name, std::string& err);
  bool ReleaseSavepoint(Txn* txn, const std::string& name, std::string& err);

  void SetNextTxnId(TxnId next) { next_txn_id_ = next; }

 private:
  bool UndoRecord(const LogRecord& rec, const std::string& db_name, std::string& err);

  StorageEngine& engine_;
  LogManager& log_;
  TxnId next_txn_id_ = 1;
};

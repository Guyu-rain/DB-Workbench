#include "txn_manager.h"
#include "log_manager.h"
#include "../storage_engine.h"
#include "../path_utils.h"

TxnManager::TxnManager(StorageEngine& engine, LogManager& log)
    : engine_(engine), log_(log) {}

Txn* TxnManager::Begin(const std::string& db_name, std::string& err) {
  if (db_name.empty()) {
    err = "Database name required for BEGIN";
    return nullptr;
  }

  Txn* txn = new Txn();
  txn->id = next_txn_id_++;
  txn->state = TxnState::ACTIVE;
  txn->db_name = db_name;

  LogRecord rec;
  rec.txn_id = txn->id;
  rec.type = LogType::BEGIN;
  log_.SetDbName(db_name);
  if (log_.Append(rec, err) == 0) {
    delete txn;
    return nullptr;
  }

  return txn;
}

bool TxnManager::Commit(Txn* txn, std::string& err) {
  if (!txn || txn->state != TxnState::ACTIVE) {
    err = "No active transaction";
    return false;
  }

  LogRecord rec;
  rec.txn_id = txn->id;
  rec.type = LogType::COMMIT;
  log_.SetDbName(txn->db_name);
  LSN lsn = log_.Append(rec, err);
  if (lsn == 0) return false;
  if (!log_.Flush(lsn, err)) return false;

  txn->state = TxnState::COMMITTED;
  return true;
}

bool TxnManager::Rollback(Txn* txn, std::string& err) {
  if (!txn || txn->state != TxnState::ACTIVE) {
    err = "No active transaction";
    return false;
  }

  for (auto it = txn->undo_chain.rbegin(); it != txn->undo_chain.rend(); ++it) {
    LogRecord rec;
    if (!log_.GetRecord(*it, rec)) continue;
    if (!UndoRecord(rec, txn->db_name, err)) return false;
  }

  LogRecord ab;
  ab.txn_id = txn->id;
  ab.type = LogType::ABORT;
  log_.SetDbName(txn->db_name);
  if (log_.Append(ab, err) == 0) return false;

  txn->state = TxnState::ABORTED;
  return true;
}

bool TxnManager::Savepoint(Txn* txn, const std::string& name, std::string& err) {
  if (!txn || txn->state != TxnState::ACTIVE) {
    err = "No active transaction";
    return false;
  }
  ::Savepoint sp;
  sp.name = name;
  sp.undo_chain_size = txn->undo_chain.size();
  txn->savepoints.push_back(sp);
  return true;
}

bool TxnManager::RollbackTo(Txn* txn, const std::string& name, std::string& err) {
  if (!txn || txn->state != TxnState::ACTIVE) {
    err = "No active transaction";
    return false;
  }

  size_t target = static_cast<size_t>(-1);
  for (auto it = txn->savepoints.rbegin(); it != txn->savepoints.rend(); ++it) {
    if (it->name == name) { target = it->undo_chain_size; break; }
  }
  if (target == static_cast<size_t>(-1)) {
    err = "Savepoint not found: " + name;
    return false;
  }

  while (txn->undo_chain.size() > target) {
    LSN lsn = txn->undo_chain.back();
    txn->undo_chain.pop_back();
    LogRecord rec;
    if (!log_.GetRecord(lsn, rec)) continue;
    if (!UndoRecord(rec, txn->db_name, err)) return false;
  }
  return true;
}

bool TxnManager::ReleaseSavepoint(Txn* txn, const std::string& name, std::string& err) {
  if (!txn || txn->state != TxnState::ACTIVE) {
    err = "No active transaction";
    return false;
  }
  for (auto it = txn->savepoints.begin(); it != txn->savepoints.end(); ++it) {
    if (it->name == name) {
      txn->savepoints.erase(it);
      return true;
    }
  }
  err = "Savepoint not found: " + name;
  return false;
}

bool TxnManager::UndoRecord(const LogRecord& rec, const std::string& db_name, std::string& err) {
  std::string dat = dbms_paths::DatPath(db_name);
  std::string dbf = dbms_paths::DbfPath(db_name);
  TableSchema schema;
  if (!engine_.LoadSchema(dbf, rec.rid.table_name, schema, err)) return false;

  if (rec.type == LogType::INSERT) {
    if (!rec.after.empty()) {
      std::vector<uint8_t> bytes = rec.after;
      if (!bytes.empty()) bytes[0] = 0;
    return engine_.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), bytes, err);
    }
    return true;
  }
  if (rec.type == LogType::UPDATE) {
    return engine_.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), rec.before, err);
  }
  if (rec.type == LogType::DELETE) {
    return engine_.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), rec.before, err);
  }
  return true;
}

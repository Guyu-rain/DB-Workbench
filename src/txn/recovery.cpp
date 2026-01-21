#include "recovery.h"
#include "log_manager.h"
#include "../storage_engine.h"
#include "../path_utils.h"
#include <map>

namespace {
bool ApplyRedo(StorageEngine& engine, const std::string& db_name, const LogRecord& rec, std::string& err) {
  std::string dat = dbms_paths::DatPath(db_name);
  std::string dbf = dbms_paths::DbfPath(db_name);
  TableSchema schema;
  if (!engine.LoadSchema(dbf, rec.rid.table_name, schema, err)) return false;

  if (rec.type == LogType::INSERT) {
    return engine.WriteInsertBlockAt(dat, schema, static_cast<long>(rec.rid.file_offset), rec.after, err);
  }
  if (rec.type == LogType::UPDATE) {
    return engine.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), rec.after, err);
  }
  if (rec.type == LogType::DELETE) {
    if (rec.before.empty()) return true;
    std::vector<uint8_t> bytes = rec.before;
    if (!bytes.empty()) bytes[0] = 0;
    return engine.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), bytes, err);
  }
  return true;
}

bool ApplyUndo(StorageEngine& engine, const std::string& db_name, const LogRecord& rec, std::string& err) {
  std::string dat = dbms_paths::DatPath(db_name);
  if (rec.type == LogType::INSERT) {
    if (rec.after.empty()) return true;
    std::vector<uint8_t> bytes = rec.after;
    if (!bytes.empty()) bytes[0] = 0;
    return engine.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), bytes, err);
  }
  if (rec.type == LogType::UPDATE) {
    return engine.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), rec.before, err);
  }
  if (rec.type == LogType::DELETE) {
    return engine.WriteRecordBytesAt(dat, static_cast<long>(rec.rid.file_offset), rec.before, err);
  }
  return true;
}
}

TxnId Recovery::Run(StorageEngine& engine, const std::string& db_name, std::string& err, LSN* max_lsn_out) {
  LogManager log(db_name);
  std::vector<LogRecord> records;
  if (!log.ReadAll(records, err)) return 0;

  TxnId max_txn = 0;
  LSN max_lsn_total = 0;
  LSN min_lsn = 0;
  for (const auto& rec : records) {
    if (rec.txn_id > max_txn) max_txn = rec.txn_id;
    if (rec.lsn > max_lsn_total) max_lsn_total = rec.lsn;
    if (rec.type != LogType::CHECKPOINT) continue;
    CheckpointMeta meta;
    if (DecodeCheckpointMeta(rec, meta) && meta.checkpoint_lsn != 0) {
      min_lsn = std::max(min_lsn, static_cast<LSN>(meta.checkpoint_lsn));
    } else {
      min_lsn = std::max(min_lsn, rec.lsn);
    }
  }
  std::map<TxnId, bool> committed;
  std::map<TxnId, bool> active;
  std::map<TxnId, std::vector<LogRecord>> per_txn;

  for (const auto& rec : records) {
    if (min_lsn != 0 && rec.lsn < min_lsn) continue;
    if (rec.type == LogType::CHECKPOINT) continue;
    per_txn[rec.txn_id].push_back(rec);
    if (rec.type == LogType::BEGIN) active[rec.txn_id] = true;
    if (rec.type == LogType::COMMIT) { committed[rec.txn_id] = true; active.erase(rec.txn_id); }
    if (rec.type == LogType::ABORT) { active.erase(rec.txn_id); }
  }

  for (const auto& rec : records) {
    if (min_lsn != 0 && rec.lsn < min_lsn) continue;
    if (committed.count(rec.txn_id)) {
      if (!ApplyRedo(engine, db_name, rec, err)) return max_txn;
    }
  }

  for (const auto& kv : active) {
    auto it = per_txn.find(kv.first);
    if (it == per_txn.end()) continue;
    auto& logs = it->second;
    for (auto rit = logs.rbegin(); rit != logs.rend(); ++rit) {
      if (!ApplyUndo(engine, db_name, *rit, err)) return max_txn;
    }
  }

  if (max_lsn_out) *max_lsn_out = max_lsn_total;
  return max_txn;
}

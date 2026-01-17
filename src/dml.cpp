#include "dml.h"
#include <algorithm>
#include <cctype>
#include <string>
#include <map>
#include "txn/log_manager.h"
#include "txn/lock_manager.h"

namespace {
std::string Lower(const std::string& s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return out;
}

std::string NormalizeValue(std::string s) {
  if (s.size() >= 2) {
    if ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"')) {
      return s.substr(1, s.size() - 2);
    }
  }
  return s;
}

void AddTouchedTable(Txn* txn, const std::string& table) {
  if (!txn) return;
  auto it = std::find(txn->touched_tables.begin(), txn->touched_tables.end(), table);
  if (it == txn->touched_tables.end()) txn->touched_tables.push_back(table);
}
}

bool DMLService::Match(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conditions) const {
  if (conditions.empty()) return true;

  auto asNumber = [](const std::string& s, double& out) {
      try {
          size_t idx = 0;
          out = std::stod(s, &idx);
          // ensure backend is not garbage
          return idx == s.size(); 
      } catch (...) { return false; }
  };

  for (const auto& cond : conditions) {
      if (cond.fieldName.empty()) continue;

      auto it = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f) { return Lower(f.name) == Lower(cond.fieldName); });
      if (it == schema.fields.end()) return false;
      size_t idx = static_cast<size_t>(std::distance(schema.fields.begin(), it));
      if (idx >= rec.values.size()) return false;
      std::string val = NormalizeValue(rec.values[idx]);
      std::string condVal = NormalizeValue(cond.value);

      bool match = false;
      if (cond.op == "IN") {
          for(const auto& v : cond.values) {
              std::string nv = NormalizeValue(v);
              // Try numeric comparison first
              double valNum = 0, vNum = 0;
              if (asNumber(val, valNum) && asNumber(nv, vNum)) {
                  if (std::abs(valNum - vNum) < 1e-9) { match = true; break; }
              }
              // Fallback to string comparison
              if (val == nv) { match = true; break; }
          }
      } 
      else if (cond.op == "=") {
          double valNum = 0, cNum = 0;
          if (asNumber(val, valNum) && asNumber(condVal, cNum)) {
              match = (std::abs(valNum - cNum) < 1e-9);
          } else {
              match = (val == condVal);
          }
      }
      else if (cond.op == "!=") {
          // Inverse of =
          double valNum = 0, cNum = 0;
          if (asNumber(val, valNum) && asNumber(condVal, cNum)) {
              match = (std::abs(valNum - cNum) >= 1e-9);
          } else {
              match = (val != condVal);
          }
      }
      else if (cond.op == "CONTAINS") match = (val.find(condVal) != std::string::npos);
      else if (cond.op == ">" || cond.op == ">=" || cond.op == "<" || cond.op == "<=") {
        double lv = 0, rv = 0;
        if (asNumber(val, lv) && asNumber(condVal, rv)) {
            if (cond.op == ">") match = lv > rv;
            else if (cond.op == ">=") match = lv >= rv;
            else if (cond.op == "<") match = lv < rv;
            else if (cond.op == "<=") match = lv <= rv;
        } else {
            // Fallback to string comparison
            if (cond.op == ">") match = val > condVal;
            else if (cond.op == ">=") match = val >= condVal;
            else if (cond.op == "<") match = val < condVal;
            else if (cond.op == "<=") match = val <= condVal;
        }
      }

      if (!match) return false;
  }
  return true;
}

static std::string GetIdxPath(const std::string& datPath, const std::string& tableName, const std::string& fieldName) {
    return datPath + "." + tableName + "." + fieldName + ".idx";
}

bool DMLService::Insert(const std::string& datPath, const TableSchema& schema, const std::vector<Record>& records, std::string& err,
                        Txn* txn, LogManager* log, LockManager* lock_manager) {
  // Map field names to indices
  std::map<std::string, size_t> fieldMap;
  for(size_t i=0; i<schema.fields.size(); ++i) fieldMap[schema.fields[i].name] = i;

  if (txn && log) {
      for (const auto& r : records) {
          long offset = 0;
          if (!engine_.ComputeAppendRecordOffset(datPath, schema, offset, err)) return false;
          if (lock_manager) {
              RID rid{schema.tableName, static_cast<uint64_t>(offset)};
              if (!lock_manager->LockExclusive(txn->id, rid, err)) return false;
          }
          std::vector<uint8_t> after;
          if (!engine_.SerializeRecord(schema, r, after, err)) return false;

          LogRecord rec;
          rec.txn_id = txn->id;
          rec.type = LogType::INSERT;
          rec.rid.table_name = schema.tableName;
          rec.rid.file_offset = static_cast<uint64_t>(offset);
          rec.after = after;
          LSN lsn = log->Append(rec, err);
          if (lsn == 0) return false;
          txn->undo_chain.push_back(lsn);

          long realOffset = 0;
          if (!engine_.AppendRecord(datPath, schema, r, realOffset, err)) return false;
          if (realOffset != offset) {
              err = "Append offset mismatch for WAL";
              return false;
          }
          AddTouchedTable(txn, schema.tableName);
      }
      return true;
  }

  // Non-transactional path (legacy behavior)
  std::map<std::string, std::map<std::string, long>> openIndexes;
  for (const auto& idxDef : schema.indexes) {
      std::map<std::string, long> idx;
      engine_.LoadIndex(GetIdxPath(datPath, schema.tableName, idxDef.name), idx, err);
      openIndexes[idxDef.name] = idx;
  }

  for (const auto& r : records) {
      for (const auto& pair : openIndexes) {
           std::string idxName = pair.first;
           const IndexDef* def = nullptr;
           for (const auto& d : schema.indexes) if (d.name == idxName) def = &d;
           if (def && def->isUnique) {
               if (fieldMap.count(def->fieldName)) {
                   size_t fIdx = fieldMap[def->fieldName];
                   if (fIdx < r.values.size()) {
                        std::string val = NormalizeValue(r.values[fIdx]);
                        if (pair.second.count(val)) {
                            err = "Duplicate entry '" + val + "' for key '" + def->name + "'";
                            return false;
                        }
                   }
               }
           }
      }
  }

  for (const auto& r : records) {
      long offset = 0;
      if (!engine_.AppendRecord(datPath, schema, r, offset, err)) return false;
      for (auto& pair : openIndexes) {
          std::string idxName = pair.first;
          const IndexDef* def = nullptr;
          for (const auto& d : schema.indexes) if (d.name == idxName) def = &d;
          if (def && fieldMap.count(def->fieldName)) {
                size_t fIdx = fieldMap[def->fieldName];
                if (fIdx < r.values.size()) {
                    std::string val = NormalizeValue(r.values[fIdx]);
                    pair.second[val] = offset;
                }
          }
      }
  }

  for (const auto& pair : openIndexes) {
       if (!engine_.SaveIndex(GetIdxPath(datPath, schema.tableName, pair.first), pair.second, err)) return false;
  }

  return true;
}

bool DMLService::Delete(const std::string& datPath, const TableSchema& schema, const std::vector<Condition>& conditions, std::string& err,
                        Txn* txn, LogManager* log, LockManager* lock_manager) {
  if (txn && log) {
      std::vector<std::pair<long, Record>> records;
      if (!engine_.ReadRecordsWithOffsets(datPath, schema, records, err)) return false;
      bool hit = false;
      for (auto& p : records) {
          if (!Match(schema, p.second, conditions)) continue;
          hit = true;
          if (lock_manager) {
              RID rid{schema.tableName, static_cast<uint64_t>(p.first)};
              if (!lock_manager->LockExclusive(txn->id, rid, err)) return false;
          }

          std::vector<uint8_t> before;
          if (!engine_.SerializeRecord(schema, p.second, before, err)) return false;

          LogRecord rec;
          rec.txn_id = txn->id;
          rec.type = LogType::DELETE;
          rec.rid.table_name = schema.tableName;
          rec.rid.file_offset = static_cast<uint64_t>(p.first);
          rec.before = before;
          LSN lsn = log->Append(rec, err);
          if (lsn == 0) return false;
          txn->undo_chain.push_back(lsn);

          std::vector<uint8_t> after = before;
          if (!after.empty()) after[0] = 0;
          if (!engine_.WriteRecordBytesAt(datPath, p.first, after, err)) return false;
          AddTouchedTable(txn, schema.tableName);
      }
      if (!hit) err = "No record matched";
      return true;
  }

  std::vector<Record> records;
  if (!engine_.ReadRecords(datPath, schema, records, err)) return false;
  bool hit = false;
  for (auto& r : records) {
    if (Match(schema, r, conditions)) {
      r.valid = false;
      hit = true;
    }
  }
  if (!hit) {
    err = "No record matched";
  }
  if (!engine_.SaveRecords(datPath, schema, records, err)) return false;

  if (!schema.indexes.empty()) {
      std::vector<std::pair<long, Record>> newRecords;
      if (!engine_.ReadRecordsWithOffsets(datPath, schema, newRecords, err)) return false;

      for (const auto& idxDef : schema.indexes) {
           std::map<std::string, long> idxMap;
           size_t fIdx = -1;
           for (size_t i = 0; i < schema.fields.size(); ++i) {
               if (schema.fields[i].name == idxDef.fieldName) { fIdx = i; break; }
           }
           if (fIdx == static_cast<size_t>(-1)) continue;

           for (const auto& p : newRecords) {
               if (fIdx < p.second.values.size()) {
                   std::string val = NormalizeValue(p.second.values[fIdx]);
                   idxMap[val] = p.first;
               }
           }
           engine_.SaveIndex(GetIdxPath(datPath, schema.tableName, idxDef.name), idxMap, err);
      }
  }

  return true;
}

bool DMLService::Update(const std::string& datPath, const TableSchema& schema, const std::vector<Condition>& conditions,
                        const std::vector<std::pair<std::string, std::string>>& assignments, std::string& err,
                        Txn* txn, LogManager* log, LockManager* lock_manager) {
  if (txn && log) {
      std::vector<std::pair<long, Record>> records;
      if (!engine_.ReadRecordsWithOffsets(datPath, schema, records, err)) return false;
      bool hit = false;
      for (auto& p : records) {
          if (!Match(schema, p.second, conditions)) continue;
          hit = true;
          if (lock_manager) {
              RID rid{schema.tableName, static_cast<uint64_t>(p.first)};
              if (!lock_manager->LockExclusive(txn->id, rid, err)) return false;
          }
          Record updated = p.second;
          for (const auto& kv : assignments) {
              auto it = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f) { return f.name == kv.first; });
              if (it == schema.fields.end()) continue;
              size_t idx = static_cast<size_t>(std::distance(schema.fields.begin(), it));
              if (idx < updated.values.size()) updated.values[idx] = kv.second;
          }

          std::vector<uint8_t> before;
          std::vector<uint8_t> after;
          if (!engine_.SerializeRecord(schema, p.second, before, err)) return false;
          if (!engine_.SerializeRecord(schema, updated, after, err)) return false;
            if (before.size() != after.size()) {
                // Fallback: treat as DELETE + INSERT (stable offsets for old record, new record appended)
                LogRecord del;
                del.txn_id = txn->id;
                del.type = LogType::DELETE;
                del.rid.table_name = schema.tableName;
                del.rid.file_offset = static_cast<uint64_t>(p.first);
                del.before = before;
                LSN delLsn = log->Append(del, err);
                if (delLsn == 0) return false;
                txn->undo_chain.push_back(delLsn);

                long newOffset = 0;
                if (!engine_.ComputeAppendRecordOffset(datPath, schema, newOffset, err)) return false;
                if (lock_manager) {
                    RID newRid{schema.tableName, static_cast<uint64_t>(newOffset)};
                    if (!lock_manager->LockExclusive(txn->id, newRid, err)) return false;
                }

                LogRecord ins;
                ins.txn_id = txn->id;
                ins.type = LogType::INSERT;
                ins.rid.table_name = schema.tableName;
                ins.rid.file_offset = static_cast<uint64_t>(newOffset);
                ins.after = after;
                LSN insLsn = log->Append(ins, err);
                if (insLsn == 0) return false;
                txn->undo_chain.push_back(insLsn);

                std::vector<uint8_t> tomb = before;
                if (!tomb.empty()) tomb[0] = 0;
                if (!engine_.WriteRecordBytesAt(datPath, p.first, tomb, err)) return false;

                long realOffset = 0;
                if (!engine_.AppendRecord(datPath, schema, updated, realOffset, err)) return false;
                if (realOffset != newOffset) {
                    err = "Append offset mismatch for WAL";
                    return false;
                }
                AddTouchedTable(txn, schema.tableName);
            } else {
                LogRecord rec;
                rec.txn_id = txn->id;
                rec.type = LogType::UPDATE;
                rec.rid.table_name = schema.tableName;
                rec.rid.file_offset = static_cast<uint64_t>(p.first);
                rec.before = before;
                rec.after = after;
                LSN lsn = log->Append(rec, err);
                if (lsn == 0) return false;
                txn->undo_chain.push_back(lsn);

                if (!engine_.WriteRecordBytesAt(datPath, p.first, after, err)) return false;
                AddTouchedTable(txn, schema.tableName);
            }
      }
      if (!hit) err = "No record matched";
      return true;
  }

  std::vector<Record> records;
  if (!engine_.ReadRecords(datPath, schema, records, err)) return false;
  bool hit = false;
  for (auto& r : records) {
    if (!Match(schema, r, conditions)) continue;
    hit = true;
    for (const auto& kv : assignments) {
      auto it = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f) { return f.name == kv.first; });
      if (it == schema.fields.end()) continue;
      size_t idx = static_cast<size_t>(std::distance(schema.fields.begin(), it));
      if (idx < r.values.size()) r.values[idx] = kv.second;
    }
  }
  if (!hit) err = "No record matched";
  if (!engine_.SaveRecords(datPath, schema, records, err)) return false;

  if (!schema.indexes.empty()) {
      std::vector<std::pair<long, Record>> newRecords;
      if (!engine_.ReadRecordsWithOffsets(datPath, schema, newRecords, err)) return false;

      for (const auto& idxDef : schema.indexes) {
           std::map<std::string, long> idxMap;
           size_t fIdx = -1;
           for (size_t i = 0; i < schema.fields.size(); ++i) {
               if (schema.fields[i].name == idxDef.fieldName) { fIdx = i; break; }
           }
           if (fIdx == static_cast<size_t>(-1)) continue;

           for (const auto& p : newRecords) {
               if (fIdx < p.second.values.size()) {
                   std::string val = NormalizeValue(p.second.values[fIdx]);
                   idxMap[val] = p.first;
               }
           }
           engine_.SaveIndex(GetIdxPath(datPath, schema.tableName, idxDef.name), idxMap, err);
      }
  }
  return true;
}

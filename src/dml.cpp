#include "dml.h"
#include <algorithm>
#include <cctype>
#include <string>
#include <map>
#include <unordered_set>
#include "path_utils.h"
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

std::string Trim(std::string s) {
  auto notSpace = [](unsigned char ch) { return !std::isspace(ch); };
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
  s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
  return s;
}

void AddTouchedTable(Txn* txn, const std::string& table) {
  if (!txn) return;
  auto it = std::find(txn->touched_tables.begin(), txn->touched_tables.end(), table);
  if (it == txn->touched_tables.end()) txn->touched_tables.push_back(table);
}

std::string BuildCompositeKey(const Record& rec, const std::vector<size_t>& keyIdxs) {
  std::string key;
  for (size_t i = 0; i < keyIdxs.size(); ++i) {
    if (i) key.push_back('\x1f');  // ASCII unit separator
    const size_t idx = keyIdxs[i];
    std::string val = (idx < rec.values.size()) ? NormalizeValue(rec.values[idx]) : "";
    key += val;
  }
  return key;
}

std::string BuildKeyDisplay(const Record& rec, const std::vector<size_t>& keyIdxs) {
  std::string out;
  for (size_t i = 0; i < keyIdxs.size(); ++i) {
    if (i) out += ", ";
    const size_t idx = keyIdxs[i];
    std::string val = (idx < rec.values.size()) ? NormalizeValue(rec.values[idx]) : "";
    out += val;
  }
  return out;
}

static std::string GetIdxPath(const std::string& datPath, const std::string& tableName, const std::string& fieldName) {
  return dbms_paths::IndexPathFromDat(datPath, tableName, fieldName);
}

bool FindFieldIndex(const TableSchema& schema, const std::string& name, size_t& outIdx) {
  std::string low = Lower(name);
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    if (Lower(schema.fields[i].name) == low) { outIdx = i; return true; }
  }
  return false;
}

bool IsNullableColumn(const TableSchema& schema, const std::string& name) {
  size_t idx = 0;
  if (!FindFieldIndex(schema, name, idx)) return false;
  return schema.fields[idx].nullable;
}

bool IsNullValue(const std::string& s) {
  std::string v = Trim(NormalizeValue(s));
  return v.empty() || Lower(v) == "null";
}

std::vector<std::string> ResolveRefColumns(const TableSchema& refSchema, const ForeignKeyDef& fk) {
  if (!fk.refColumns.empty()) return fk.refColumns;
  std::vector<std::string> pkCols;
  for (const auto& f : refSchema.fields) {
    if (f.isKey) pkCols.push_back(f.name);
  }
  if (!pkCols.empty() && pkCols.size() == fk.columns.size()) return pkCols;
  return fk.columns;
}

std::string StripIdentQuotes(std::string s) {
  s = Trim(NormalizeValue(s));
  if (s.size() >= 2) {
    char f = s.front();
    char b = s.back();
    if ((f == '`' && b == '`') || (f == '"' && b == '"') || (f == '\'' && b == '\'')) {
      return s.substr(1, s.size() - 2);
    }
  }
  return s;
}

std::string NormalizeScalar(std::string s) {
  return Trim(NormalizeValue(s));
}

ForeignKeyDef NormalizeForeignKey(const ForeignKeyDef& fk) {
  ForeignKeyDef out = fk;
  out.name = StripIdentQuotes(out.name);
  out.refTable = StripIdentQuotes(out.refTable);
  size_t lp = out.refTable.find('(');
  if (lp != std::string::npos) out.refTable = out.refTable.substr(0, lp);
  out.refTable = Trim(out.refTable);
  for (auto& c : out.columns) c = StripIdentQuotes(c);
  for (auto& c : out.refColumns) c = StripIdentQuotes(c);
  return out;
}

bool LoadSchemaCached(StorageEngine& engine, const std::string& dbfPath,
                      const std::string& tableName, std::map<std::string, TableSchema>& cache,
                      TableSchema& out, std::string& err) {
  std::string normalized = Trim(tableName);
  auto it = cache.find(normalized);
  if (it != cache.end()) { out = it->second; return true; }
  if (!engine.LoadSchema(dbfPath, normalized, out, err)) {
    std::vector<TableSchema> schemas;
    std::string scanErr;
    if (!engine.LoadSchemas(dbfPath, schemas, scanErr)) return false;
    for (const auto& s : schemas) {
      if (Lower(s.tableName) == Lower(normalized)) {
        out = s;
        cache[normalized] = out;
        return true;
      }
    }
    return false;
  }
  cache[normalized] = out;
  return true;
}

bool HasUniqueIndexOn(const TableSchema& schema, const std::string& fieldName) {
  for (const auto& f : schema.fields) {
    if (Lower(f.name) == Lower(fieldName) && f.isKey) return true;
  }
  for (const auto& idx : schema.indexes) {
    if (Lower(idx.fieldName) == Lower(fieldName) && idx.isUnique) return true;
  }
  return false;
}

bool FindReferencedRecord(StorageEngine& engine, const std::string& datPath, const TableSchema& refSchema,
                          const std::vector<std::string>& refCols, const std::vector<std::string>& values,
                          std::string& err) {
  if (refCols.size() != values.size()) return false;
  auto asNumber = [](const std::string& s, double& out) {
    try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
  };
  if (refCols.size() == 1 && HasUniqueIndexOn(refSchema, refCols[0])) {
    std::string idxName;
    for (const auto& idx : refSchema.indexes) {
      if (Lower(idx.fieldName) == Lower(refCols[0])) { idxName = idx.name; break; }
    }
    if (idxName.empty()) idxName = "PRIMARY";
    std::map<std::string, long> idx;
    std::string ignErr;
    std::string idxPath = dbms_paths::IndexPathFromDat(datPath, refSchema.tableName, idxName);
    if (engine.LoadIndex(idxPath, idx, ignErr)) {
      std::string key = NormalizeValue(values[0]);
      auto it = idx.find(key);
      if (it != idx.end()) {
        Record rec;
        std::string idxErr;
        if (engine.ReadRecordAt(datPath, refSchema, it->second, rec, idxErr)) {
          if (rec.valid) return true;
        }
      }
    }
  }

  std::vector<Record> records;
  if (!engine.ReadRecords(datPath, refSchema, records, err)) return false;
  for (const auto& r : records) {
    if (!r.valid) continue;
    bool match = true;
    for (size_t i = 0; i < refCols.size(); ++i) {
      size_t idx = 0;
      if (!FindFieldIndex(refSchema, refCols[i], idx)) { match = false; break; }
      std::string val = (idx < r.values.size()) ? NormalizeScalar(r.values[idx]) : "";
      std::string target = NormalizeScalar(values[i]);
      if (val != target) {
        double v1 = 0, v2 = 0;
        if (!(asNumber(val, v1) && asNumber(target, v2) && std::abs(v1 - v2) < 1e-9)) {
          match = false;
          break;
        }
      }
    }
    if (match) return true;
  }
  return false;
}

bool RebuildIndexesForTable(StorageEngine& engine, const std::string& datPath, const TableSchema& schema, std::string& err) {
  if (schema.indexes.empty()) return true;
  std::vector<std::pair<long, Record>> newRecords;
  if (!engine.ReadRecordsWithOffsets(datPath, schema, newRecords, err)) return false;
  if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;
  for (const auto& idxDef : schema.indexes) {
    std::map<std::string, long> idxMap;
    size_t fIdx = static_cast<size_t>(-1);
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
    if (!engine.SaveIndex(GetIdxPath(datPath, schema.tableName, idxDef.name), idxMap, err)) return false;
  }
  return true;
}

bool ApplyDeleteAt(StorageEngine& engine, const std::string& datPath, const TableSchema& schema, long offset,
                   const Record& rec, Txn* txn, LogManager* log, LockManager* lock_manager, std::string& err) {
  if (lock_manager && txn) {
    RID rid{schema.tableName, static_cast<uint64_t>(offset)};
    if (!lock_manager->LockExclusive(txn->id, rid, err)) return false;
  }
  std::vector<uint8_t> before;
  if (!engine.SerializeRecord(schema, rec, before, err)) return false;
  LogRecord lr;
  lr.txn_id = txn->id;
  lr.type = LogType::DELETE;
  lr.rid.table_name = schema.tableName;
  lr.rid.file_offset = static_cast<uint64_t>(offset);
  lr.before = before;
  LSN lsn = log->Append(lr, err);
  if (lsn == 0) return false;
  txn->undo_chain.push_back(lsn);
  std::vector<uint8_t> after = before;
  if (!after.empty()) after[0] = 0;
  return engine.WriteRecordBytesAt(datPath, offset, after, err);
}

bool ApplyUpdateAt(StorageEngine& engine, const std::string& datPath, const TableSchema& schema, long offset,
                   const Record& beforeRec, const Record& afterRec,
                   Txn* txn, LogManager* log, LockManager* lock_manager, std::string& err) {
  if (lock_manager && txn) {
    RID rid{schema.tableName, static_cast<uint64_t>(offset)};
    if (!lock_manager->LockExclusive(txn->id, rid, err)) return false;
  }
  std::vector<uint8_t> before;
  std::vector<uint8_t> after;
  if (!engine.SerializeRecord(schema, beforeRec, before, err)) return false;
  if (!engine.SerializeRecord(schema, afterRec, after, err)) return false;
  if (before.size() != after.size()) {
    if (!txn || !log) { err = "Update size mismatch for SET NULL"; return false; }
    // Fallback: represent size-changing update as DELETE + INSERT to keep WAL consistent.
    LogRecord del;
    del.txn_id = txn->id;
    del.type = LogType::DELETE;
    del.rid.table_name = schema.tableName;
    del.rid.file_offset = static_cast<uint64_t>(offset);
    del.before = before;
    LSN delLsn = log->Append(del, err);
    if (delLsn == 0) return false;
    txn->undo_chain.push_back(delLsn);

    long newOffset = 0;
    if (!engine.ComputeAppendRecordOffset(datPath, schema, newOffset, err)) return false;
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
    if (!engine.WriteRecordBytesAt(datPath, offset, tomb, err)) return false;

    long realOffset = 0;
    if (!engine.AppendRecord(datPath, schema, afterRec, realOffset, err)) return false;
    if (realOffset != newOffset) {
      err = "Append offset mismatch for WAL";
      return false;
    }
    return true;
  }
  LogRecord lr;
  lr.txn_id = txn->id;
  lr.type = LogType::UPDATE;
  lr.rid.table_name = schema.tableName;
  lr.rid.file_offset = static_cast<uint64_t>(offset);
  lr.before = before;
  lr.after = after;
  LSN lsn = log->Append(lr, err);
  if (lsn == 0) return false;
  txn->undo_chain.push_back(lsn);
  return engine.WriteRecordBytesAt(datPath, offset, after, err);
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

// moved to top of file

bool DMLService::Insert(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const std::vector<Record>& records, std::string& err,
                        Txn* txn, LogManager* log, LockManager* lock_manager) {
  // Map field names to indices
  std::map<std::string, size_t> fieldMap;
  for(size_t i=0; i<schema.fields.size(); ++i) fieldMap[schema.fields[i].name] = i;

  std::vector<size_t> keyIdxs;
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    if (schema.fields[i].isKey) keyIdxs.push_back(i);
  }
  if (!keyIdxs.empty()) {
    std::unordered_set<std::string> seen;
    std::vector<Record> existing;
    if (!engine_.ReadRecords(datPath, schema, existing, err)) return false;
    for (const auto& r : existing) {
      if (!r.valid) continue;
      seen.insert(BuildCompositeKey(r, keyIdxs));
    }
    for (const auto& r : records) {
      std::string key = BuildCompositeKey(r, keyIdxs);
      if (seen.count(key)) {
        err = "Duplicate entry '" + BuildKeyDisplay(r, keyIdxs) + "' for primary key";
        return false;
      }
      seen.insert(key);
    }
  }

  if (!schema.foreignKeys.empty()) {
    std::map<std::string, TableSchema> schemaCache;
    for (const auto& r : records) {
      for (const auto& fkRaw : schema.foreignKeys) {
        ForeignKeyDef fk = NormalizeForeignKey(fkRaw);
        std::vector<std::string> values;
        bool hasNull = false;
        for (const auto& col : fk.columns) {
          size_t idx = 0;
          if (!FindFieldIndex(schema, col, idx)) { err = "Foreign key column not found: " + col; return false; }
          std::string val = (idx < r.values.size()) ? r.values[idx] : "";
          if (IsNullValue(val)) { hasNull = true; break; }
          values.push_back(val);
        }
        if (hasNull) continue;
        TableSchema refSchema;
        if (!LoadSchemaCached(engine_, dbfPath, fk.refTable, schemaCache, refSchema, err)) return false;
        std::vector<std::string> refCols = ResolveRefColumns(refSchema, fk);
        if (!FindReferencedRecord(engine_, datPath, refSchema, refCols, values, err)) {
          err = "Foreign key constraint fails on table '" + schema.tableName + "'";
          return false;
        }
      }
    }
  }

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

  if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;

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

bool DMLService::Delete(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema,
                        const std::vector<Condition>& conditions, ReferentialAction action, bool actionSpecified,
                        std::string& err, Txn* txn, LogManager* log, LockManager* lock_manager) {
  std::vector<TableSchema> allSchemas;
  if (!engine_.LoadSchemas(dbfPath, allSchemas, err)) return false;

  auto applyConstraints = [&](const TableSchema& parentSchema, const Record& parentRec,
                              bool useOverride, ReferentialAction overrideAction,
                              const auto& self) -> bool {
    for (const auto& childSchema : allSchemas) {
      for (const auto& fkRaw : childSchema.foreignKeys) {
        ForeignKeyDef fk = NormalizeForeignKey(fkRaw);
        if (Lower(fk.refTable) != Lower(parentSchema.tableName)) continue;
        ReferentialAction act = useOverride ? overrideAction : fk.onDelete;
        std::vector<size_t> parentIdxs;
        std::vector<size_t> childIdxs;
        std::vector<std::string> refCols = ResolveRefColumns(parentSchema, fk);
        if (refCols.size() != fk.columns.size()) { err = "Foreign key column mismatch"; return false; }
        for (const auto& col : refCols) {
          size_t idx = 0;
          if (!FindFieldIndex(parentSchema, col, idx)) { err = "Referenced column not found: " + col; return false; }
          parentIdxs.push_back(idx);
        }
        for (const auto& col : fk.columns) {
          size_t idx = 0;
          if (!FindFieldIndex(childSchema, col, idx)) { err = "Foreign key column not found: " + col; return false; }
          childIdxs.push_back(idx);
        }
        if (txn && log) {
          std::vector<std::pair<long, Record>> childRecords;
          if (!engine_.ReadRecordsWithOffsets(datPath, childSchema, childRecords, err)) return false;
          for (const auto& p : childRecords) {
            const Record& r = p.second;
            if (!r.valid) continue;
            bool match = true;
            for (size_t i = 0; i < childIdxs.size(); ++i) {
              std::string cval = (childIdxs[i] < r.values.size()) ? r.values[childIdxs[i]] : "";
              if (IsNullValue(cval)) { match = false; break; }
              std::string pval = (parentIdxs[i] < parentRec.values.size()) ? parentRec.values[parentIdxs[i]] : "";
              if (NormalizeScalar(cval) != NormalizeScalar(pval)) { match = false; break; }
            }
            if (!match) continue;
            if (act == ReferentialAction::kSetNull) {
              for (const auto& col : fk.columns) {
                if (!IsNullableColumn(childSchema, col)) {
                  err = "SET NULL not allowed for non-nullable column: " + col;
                  return false;
                }
              }
            }
            if (act == ReferentialAction::kRestrict) {
              err = "Delete restricted by foreign key";
              return false;
            }
            if (act == ReferentialAction::kCascade) {
              if (!self(childSchema, r, false, overrideAction, self)) return false;
              if (!ApplyDeleteAt(engine_, datPath, childSchema, p.first, r, txn, log, lock_manager, err)) return false;
              AddTouchedTable(txn, childSchema.tableName);
            } else if (act == ReferentialAction::kSetNull) {
              Record updated = r;
              for (size_t idx : childIdxs) {
                if (idx < updated.values.size()) updated.values[idx] = "NULL";
              }
              if (!ApplyUpdateAt(engine_, datPath, childSchema, p.first, r, updated, txn, log, lock_manager, err)) return false;
              AddTouchedTable(txn, childSchema.tableName);
            }
          }
        } else {
          std::vector<Record> childRecords;
          if (!engine_.ReadRecords(datPath, childSchema, childRecords, err)) return false;
          bool changed = false;
          for (auto& r : childRecords) {
            if (!r.valid) continue;
            bool match = true;
            for (size_t i = 0; i < childIdxs.size(); ++i) {
              std::string cval = (childIdxs[i] < r.values.size()) ? r.values[childIdxs[i]] : "";
              if (IsNullValue(cval)) { match = false; break; }
              std::string pval = (parentIdxs[i] < parentRec.values.size()) ? parentRec.values[parentIdxs[i]] : "";
              if (NormalizeValue(cval) != NormalizeValue(pval)) { match = false; break; }
            }
            if (!match) continue;
            if (act == ReferentialAction::kSetNull) {
              for (const auto& col : fk.columns) {
                if (!IsNullableColumn(childSchema, col)) {
                  err = "SET NULL not allowed for non-nullable column: " + col;
                  return false;
                }
              }
            }
            if (act == ReferentialAction::kRestrict) {
              err = "Delete restricted by foreign key";
              return false;
            }
            if (act == ReferentialAction::kCascade) {
              if (!self(childSchema, r, false, overrideAction, self)) return false;
              r.valid = false;
              changed = true;
            } else if (act == ReferentialAction::kSetNull) {
              for (size_t idx : childIdxs) {
                if (idx < r.values.size()) r.values[idx] = "NULL";
              }
              changed = true;
            }
          }
          if (changed) {
            if (!engine_.SaveRecords(datPath, childSchema, childRecords, err)) return false;
            if (!RebuildIndexesForTable(engine_, datPath, childSchema, err)) return false;
          }
        }
      }
    }
    return true;
  };

  if (txn && log) {
    std::vector<std::pair<long, Record>> records;
    if (!engine_.ReadRecordsWithOffsets(datPath, schema, records, err)) return false;
    bool hit = false;
    for (const auto& p : records) {
      if (!Match(schema, p.second, conditions)) continue;
      hit = true;
      if (!applyConstraints(schema, p.second, actionSpecified, action, applyConstraints)) return false;
      if (!ApplyDeleteAt(engine_, datPath, schema, p.first, p.second, txn, log, lock_manager, err)) return false;
      AddTouchedTable(txn, schema.tableName);
    }
    if (!hit) { err = "No record matched"; return false; }
    return true;
  }

  std::vector<Record> records;
  if (!engine_.ReadRecords(datPath, schema, records, err)) return false;
  bool hit = false;
  for (auto& r : records) {
    if (!Match(schema, r, conditions)) continue;
    hit = true;
    if (!applyConstraints(schema, r, actionSpecified, action, applyConstraints)) return false;
    r.valid = false;
  }
  if (!hit) { err = "No record matched"; return false; }
  if (!engine_.SaveRecords(datPath, schema, records, err)) return false;
  if (!RebuildIndexesForTable(engine_, datPath, schema, err)) return false;
  return true;
}

bool DMLService::Update(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const std::vector<Condition>& conditions,
                        const std::vector<std::pair<std::string, std::string>>& assignments, std::string& err,
                        Txn* txn, LogManager* log, LockManager* lock_manager) {
  std::map<std::string, TableSchema> schemaCache;
  auto applyAssignments = [&](const Record& src) {
    Record updated = src;
    for (const auto& kv : assignments) {
      auto it = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f) { return f.name == kv.first; });
      if (it == schema.fields.end()) continue;
      size_t idx = static_cast<size_t>(std::distance(schema.fields.begin(), it));
      if (idx < updated.values.size()) updated.values[idx] = kv.second;
    }
    return updated;
  };

  auto checkForeignKeys = [&](const Record& updated) -> bool {
    for (const auto& fkRaw : schema.foreignKeys) {
      ForeignKeyDef fk = NormalizeForeignKey(fkRaw);
      bool touches = false;
      for (const auto& kv : assignments) {
        for (const auto& col : fk.columns) {
          if (Lower(kv.first) == Lower(col)) touches = true;
        }
      }
      if (!touches) continue;
      std::vector<std::string> values;
      bool hasNull = false;
      for (const auto& col : fk.columns) {
        size_t idx = 0;
        if (!FindFieldIndex(schema, col, idx)) { err = "Foreign key column not found: " + col; return false; }
        std::string val = (idx < updated.values.size()) ? updated.values[idx] : "";
        if (IsNullValue(val)) { hasNull = true; break; }
        values.push_back(val);
      }
      if (hasNull) continue;
      TableSchema refSchema;
      if (!LoadSchemaCached(engine_, dbfPath, fk.refTable, schemaCache, refSchema, err)) return false;
      std::vector<std::string> refCols = ResolveRefColumns(refSchema, fk);
      if (!FindReferencedRecord(engine_, datPath, refSchema, refCols, values, err)) {
        err = "Foreign key constraint fails on table '" + schema.tableName + "'";
        return false;
      }
    }
    return true;
  };

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
          Record updated = applyAssignments(p.second);
          if (!checkForeignKeys(updated)) return false;

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
    Record updated = applyAssignments(r);
    if (!checkForeignKeys(updated)) return false;
    r = updated;
  }
  if (!hit) err = "No record matched";
  if (!engine_.SaveRecords(datPath, schema, records, err)) return false;

  if (!schema.indexes.empty()) {
      std::vector<std::pair<long, Record>> newRecords;
      if (!engine_.ReadRecordsWithOffsets(datPath, schema, newRecords, err)) return false;

      if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;
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

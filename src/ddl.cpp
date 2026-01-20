#include "ddl.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <map>
#include <set>
#include <cstdio>
#include "path_utils.h"
#include "parser.h"

namespace {
std::string NormalizeValue(std::string s) {
    if (s.size() >= 2) {
        if ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

std::string Lower(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::string Trim(std::string s) {
    auto notSpace = [](unsigned char ch) { return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

bool FindFieldIndex(const TableSchema& schema, const std::string& name, size_t& outIdx) {
    std::string low = Lower(name);
    for (size_t i = 0; i < schema.fields.size(); ++i) {
        if (Lower(schema.fields[i].name) == low) { outIdx = i; return true; }
    }
    return false;
}

std::string StripIdentQuotes(std::string s) {
    s = Trim(s);
    if (s.size() >= 2) {
        char f = s.front();
        char b = s.back();
        if ((f == '`' && b == '`') || (f == '"' && b == '"') || (f == '\'' && b == '\'')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

std::vector<std::string> ResolveRefColumns(const TableSchema& refSchema, const ForeignKeyDef& fk);
void NormalizeForeignKey(ForeignKeyDef& fk);

bool IsNullableColumn(const TableSchema& schema, const std::string& name) {
    size_t idx = 0;
    if (!FindFieldIndex(schema, name, idx)) return false;
    return schema.fields[idx].nullable;
}

bool AreForeignKeysEquivalent(const ForeignKeyDef& left, const ForeignKeyDef& right) {
    if (Lower(left.refTable) != Lower(right.refTable)) return false;
    if (left.onDelete != right.onDelete || left.onUpdate != right.onUpdate) return false;
    if (left.columns.size() != right.columns.size()) return false;
    if (left.refColumns.size() != right.refColumns.size()) return false;
    for (size_t i = 0; i < left.columns.size(); ++i) {
        if (Lower(left.columns[i]) != Lower(right.columns[i])) return false;
    }
    for (size_t i = 0; i < left.refColumns.size(); ++i) {
        if (Lower(left.refColumns[i]) != Lower(right.refColumns[i])) return false;
    }
    return true;
}

bool HasUniqueRef(const TableSchema& refSchema, const std::vector<std::string>& refCols) {
    if (refCols.size() == 1) {
        const std::string& col = refCols[0];
        for (const auto& f : refSchema.fields) {
            if (Lower(f.name) == Lower(col) && f.isKey) return true;
        }
        for (const auto& idx : refSchema.indexes) {
            if (Lower(idx.fieldName) == Lower(col) && idx.isUnique) return true;
        }
        return false;
    }
    size_t keyCount = 0;
    for (const auto& f : refSchema.fields) if (f.isKey) keyCount++;
    if (keyCount != refCols.size()) return false;
    for (const auto& col : refCols) {
        size_t idx = 0;
        if (!FindFieldIndex(refSchema, col, idx)) return false;
        if (!refSchema.fields[idx].isKey) return false;
    }
    return true;
}

bool ValidateForeignKeyDef(const std::vector<TableSchema>& schemas, const TableSchema& tableSchema, ForeignKeyDef& fk, std::string& err) {
    NormalizeForeignKey(fk);
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s) { return Lower(s.tableName) == Lower(fk.refTable); });
    if (it == schemas.end()) { err = "Referenced table not found: " + fk.refTable; return false; }
    const TableSchema& refSchema = *it;

    if (fk.columns.empty()) { err = "Foreign key missing columns"; return false; }
    fk.refColumns = ResolveRefColumns(refSchema, fk);
    if (fk.columns.size() != fk.refColumns.size()) { err = "Foreign key column count mismatch"; return false; }

    for (const auto& col : fk.columns) {
        size_t idx = 0;
        if (!FindFieldIndex(tableSchema, col, idx)) { err = "Foreign key column not found: " + col; return false; }
    }
    for (const auto& col : fk.refColumns) {
        size_t idx = 0;
        if (!FindFieldIndex(refSchema, col, idx)) { err = "Referenced column not found: " + col; return false; }
    }
    for (size_t i = 0; i < fk.columns.size(); ++i) {
        size_t idxChild = 0;
        size_t idxRef = 0;
        FindFieldIndex(tableSchema, fk.columns[i], idxChild);
        FindFieldIndex(refSchema, fk.refColumns[i], idxRef);
        if (Lower(tableSchema.fields[idxChild].type) != Lower(refSchema.fields[idxRef].type)) {
            err = "Foreign key type mismatch on column: " + fk.columns[i];
            return false;
        }
    }
    if (!HasUniqueRef(refSchema, fk.refColumns)) {
        err = "Referenced columns must be unique or primary key";
        return false;
    }
    return true;
}

bool ExistingDataSatisfiesFk(StorageEngine& engine, const std::string& datPath,
                             const TableSchema& tableSchema, const ForeignKeyDef& fk,
                             const TableSchema& refSchema, std::string& err) {
    std::vector<Record> records;
    if (!engine.ReadRecords(datPath, tableSchema, records, err)) return false;
    std::vector<Record> refRecords;
    if (!engine.ReadRecords(datPath, refSchema, refRecords, err)) return false;
    std::vector<size_t> childIdxs;
    std::vector<size_t> refIdxs;
    for (const auto& col : fk.columns) {
        size_t idx = 0;
        if (!FindFieldIndex(tableSchema, col, idx)) return false;
        childIdxs.push_back(idx);
    }
    std::vector<std::string> refCols = ResolveRefColumns(refSchema, fk);
    for (const auto& col : refCols) {
        size_t idx = 0;
        if (!FindFieldIndex(refSchema, col, idx)) return false;
        refIdxs.push_back(idx);
    }
    for (const auto& r : records) {
        if (!r.valid) continue;
        bool hasNull = false;
        std::vector<std::string> values;
        for (size_t idx : childIdxs) {
            std::string v = (idx < r.values.size()) ? NormalizeValue(r.values[idx]) : "";
            if (v.empty() || Lower(v) == "null") { hasNull = true; break; }
            values.push_back(v);
        }
        if (hasNull) continue;
        bool found = false;
        for (const auto& rr : refRecords) {
            if (!rr.valid) continue;
            bool match = true;
            for (size_t i = 0; i < refIdxs.size(); ++i) {
                std::string v = (refIdxs[i] < rr.values.size()) ? NormalizeValue(rr.values[refIdxs[i]]) : "";
                if (v != values[i]) { match = false; break; }
            }
            if (match) { found = true; break; }
        }
        if (!found) {
            err = "Existing data violates foreign key constraint";
            return false;
        }
    }
    return true;
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

void NormalizeForeignKey(ForeignKeyDef& fk) {
    fk.name = StripIdentQuotes(fk.name);
    fk.refTable = StripIdentQuotes(fk.refTable);
    size_t lp = fk.refTable.find('(');
    if (lp != std::string::npos) fk.refTable = Trim(fk.refTable.substr(0, lp));
    for (auto& c : fk.columns) c = StripIdentQuotes(c);
    for (auto& c : fk.refColumns) c = StripIdentQuotes(c);
}

bool SchemaByName(const std::vector<TableSchema>& schemas, const std::string& name, TableSchema& out) {
    std::string low = Lower(name);
    for (const auto& s : schemas) {
        if (Lower(s.tableName) == low) { out = s; return true; }
    }
    return false;
}

bool FieldExistsInSchema(const TableSchema& schema, const std::string& name) {
    std::string low = Lower(name);
    for (const auto& f : schema.fields) {
        if (Lower(f.name) == low) return true;
        size_t dot = f.name.find('.');
        if (dot != std::string::npos && Lower(f.name.substr(dot + 1)) == low) return true;
    }
    return false;
}

TableSchema BuildCombinedSchema(const TableSchema& left, const std::string& leftAlias,
                                const TableSchema* right, const std::string& rightAlias,
                                bool naturalJoin) {
    TableSchema combined;
    auto addWithAlias = [&](const TableSchema& s, const std::string& alias) {
        std::string prefix = alias.empty() ? s.tableName : alias;
        for (const auto& f : s.fields) {
            Field nf = f;
            nf.name = prefix.empty() ? f.name : (prefix + "." + f.name);
            combined.fields.push_back(nf);
        }
    };
    addWithAlias(left, leftAlias);
    if (right) addWithAlias(*right, rightAlias);

    if (naturalJoin) {
        std::set<std::string> seen;
        std::vector<Field> dedup;
        for (const auto& f : combined.fields) {
            std::string base = Lower(f.name);
            size_t dot = base.rfind('.');
            if (dot != std::string::npos) base = base.substr(dot + 1);
            if (seen.insert(base).second) dedup.push_back(f);
        }
        combined.fields = dedup;
    }
    return combined;
}

bool ValidateViewPlan(const QueryPlan& plan, const std::vector<TableSchema>& schemas,
                      std::set<std::string>& visiting, std::string& err);

bool ValidateSubQueries(const QueryPlan& plan, const std::vector<TableSchema>& schemas,
                        std::set<std::string>& visiting, std::string& err) {
    for (const auto& c : plan.conditions) {
        if (c.isSubQuery && c.subQueryPlan) {
            if (!ValidateViewPlan(*c.subQueryPlan, schemas, visiting, err)) return false;
        }
    }
    for (const auto& c : plan.havingConditions) {
        if (c.isSubQuery && c.subQueryPlan) {
            if (!ValidateViewPlan(*c.subQueryPlan, schemas, visiting, err)) return false;
        }
    }
    for (const auto& s : plan.selectExprs) {
        if (s.isSubQuery && s.subQueryPlan) {
            if (!ValidateViewPlan(*s.subQueryPlan, schemas, visiting, err)) return false;
        }
    }
    return true;
}

bool ValidateViewPlan(const QueryPlan& plan, const std::vector<TableSchema>& schemas,
                      std::set<std::string>& visiting, std::string& err) {
    TableSchema base;
    if (!plan.sourceTable.empty()) {
        if (!SchemaByName(schemas, plan.sourceTable, base)) { err = "Referenced table/view not found: " + plan.sourceTable; return false; }
        if (base.isView) {
            std::string low = Lower(base.tableName);
            if (visiting.count(low)) { err = "Recursive view detected: " + base.tableName; return false; }
            visiting.insert(low);
            Parser p;
            std::string perr;
            ParsedCommand pc = p.Parse(base.viewSql, perr);
            if (!perr.empty() || pc.type != CommandType::kSelect) { err = "Invalid stored view definition for " + base.tableName; return false; }
            if (!ValidateViewPlan(pc.query, schemas, visiting, err)) return false;
            visiting.erase(low);
        }
    } else if (plan.sourceSubQuery) {
        if (!ValidateViewPlan(*plan.sourceSubQuery, schemas, visiting, err)) return false;
    } else {
        err = "Invalid view source";
        return false;
    }

    if (!plan.joinTable.empty()) {
        TableSchema right;
        if (!SchemaByName(schemas, plan.joinTable, right)) { err = "Join table/view not found: " + plan.joinTable; return false; }
        if (right.isView) {
            std::string low = Lower(right.tableName);
            if (visiting.count(low)) { err = "Recursive view detected: " + right.tableName; return false; }
            visiting.insert(low);
            Parser p;
            std::string perr;
            ParsedCommand pc = p.Parse(right.viewSql, perr);
            if (!perr.empty() || pc.type != CommandType::kSelect) { err = "Invalid stored view definition for " + right.tableName; return false; }
            if (!ValidateViewPlan(pc.query, schemas, visiting, err)) return false;
            visiting.erase(low);
        }
    }

    return ValidateSubQueries(plan, schemas, visiting, err);
}

bool DeriveViewFields(const QueryPlan& plan, const std::vector<TableSchema>& schemas,
                      std::vector<Field>& outFields, std::string& err) {
    outFields.clear();
    TableSchema left;
    if (!plan.sourceTable.empty()) {
        if (!SchemaByName(schemas, plan.sourceTable, left)) { err = "Table/view not found: " + plan.sourceTable; return false; }
    } else if (plan.sourceSubQuery) {
        std::vector<Field> innerFields;
        if (!DeriveViewFields(*plan.sourceSubQuery, schemas, innerFields, err)) return false;
        left.tableName = plan.sourceAlias.empty() ? "Derived" : plan.sourceAlias;
        left.fields = innerFields;
    } else {
        err = "Invalid view definition (missing source)";
        return false;
    }

    TableSchema right;
    TableSchema* rightPtr = nullptr;
    if (!plan.joinTable.empty()) {
        if (!SchemaByName(schemas, plan.joinTable, right)) { err = "Join target not found: " + plan.joinTable; return false; }
        rightPtr = &right;
    }

    TableSchema combined = BuildCombinedSchema(left, plan.tableAlias, rightPtr, plan.joinTableAlias, plan.isNaturalJoin);

    size_t exprIdx = 0;
    for (const auto& sel : plan.selectExprs) {
        if (sel.isAggregate) {
            Field f;
            f.name = !sel.alias.empty() ? sel.alias : (sel.agg.func + "(" + sel.agg.field + ")");
            f.type = "string";
            outFields.push_back(f);
            continue;
        }
        if (sel.isSubQuery) {
            Field f;
            f.name = !sel.alias.empty() ? sel.alias : ("subquery_" + std::to_string(exprIdx));
            f.type = "string";
            outFields.push_back(f);
            ++exprIdx;
            continue;
        }
        std::string fieldName = sel.field;
        if (fieldName == "*") {
            for (const auto& f : combined.fields) {
                Field nf = f;
                nf.isKey = false;
                nf.nullable = true;
                size_t dot = nf.name.rfind('.');
                if (dot != std::string::npos) nf.name = nf.name.substr(dot + 1);
                outFields.push_back(nf);
            }
            ++exprIdx;
            continue;
        }
        if (!FieldExistsInSchema(combined, fieldName)) {
            err = "Column not found in view definition: " + fieldName;
            return false;
        }
        Field f;
        if (!sel.alias.empty()) {
            f.name = sel.alias;
        } else {
            size_t dot = fieldName.rfind('.');
            f.name = (dot == std::string::npos) ? fieldName : fieldName.substr(dot + 1);
        }
        f.type = "string";
        outFields.push_back(f);
        ++exprIdx;
    }
    return true;
}
}

std::string DDLService::GetIndexPath(const std::string& datPath, const std::string& tableName, const std::string& fieldName) {
    return dbms_paths::IndexPathFromDat(datPath, tableName, fieldName);
}

bool DDLService::CreateTable(const std::string& dbfPath, const std::string& datPath, const TableSchema& schema, std::string& err) {
  std::vector<TableSchema> schemas;
  engine_.LoadSchemas(dbfPath, schemas, err);  // treat missing file as new db
  auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s) { return s.tableName == schema.tableName; });
  if (it != schemas.end()) {
    err = "Table already exists";
    return false;
  }
  
  // Auto-index Primary Keys
  TableSchema finalSchema = schema;
  for(const auto& f : finalSchema.fields) {
      if(f.isKey) {
          // Check if already in indexes
          auto it = std::find_if(finalSchema.indexes.begin(), finalSchema.indexes.end(), [&](const IndexDef& idx){ return idx.fieldName == f.name; });
          if (it == finalSchema.indexes.end()) {
              finalSchema.indexes.push_back({ "PRIMARY", f.name, true });
          }
      }
  }

  // Validate foreign keys
  for (size_t i = 0; i < finalSchema.foreignKeys.size(); ++i) {
      ForeignKeyDef& fk = finalSchema.foreignKeys[i];
      if (fk.name.empty()) {
          fk.name = "fk_" + finalSchema.tableName + "_" + fk.refTable + "_" + std::to_string(i + 1);
      }
      if (!ValidateForeignKeyDef(schemas, finalSchema, fk, err)) return false;
      for (size_t j = 0; j < i; ++j) {
          if (Lower(finalSchema.foreignKeys[j].name) == Lower(fk.name)) {
              err = "Duplicate foreign key name: " + fk.name;
              return false;
          }
      }
  }

  if (!engine_.AppendSchema(dbfPath, finalSchema, err)) return false;

  // Initialize dat file with zero records
  std::vector<Record> empty;
  if (!engine_.SaveRecords(datPath, finalSchema, empty, err)) return false;

  // Create empty index files for all indexes
  if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;
  for(const auto& idx : finalSchema.indexes) {
      std::string idxPath = GetIndexPath(datPath, finalSchema.tableName, idx.fieldName);
      std::map<std::string, long> emptyMap;
      if (!engine_.SaveIndex(idxPath, emptyMap, err)) return false;
  }

  return true;
}

bool DDLService::RenameTable(const std::string& dbfPath, const std::string& datPath, const std::string& oldName, const std::string& newName, std::string& err) {
  // Rename implies renaming index files too?
  // For now, keeping implementation simple (Load/Save schemas).
  // Ideally should rename .idx files.
  // ... (Existing implementation kept, but careful about file moves)
  
  std::vector<TableSchema> schemas;
  if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
  
  TableSchema* target = nullptr;
  for (auto& s : schemas) {
    if (s.tableName == oldName) {
      s.tableName = newName;
      target = &s;
    }
  }
  for (auto& s : schemas) {
    for (auto& fk : s.foreignKeys) {
      if (Lower(fk.refTable) == Lower(oldName)) fk.refTable = newName;
    }
  }
  if (!target) {
    err = "Table not found";
    return false;
  }
  
  // Move index files
  for(const auto& idx : target->indexes) {
      std::string oldP = GetIndexPath(datPath, oldName, idx.fieldName);
      std::string newP = GetIndexPath(datPath, newName, idx.fieldName);
      std::rename(oldP.c_str(), newP.c_str());
  }

  if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;

  // Move data: read old table data, write back under new name
  TableSchema searchSchema = *target;
  searchSchema.tableName = oldName; 

  std::vector<Record> records;
  // Try to read old records. Note: this relies on StorageEngine not strictly validating table name 
  // if we can trick it, or we accept that Rename is currently destructive for data in this simple impl.
  // Given the current architecture, keeping Rename simple (metadata only + index move) is safest.
  if (!engine_.ReadRecords(datPath, searchSchema, records, err)) {
       records.clear();
  }
  
  // Write back with new table name
  if (!engine_.SaveRecords(datPath, *target, records, err)) return false; 
  
  return true;
}

bool DDLService::CreateIndex(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& fieldName, const std::string& indexName, bool isUnique, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;

    // Check if field exists
    auto fit = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f){ return f.name == fieldName; });
    if (fit == schema.fields.end()) { err="Field not found"; return false; }
    size_t valIndex = static_cast<size_t>(std::distance(schema.fields.begin(), fit));

    // Check if already indexed
    auto idxIt = std::find_if(schema.indexes.begin(), schema.indexes.end(),
                              [&](const IndexDef& d){ return d.fieldName == fieldName; });
    if (idxIt != schema.indexes.end()) {
        // If a unique index already exists on this field (e.g., PRIMARY), treat as no-op.
        if (isUnique && idxIt->isUnique) return true;
        err = "Index already exists on this field";
        return false;
    }
    
    // Check data for uniqueness if requested
    std::vector<std::pair<long, Record>> records;
    if (!engine_.ReadRecordsWithOffsets(datPath, schema, records, err)) return false;

    if (isUnique) {
        std::map<std::string, int> counts;
        for(const auto& p : records) {
             if(valIndex < p.second.values.size()) {
                 std::string val = NormalizeValue(p.second.values[valIndex]);
                 counts[val]++;
                 if(counts[val] > 1) {
                     err = "Duplicate values found, cannot create unique index: " + val;
                     return false;
                 }
             }
        }
    }

    IndexDef newIdx;
    newIdx.name = indexName.empty() ? ("idx_" + fieldName) : indexName;
    newIdx.fieldName = fieldName;
    newIdx.isUnique = isUnique;

    schema.indexes.push_back(newIdx);
    if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;

    std::map<std::string, long> idxMap;
    for(const auto& p : records) {
        if(valIndex < p.second.values.size()) {
            std::string val = NormalizeValue(p.second.values[valIndex]);
            idxMap[val] = p.first;
        }
    }

    if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;
    return engine_.SaveIndex(GetIndexPath(datPath, tableName, newIdx.name), idxMap, err);
}

bool DDLService::DropIndex(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& indexName, std::string& err) {
    // Similar to CreateIndex but remove from schema and delete file
     std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    
    // indexName passed here corresponds to what parser calls fieldName (reused field)
    auto iit = std::find_if(it->indexes.begin(), it->indexes.end(), [&](const IndexDef& d){ return d.name == indexName; });
    if (iit == it->indexes.end()) { err = "Index not found"; return false; }
    
    // Capture name before erase if we need valid reference? No iterator is fine. 
    // Wait, GetIndexPath uses string.
    std::string actualName = iit->name; 
    
    it->indexes.erase(iit);
    if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;

    // Delete file
    std::string idxPath = GetIndexPath(datPath, tableName, actualName);
    std::remove(idxPath.c_str());
    return true;
}

bool DDLService::ListIndexes(const std::string& dbfPath, const std::string& tableName, std::vector<IndexDef>& outIndexes, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    
    outIndexes = it->indexes;
    return true;
}

bool DDLService::RebuildIndexes(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, std::string& err) {
     std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;

    if (schema.indexes.empty()) return true;

    std::vector<std::pair<long, Record>> records;
    if (!engine_.ReadRecordsWithOffsets(datPath, schema, records, err)) return false;

    if (!dbms_paths::EnsureIndexDirFromDat(datPath, err)) return false;
    for (const auto& idxDef : schema.indexes) {
         auto fit = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f){ return f.name == idxDef.fieldName; });
         if (fit == schema.fields.end()) continue;
         size_t valIndex = static_cast<size_t>(std::distance(schema.fields.begin(), fit));

         std::map<std::string, long> idxMap;
         for(const auto& p : records) {
            if(valIndex < p.second.values.size()) {
                std::string val = NormalizeValue(p.second.values[valIndex]);
                idxMap[val] = p.first;
            }
         }
         engine_.SaveIndex(GetIndexPath(datPath, tableName, idxDef.name), idxMap, err);
    }
    return true;
}

bool DDLService::AddForeignKey(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, ForeignKeyDef fk, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return Lower(s.tableName) == Lower(tableName); });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;

    NormalizeForeignKey(fk);
    if (fk.name.empty()) {
        fk.name = "fk_" + schema.tableName + "_" + fk.refTable + "_" + std::to_string(schema.foreignKeys.size() + 1);
    }
    if (!ValidateForeignKeyDef(schemas, schema, fk, err)) return false;
    for (const auto& existing : schema.foreignKeys) {
        ForeignKeyDef normalized = existing;
        NormalizeForeignKey(normalized);
        if (Lower(normalized.name) == Lower(fk.name)) {
            if (AreForeignKeysEquivalent(normalized, fk)) return true;
            err = "Foreign key already exists";
            return false;
        }
    }
    {
        auto refIt = std::find_if(schemas.begin(), schemas.end(),
                                  [&](const TableSchema& s){ return Lower(s.tableName) == Lower(fk.refTable); });
        if (refIt == schemas.end()) { err = "Referenced table not found: " + fk.refTable; return false; }
        if (!ExistingDataSatisfiesFk(engine_, datPath, schema, fk, *refIt, err)) return false;
    }
    schema.foreignKeys.push_back(fk);
    return engine_.SaveSchemas(dbfPath, schemas, err);
}

bool DDLService::DropForeignKey(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& fkName, std::string& err) {
    (void)datPath;
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return Lower(s.tableName) == Lower(tableName); });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;
    auto fit = std::find_if(schema.foreignKeys.begin(), schema.foreignKeys.end(),
                            [&](const ForeignKeyDef& fk){ return Lower(fk.name) == Lower(fkName); });
    if (fit == schema.foreignKeys.end()) { err = "Foreign key not found"; return false; }
    schema.foreignKeys.erase(fit);
    return engine_.SaveSchemas(dbfPath, schemas, err);
}

bool DDLService::CreateView(const std::string& dbfPath, const std::string& datPath, const std::string& viewName,
                            const std::string& viewSql, const QueryPlan& plan,
                            const std::vector<std::string>& columnNames, bool orReplace, std::string& err) {
    (void)datPath;
    if (viewName.empty()) { err = "View name is required"; return false; }
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;

    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return Lower(s.tableName) == Lower(viewName); });
    if (it != schemas.end()) {
        if (!it->isView) {
            err = "A table with the same name already exists";
            return false;
        }
        if (!orReplace) { err = "View already exists"; return false; }
        schemas.erase(it);
    }

    std::set<std::string> visiting;
    visiting.insert(Lower(viewName));
    if (!ValidateViewPlan(plan, schemas, visiting, err)) return false;

    std::vector<Field> fields;
    if (!DeriveViewFields(plan, schemas, fields, err)) return false;

    if (!columnNames.empty()) {
        if (columnNames.size() != fields.size()) {
            err = "Column list size does not match SELECT list";
            return false;
        }
        for (size_t i = 0; i < fields.size(); ++i) fields[i].name = StripIdentQuotes(columnNames[i]);
    }

    TableSchema view;
    view.tableName = viewName;
    view.fields = fields;
    view.isView = true;
    view.viewSql = viewSql;

    schemas.push_back(view);
    return engine_.SaveSchemas(dbfPath, schemas, err);
}

bool DDLService::DropView(const std::string& dbfPath, const std::string& datPath, const std::string& viewName, bool ifExists, std::string& err) {
    (void)datPath;
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return Lower(s.tableName) == Lower(viewName); });
    if (it == schemas.end() || !it->isView) {
        if (ifExists) return true;
        err = "View not found";
        return false;
    }
    schemas.erase(it);
    return engine_.SaveSchemas(dbfPath, schemas, err);
}

bool DDLService::DropTable(const std::string& dbfPath, const std::string& datPath, const std::string& tableName,
                           ReferentialAction action, std::string& err) {
  std::vector<TableSchema> schemas;
  if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;

  auto it = std::find_if(schemas.begin(), schemas.end(),
                         [&](const TableSchema& s) { return Lower(s.tableName) == Lower(tableName); });
  if (it == schemas.end()) {
    err = "Table not found";
    return false;
  }
  if (it->isView) {
    err = "Use DROP VIEW to remove a view";
    return false;
  }

  // Enforce referential actions for tables that reference this one
  for (auto& s : schemas) {
    bool changed = false;
    for (auto fkIt = s.foreignKeys.begin(); fkIt != s.foreignKeys.end();) {
      if (Lower(fkIt->refTable) != Lower(tableName)) { ++fkIt; continue; }
      if (action == ReferentialAction::kRestrict) {
        err = "Drop restricted by foreign key: " + s.tableName;
        return false;
      }
      if (action == ReferentialAction::kSetNull) {
        for (const auto& col : fkIt->columns) {
          if (!IsNullableColumn(s, col)) {
            err = "SET NULL not allowed for non-nullable column: " + col;
            return false;
          }
        }
      }
      std::vector<Record> records;
      if (!engine_.ReadRecords(datPath, s, records, err)) return false;
      for (auto& r : records) {
        if (!r.valid) continue;
        bool hasRef = false;
        for (const auto& col : fkIt->columns) {
          size_t idx = 0;
          if (!FindFieldIndex(s, col, idx)) continue;
          if (idx < r.values.size()) {
            std::string v = NormalizeValue(r.values[idx]);
            if (!v.empty() && Lower(v) != "null") {
              hasRef = true;
              break;
            }
          }
        }
        if (!hasRef) continue;
        if (action == ReferentialAction::kCascade) {
          r.valid = false;
          changed = true;
        } else if (action == ReferentialAction::kSetNull) {
          for (const auto& col : fkIt->columns) {
            size_t idx = 0;
            if (FindFieldIndex(s, col, idx) && idx < r.values.size()) r.values[idx] = "NULL";
          }
          changed = true;
        }
      }
      if (changed) {
        if (!engine_.SaveRecords(datPath, s, records, err)) return false;
        if (!RebuildIndexes(dbfPath, datPath, s.tableName, err)) return false;
      }
      fkIt = s.foreignKeys.erase(fkIt);
    }
  }

  // Remove associated index files
  for (const auto& idx : it->indexes) {
    std::string idxPath = GetIndexPath(datPath, tableName, idx.name);
    std::remove(idxPath.c_str());
  }

  auto oldSize = schemas.size();
  schemas.erase(std::remove_if(schemas.begin(), schemas.end(),
                               [&](const TableSchema& s) { return Lower(s.tableName) == Lower(tableName); }),
                schemas.end());
  if (schemas.size() == oldSize) {
    err = "Table not found";
    return false;
  }
  if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;

  // Rewrite dat, omitting the dropped table data
  if (schemas.empty()) {
    std::ofstream ofs(datPath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
      err = "Cannot open dat for drop";
      return false;
    }
    return true;
  }
  TableSchema& first = schemas[0];
  std::vector<Record> recs;
  if (!engine_.ReadRecords(datPath, first, recs, err)) {
    recs.clear();
  }
  return engine_.SaveRecords(datPath, first, recs, err);
}

bool DDLService::AddColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const Field& newField, const std::string& afterCol, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema oldSchema = *it;
    TableSchema& newSchema = *it;

    for(const auto& f : newSchema.fields) if (f.name == newField.name) { err = "Column exists"; return false; }

    size_t insertPos = newSchema.fields.size();
    if (afterCol == "FIRST") {
        insertPos = 0;
    } else if (!afterCol.empty()) {
        bool found = false;
        for(size_t i=0; i<newSchema.fields.size(); ++i) {
            if (newSchema.fields[i].name == afterCol) {
                insertPos = i + 1;
                found = true;
                break;
            }
        }
        if (!found) { err = "AFTER column not found: " + afterCol; return false; }
    }
    
    newSchema.fields.insert(newSchema.fields.begin() + insertPos, newField);

    std::vector<Record> records;
    // Ignore error here (empty table)
    std::string tmpErr;
    engine_.ReadRecords(datPath, oldSchema, records, tmpErr);

    for(auto& r : records) {
        if (insertPos <= r.values.size()) {
            std::string val = "NULL";
            if (!newField.nullable) val = ""; 
            r.values.insert(r.values.begin() + insertPos, val);
        }
    }

    if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;
    if (!engine_.SaveRecords(datPath, newSchema, records, err)) return false;
    return true;
}

bool DDLService::DropColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& colName, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema oldSchema = *it;
    TableSchema& newSchema = *it;

    size_t colIdx = -1;
    for(size_t i=0; i<newSchema.fields.size(); ++i) {
        if (newSchema.fields[i].name == colName) { colIdx = i; break; }
    }
    if (colIdx == static_cast<size_t>(-1)) { err = "Column not found"; return false; }

    // Remove indexes using this column
    for (auto iit = newSchema.indexes.begin(); iit != newSchema.indexes.end(); ) {
        if (iit->fieldName == colName) {
             std::string path = GetIndexPath(datPath, tableName, iit->name);
             std::remove(path.c_str());
             iit = newSchema.indexes.erase(iit);
        } else {
             ++iit;
        }
    }

    newSchema.fields.erase(newSchema.fields.begin() + colIdx);

    std::vector<Record> records;
    std::string tmpErr;
    engine_.ReadRecords(datPath, oldSchema, records, tmpErr);

    for(auto& r : records) {
        if (colIdx < r.values.size()) {
            r.values.erase(r.values.begin() + colIdx);
        }
    }

    if (!engine_.SaveSchemas(dbfPath, schemas, err)) return false;
    if (!engine_.SaveRecords(datPath, newSchema, records, err)) return false;
    return true;
}

bool DDLService::ModifyColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const Field& newField, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;

    bool found = false;
    for(auto& f : schema.fields) {
        if (f.name == newField.name) {
            f.type = newField.type; // update type
            f.isKey = newField.isKey;
            f.nullable = newField.nullable;
            // Should verify data? Skip for now.
            found = true;
            break;
        }
    }
    if (!found) { err = "Column not found"; return false; }

    return engine_.SaveSchemas(dbfPath, schemas, err);
}

bool DDLService::RenameColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& oldName, const std::string& newName, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
    
    auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == tableName; });
    if (it == schemas.end()) { err = "Table not found"; return false; }
    TableSchema& schema = *it;

    bool found = false;
    for(auto& f : schema.fields) {
        if (f.name == oldName) {
            f.name = newName;
            found = true;
            break;
        }
    }
    if (!found) { err = "Column not found"; return false; }
    
    // Update Constraints/Indexes
    for(auto& idx : schema.indexes) {
        if (idx.fieldName == oldName) {
            idx.fieldName = newName;
            // We do NOT rename the index itself or the file, just the field reference
        }
    }

    return engine_.SaveSchemas(dbfPath, schemas, err);
}

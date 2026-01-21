#include "query.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <string>
#include <map>
#include <set>
#include <functional>
#include "parser.h"
#include "txn/lock_manager.h"
#include "path_utils.h"

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
}

// Helper to get value dynamically, supporting "Table.Column" or just "Column"
static bool GetFieldValue(const TableSchema& schema, const Record& rec, const std::string& fieldName, std::string& outVal) {
    if (fieldName.empty()) return false;
    std::string lowName = Lower(fieldName);
    
    // Strict match first?
    for (size_t i = 0; i < schema.fields.size(); ++i) {
        if (Lower(schema.fields[i].name) == lowName) {
            if(i < rec.values.size()) { outVal = rec.values[i]; return true; }
            return false;
        }
    }
    
    // If fieldName has no '.', try to match suffix? e.g. "id" matches "T1.id"
    if (fieldName.find('.') == std::string::npos) {
         for (size_t i = 0; i < schema.fields.size(); ++i) {
             std::string fName = Lower(schema.fields[i].name);
             size_t dot = fName.find('.');
             if (dot != std::string::npos) {
                 if (fName.substr(dot + 1) == lowName) {
                    if(i < rec.values.size()) { outVal = rec.values[i]; return true; }
                    return false;
                 }
             }
         }
    }
    return false;
}

static bool FieldExists(const TableSchema& schema, const std::string& fieldName) {
    if (fieldName.empty()) return false;
    std::string lowName = Lower(fieldName);
    for (const auto& f : schema.fields) {
        if (Lower(f.name) == lowName) return true;
    }
    if (fieldName.find('.') == std::string::npos) {
        for (const auto& f : schema.fields) {
            std::string fName = Lower(f.name);
            size_t dot = fName.find('.');
            if (dot != std::string::npos && fName.substr(dot + 1) == lowName) return true;
        }
    }
    return false;
}

// Helper to infer schema from a subquery result for outer query usage
static TableSchema InferSchemaFromPlan(const TableSchema& srcSchema, const QueryPlan& plan) {
    TableSchema out;
    out.tableName = "Derived";
    bool hasStar = false; // default if empty
    if (plan.projection.empty()) hasStar = true;
    for(const auto& p : plan.projection) if (p=="*") hasStar = true;
    
    if (hasStar) {
        // Copy fields
        out.fields = srcSchema.fields;
        // If there are other expressions, they are appended? 
        // Current system likely doesn't support "*, col".
    } else {
        // Explicit fields
        for (size_t i = 0; i < plan.projection.size(); ++i) {
             Field f;
             // If alias exists, use it
             std::string alias;
             if (i < plan.projectionAliases.size()) alias = plan.projectionAliases[i];
             
             f.name = alias.empty() ? plan.projection[i] : alias;
             f.type = "string"; // simplistic
             out.fields.push_back(f);
        }
        // Also add aggregates as fields
        for (const auto& agg : plan.aggregates) {
             Field f;
             f.name = agg.alias.empty() ? (agg.func + "(" + agg.field + ")") : agg.alias;
             f.type = "string";
             out.fields.push_back(f);
        }
    }
    return out;
}

bool QueryService::BuildCombinedSchemaForPlan(const std::string& dbfPath, const QueryPlan& plan, TableSchema& combined, std::string& err) {
    TableSchema source;
    if (plan.sourceSubQuery) {
        TableSchema innerCombined;
        if (!BuildCombinedSchemaForPlan(dbfPath, *plan.sourceSubQuery, innerCombined, err)) return false;
        source = InferSchemaFromPlan(innerCombined, *plan.sourceSubQuery);
    } else if (!plan.sourceTable.empty()) {
        if (!engine_.LoadSchema(dbfPath, plan.sourceTable, source, err)) {
            // fallback case-insensitive search
            std::vector<TableSchema> schemas;
            if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
            bool found = false;
            for (const auto& s : schemas) {
                if (Lower(s.tableName) == Lower(plan.sourceTable)) { source = s; found = true; break; }
            }
            if (!found) { err = "Table/view not found: " + plan.sourceTable; return false; }
        }
    } else {
        err = "Invalid query plan source";
        return false;
    }

    combined.fields.clear();
    std::string t1Prefix;
    if (plan.sourceSubQuery) {
        t1Prefix = plan.tableAlias;
    } else {
        t1Prefix = plan.tableAlias.empty() ? source.tableName : plan.tableAlias;
    }
    for (const auto& f : source.fields) {
        Field nf = f;
        if (!t1Prefix.empty()) nf.name = t1Prefix + "." + f.name;
        combined.fields.push_back(nf);
    }

    if (!plan.joinTable.empty()) {
        std::vector<TableSchema> allSchemas;
        if (!engine_.LoadSchemas(dbfPath, allSchemas, err)) return false;
        TableSchema schema2;
        bool found = false;
        for (const auto& s : allSchemas) {
            if (Lower(s.tableName) == Lower(plan.joinTable)) { schema2 = s; found = true; break; }
        }
        if (!found) { err = "Join table not found: " + plan.joinTable; return false; }

        std::string t2Prefix = plan.joinTableAlias.empty() ? schema2.tableName : plan.joinTableAlias;
        for (const auto& f : schema2.fields) {
            Field nf = f;
            nf.name = t2Prefix + "." + f.name;
            combined.fields.push_back(nf);
        }
    }

    return true;
}

bool QueryService::ExecuteSubQuery(const std::string& datPath, const std::string& dbfPath, const QueryPlan& plan, std::vector<Record>& out, std::string& err) {
    std::function<bool(const QueryPlan&, TableSchema&)> resolveSchema = 
        [&](const QueryPlan& p, TableSchema& s) -> bool {
        if (!p.sourceTable.empty()) {
            return engine_.LoadSchema(dbfPath, p.sourceTable, s, err);
        }
        if (p.sourceSubQuery) {
            return BuildCombinedSchemaForPlan(dbfPath, *p.sourceSubQuery, s, err);
        }
        err = "Invalid plan source";
        return false;
    };

    TableSchema sourceSchema;
    if (!resolveSchema(plan, sourceSchema)) return false;

    return Select(datPath, dbfPath, sourceSchema, plan, out, err);
}

// Overload for correlated subquery support
bool QueryService::ExecuteSubQuery(const std::string& datPath, const std::string& dbfPath, const QueryPlan& plan, std::vector<Record>& out, std::string& err, const Record* outerRec, const TableSchema* outerSchema) {
    // For now, just delegate to the non-correlated version
    // In a full implementation, we would pass outerRec and outerSchema through to field resolution
    return ExecuteSubQuery(datPath, dbfPath, plan, out, err);
}

bool QueryService::ResolvePlanSourceSchema(const std::string& dbfPath, const QueryPlan& plan, TableSchema& schemaOut, std::string& err) {
    if (!plan.sourceTable.empty()) {
        if (engine_.LoadSchema(dbfPath, plan.sourceTable, schemaOut, err)) return true;
        // fallback case-insensitive search
        std::vector<TableSchema> schemas;
        if (!engine_.LoadSchemas(dbfPath, schemas, err)) return false;
        for (const auto& s : schemas) {
            if (Lower(s.tableName) == Lower(plan.sourceTable)) { schemaOut = s; return true; }
        }
        err = "Table/view not found: " + plan.sourceTable;
        return false;
    }
    if (plan.sourceSubQuery) {
        TableSchema combined;
        if (!BuildCombinedSchemaForPlan(dbfPath, *plan.sourceSubQuery, combined, err)) return false;
        schemaOut = InferSchemaFromPlan(combined, *plan.sourceSubQuery);
        return true;
    }
    err = "Invalid query plan source";
    return false;
}

bool QueryService::EvaluateView(const std::string& datPath, const std::string& dbfPath, const TableSchema& viewSchema, std::vector<Record>& out, std::string& err, Txn* txn, LockManager* lock_manager, int depth) {
    if (depth > 8) { err = "View recursion depth exceeded"; return false; }
    if (viewSchema.viewSql.empty()) { err = "View definition missing"; return false; }
    Parser p;
    ParsedCommand cmd = p.Parse(viewSchema.viewSql, err);
    if (!err.empty() || cmd.type != CommandType::kSelect) {
        if (err.empty()) err = "Invalid view definition";
        return false;
    }
    TableSchema baseSchema;
    if (!ResolvePlanSourceSchema(dbfPath, cmd.query, baseSchema, err)) return false;
    return Select(datPath, dbfPath, baseSchema, cmd.query, out, err, txn, lock_manager);
}

static bool GetFieldValueForOrder(const TableSchema& schema, const Record& rec, const std::string& fieldName,
                                  const std::map<std::string, std::string>& aliasMap, std::string& outVal) {
    std::string name = fieldName;
    auto it = aliasMap.find(Lower(fieldName));
    if (it != aliasMap.end()) name = it->second;
    return GetFieldValue(schema, rec, name, outVal);
}

bool QueryService::MatchConditions(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conds, const std::string& datPath, const std::string& dbfPath) {
  return MatchConditions(schema, rec, conds, datPath, dbfPath, nullptr, nullptr);
}

bool QueryService::MatchConditions(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conds, const std::string& datPath, const std::string& dbfPath, const Record* outerRec, const TableSchema* outerSchema) {
  auto matchSingle = [&](const Condition& cond) {
    // Handle EXISTS/NOT EXISTS
    if (cond.op == "EXISTS" || cond.op == "NOT EXISTS") {
        if (cond.isSubQuery && cond.subQueryPlan) {
            std::vector<Record> subRows;
            std::string subErr;
            if (!ExecuteSubQuery(datPath, dbfPath, *cond.subQueryPlan, subRows, subErr, outerRec, outerSchema)) return false;
            bool hasRows = !subRows.empty();
            return (cond.op == "EXISTS") ? hasRows : !hasRows;
        }
        return false;
    }
    
    if (cond.fieldName.empty()) return true;
    std::string val;
    if (!GetFieldValue(schema, rec, cond.fieldName, val)) return false;
    val = NormalizeValue(val);
    
    std::string condVal;
    
    if (cond.isSubQuery && cond.subQueryPlan) {
        std::vector<Record> subRows;
        std::string subErr;
        if (!ExecuteSubQuery(datPath, dbfPath, *cond.subQueryPlan, subRows, subErr, outerRec, outerSchema)) return false;
        
        if (cond.op == "IN") {
            for(const auto& r : subRows) {
                if(!r.values.empty() && r.values[0] == val) return true;
                try {
                     double v1 = std::stod(val);
                     double v2 = std::stod(r.values[0]);
                     if (std::abs(v1-v2)<1e-9) return true;
                } catch(...) {}
            }
            return false;
        }
        
        if (subRows.empty() || subRows[0].values.empty()) return false;
        condVal = subRows[0].values[0];
    } else {
        condVal = NormalizeValue(cond.value);
    }

    auto asNumber = [](const std::string& s, double& out) {
      try { size_t i=0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
    };

    if (cond.op == "BETWEEN") {
        if (cond.values.size() != 2) return false;
        std::string minVal = NormalizeValue(cond.values[0]);
        std::string maxVal = NormalizeValue(cond.values[1]);
        
        double valNum = 0, minNum = 0, maxNum = 0;
        if (asNumber(val, valNum) && asNumber(minVal, minNum) && asNumber(maxVal, maxNum)) {
            return valNum >= minNum && valNum <= maxNum;
        }
        // String comparison as fallback
        return val >= minVal && val <= maxVal;
    }

    if (cond.op == "LIKE") {
        std::string pattern = NormalizeValue(cond.value);
        
        // Convert SQL LIKE pattern to simple matching
        // %: matches any sequence of characters (including empty)
        // Note: SQL also supports _ for single character, but implementing % first
        
        if (pattern.empty()) return val.empty();
        
        // Case 1: %text% - contains
        if (pattern.size() >= 2 && pattern.front() == '%' && pattern.back() == '%') {
            std::string searchStr = pattern.substr(1, pattern.size() - 2);
            return val.find(searchStr) != std::string::npos;
        }
        
        // Case 2: %text - ends with
        if (pattern.size() >= 1 && pattern.front() == '%') {
            std::string suffix = pattern.substr(1);
            if (val.size() >= suffix.size()) {
                return val.substr(val.size() - suffix.size()) == suffix;
            }
            return false;
        }
        
        // Case 3: text% - starts with
        if (pattern.size() >= 1 && pattern.back() == '%') {
            std::string prefix = pattern.substr(0, pattern.size() - 1);
            if (val.size() >= prefix.size()) {
                return val.substr(0, prefix.size()) == prefix;
            }
            return false;
        }
        
        // Case 4: exact match (no wildcards)
        return val == pattern;
    }

    if (cond.op == "NOT LIKE") {
        std::string pattern = NormalizeValue(cond.value);
        
        // Use the same LIKE matching logic but negate the result
        if (pattern.empty()) return !val.empty();
        
        // Case 1: %text% - does not contain
        if (pattern.size() >= 2 && pattern.front() == '%' && pattern.back() == '%') {
            std::string searchStr = pattern.substr(1, pattern.size() - 2);
            return val.find(searchStr) == std::string::npos;
        }
        
        // Case 2: %text - does not end with
        if (pattern.size() >= 1 && pattern.front() == '%') {
            std::string suffix = pattern.substr(1);
            if (val.size() >= suffix.size()) {
                return val.substr(val.size() - suffix.size()) != suffix;
            }
            return true;
        }
        
        // Case 3: text% - does not start with
        if (pattern.size() >= 1 && pattern.back() == '%') {
            std::string prefix = pattern.substr(0, pattern.size() - 1);
            if (val.size() >= prefix.size()) {
                return val.substr(0, prefix.size()) != prefix;
            }
            return true;
        }
        
        // Case 4: not exact match (no wildcards)
        return val != pattern;
    }

    if (cond.op == "IN" && !cond.isSubQuery) {
        for (const auto& v : cond.values) {
           std::string nv = NormalizeValue(v);
           double valNum = 0, vNum = 0;
           if (asNumber(val, valNum) && asNumber(nv, vNum)) {
               if (std::abs(valNum - vNum) < 1e-9) return true;
           }
           if (val == nv) return true;
        }
        return false;
    }

    if (cond.op == "=") {
         double k1=0, k2=0;
         if (asNumber(val, k1) && asNumber(condVal, k2)) return std::abs(k1-k2)<1e-9;
         return val == condVal;
    }
    if (cond.op == "!=") {
         double k1=0, k2=0;
         if (asNumber(val, k1) && asNumber(condVal, k2)) return std::abs(k1-k2)>=1e-9;
         return val != condVal;
    }
    if (cond.op == "CONTAINS") return val.find(condVal) != std::string::npos;
    if (cond.op == ">" || cond.op == ">=" || cond.op == "<" || cond.op == "<=") {
      double lv = 0, rv = 0;
      if (asNumber(val, lv) && asNumber(condVal, rv)) {
        if (cond.op == ">") return lv > rv;
        if (cond.op == ">=") return lv >= rv;
        if (cond.op == "<") return lv < rv;
        if (cond.op == "<=") return lv <= rv;
      }
      if (cond.op == ">") return val > condVal;
      if (cond.op == ">=") return val >= condVal;
      if (cond.op == "<") return val < condVal;
      if (cond.op == "<=") return val <= condVal;
    }
    return false;
  };

  for (const auto& c : conds) {
    if (!matchSingle(c)) return false;
  }
  return true;
}

Record QueryService::Project(const TableSchema& schema, const Record& rec, const std::vector<std::string>& projection) const {
  if (projection.empty()) return rec;
  Record out;
  out.valid = rec.valid;
  for (const auto& name : projection) {
    if (name == "*") { out = rec; return out; } // handle * explicitly if present
    std::string val;
    if (GetFieldValue(schema, rec, name, val)) {
        out.values.push_back(val);
    } else {
        out.values.push_back("NULL"); 
    }
  }
  return out;
}


bool QueryService::Select(const std::string& datPath, const std::string& dbfPath, const TableSchema& schema, const QueryPlan& plan,
                          std::vector<Record>& out, std::string& err, Txn* txn, LockManager* lock_manager) {
  std::vector<RID> sharedLocks;
  std::set<std::string> sharedKeys;
  auto trackShared = [&](const RID& rid, std::string& errOut) -> bool {
      if (!lock_manager || !txn) return true;
      std::string key = rid.table_name + "#" + std::to_string(rid.file_offset);
      if (sharedKeys.count(key)) return true;
      if (!lock_manager->LockShared(txn->id, rid, errOut)) return false;
      sharedKeys.insert(key);
      sharedLocks.push_back(rid);
      return true;
  };
  struct SharedLockReleaser {
      LockManager* lm = nullptr;
      Txn* txn = nullptr;
      std::vector<RID>* rids = nullptr;
      ~SharedLockReleaser() {
          if (!lm || !txn || !rids) return;
          for (const auto& rid : *rids) lm->ReleaseShared(txn->id, rid);
      }
  } releaser{lock_manager, txn, &sharedLocks};

  static thread_local std::vector<std::string> viewStack;

  std::vector<Record> r1;
  std::vector<std::pair<long, Record>> r1o;
  
  // Try Index Optimization
  bool indexUsed = false;
  if (schema.isView) {
      std::string lowName = Lower(schema.tableName);
      if (std::find(viewStack.begin(), viewStack.end(), lowName) != viewStack.end()) {
          err = "View recursion detected";
          return false;
      }
      viewStack.push_back(lowName);
      struct ViewPop { std::vector<std::string>* stack; ~ViewPop(){ if (stack && !stack->empty()) stack->pop_back(); } } popGuard{&viewStack};
      if (!EvaluateView(datPath, dbfPath, schema, r1, err, txn, lock_manager, static_cast<int>(viewStack.size()))) return false;
      indexUsed = true;
  }

  if (!indexUsed) {
  for (const auto& c : plan.conditions) {
      if (c.op == "=" && !c.fieldName.empty()) {
          // Check if field is indexed
           auto it = std::find_if(schema.indexes.begin(), schema.indexes.end(), [&](const IndexDef& d){ return d.fieldName == c.fieldName; });
           if (it != schema.indexes.end()) {
               // Use index name for file path
               std::string idxPath = dbms_paths::IndexPathFromDat(datPath, schema.tableName, it->name);
               std::map<std::string, long> idx;
               // Load index. If fail (missing file), fall back to scan
               std::string ignErr;
               if (engine_.LoadIndex(idxPath, idx, ignErr)) {
                   std::string key = NormalizeValue(c.value);
                   std::vector<std::string> keys = {key, c.value, "'" + key + "'", "\"" + key + "\""};
                   for (const auto& k : keys) {
                       if (!idx.count(k)) continue;
                       Record rec;
                       if (engine_.ReadRecordAt(datPath, schema, idx[k], rec, ignErr)) {
                           if (rec.valid) {
                               r1.push_back(rec);
                               RID rid{schema.tableName, static_cast<uint64_t>(idx[k])};
                               if (!trackShared(rid, ignErr)) { err = ignErr; return false; }
                           }
                       }
                       break;
                   }
                   indexUsed = true;
                   break;
               }
           }
      }
  }
  }

  if (!indexUsed) {
      if (plan.sourceSubQuery) {
          if (!ExecuteSubQuery(datPath, dbfPath, *plan.sourceSubQuery, r1, err)) return false;
          indexUsed = true;
      } else {
          if (!engine_.ReadRecordsWithOffsets(datPath, schema, r1o, err)) return false;
          for (const auto& p : r1o) r1.push_back(p.second);
      }
  }

  bool isJoin = !plan.joinTable.empty();
  std::vector<Record> r2;
  std::vector<std::pair<long, Record>> r2o;
  TableSchema schema2;
  
  // Combine Schemas (preserving alias info in field names)
  TableSchema combinedSchema;
  std::string t1Prefix;
  if (plan.sourceSubQuery) {
      t1Prefix = plan.tableAlias;
  } else {
      t1Prefix = plan.tableAlias.empty() ? schema.tableName : plan.tableAlias;
  }
  
  for (const auto& f : schema.fields) {
      Field nf = f;
      if (!t1Prefix.empty()) nf.name = t1Prefix + "." + f.name;
      combinedSchema.fields.push_back(nf);
  }

  isJoin = isJoin && !plan.joinTable.empty();
  
  std::vector<std::pair<size_t, size_t>> naturalPairs;
  if (isJoin) {
      // Load Schema2
      std::vector<TableSchema> allSchemas;
      if (!engine_.LoadSchemas(dbfPath, allSchemas, err)) return false;
      bool found = false;
      for (const auto& s : allSchemas) {
          if (Lower(s.tableName) == Lower(plan.joinTable)) {
              schema2 = s;
              found = true;
              break;
          }
      }
      if (!found) { err = "Join table not found: " + plan.joinTable; return false; }
      
      if (!engine_.ReadRecordsWithOffsets(datPath, schema2, r2o, err)) return false;
      for (const auto& p : r2o) r2.push_back(p.second);
      
      std::string t2Prefix = plan.joinTableAlias.empty() ? schema2.tableName : plan.joinTableAlias;
      for (const auto& f : schema2.fields) {
         Field nf = f;
         nf.name = t2Prefix + "." + f.name; 
         combinedSchema.fields.push_back(nf);
      }

      if (plan.isNaturalJoin) {
          for (size_t i = 0; i < schema.fields.size(); ++i) {
              for (size_t j = 0; j < schema2.fields.size(); ++j) {
                  if (Lower(schema.fields[i].name) == Lower(schema2.fields[j].name)) {
                      naturalPairs.push_back({i, j});
                  }
              }
          }
      }
  }

  std::vector<std::string> effectiveProjection = plan.projection;
  if (plan.isNaturalJoin) {
      bool isStar = effectiveProjection.empty() ||
                    (effectiveProjection.size() == 1 && effectiveProjection[0] == "*");
      if (isStar) {
          std::set<std::string> seen;
          effectiveProjection.clear();
          for (const auto& f : combinedSchema.fields) {
              std::string base = Lower(f.name);
              size_t dot = base.rfind('.');
              if (dot != std::string::npos) base = base.substr(dot + 1);
              if (seen.insert(base).second) effectiveProjection.push_back(f.name);
          }
      }
  }

  out.clear();
  
  if (!isJoin) {
      std::vector<Record> matched;
      if (!indexUsed) {
          for (const auto& p : r1o) {
              const auto& r = p.second;
              if (!r.valid) continue;
              if (!MatchConditions(combinedSchema, r, plan.conditions, datPath, dbfPath)) continue;
              RID rid{schema.tableName, static_cast<uint64_t>(p.first)};
              if (!trackShared(rid, err)) return false;
              matched.push_back(r);
          }
      } else {
          for (const auto& r : r1) {
              if (!r.valid) continue;
              if (!MatchConditions(combinedSchema, r, plan.conditions, datPath, dbfPath)) continue;
              matched.push_back(r);
          }
      }

      bool hasAgg = !plan.aggregates.empty() || !plan.groupBy.empty();
      if (hasAgg) {
          std::map<std::string, bool> groupBySet;
          for (const auto& g : plan.groupBy) groupBySet[Lower(g)] = true;

          for (const auto& sel : plan.selectExprs) {
              if (!sel.isAggregate) {
                  if (!sel.field.empty() && sel.field != "*" && !groupBySet[Lower(sel.field)]) {
                      err = "Non-aggregate field not in GROUP BY: " + sel.field;
                      return false;
                  }
              }
          }

          struct AggState {
              std::string func;
              std::string field;
              long count = 0;
              double sum = 0;
              std::string minVal;
              std::string maxVal;
              bool hasVal = false;
          };
          struct GroupData {
              std::map<std::string, std::string> groupVals; // lower(field)->value
              std::vector<AggState> aggs;
          };

          auto asNumber = [](const std::string& s, double& out) {
              try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
          };
          auto lessValue = [&](const std::string& a, const std::string& b) {
              double an = 0, bn = 0;
              bool aNum = asNumber(a, an);
              bool bNum = asNumber(b, bn);
              if (aNum && bNum) return an < bn;
              return a < b;
          };

          std::map<std::string, GroupData> groups;
          for (const auto& r : matched) {
              std::string key;
              GroupData* gd = nullptr;
              if (!plan.groupBy.empty()) {
                  for (const auto& g : plan.groupBy) {
                      std::string v;
                      if (!GetFieldValue(combinedSchema, r, g, v)) {
                          err = "GROUP BY field not found: " + g;
                          return false;
                      }
                      key += v;
                      key.push_back('\x1f');
                  }
              }

              auto it = groups.find(key);
              if (it == groups.end()) {
                  GroupData init;
                  for (const auto& g : plan.groupBy) {
                      std::string v;
                      GetFieldValue(combinedSchema, r, g, v);
                      init.groupVals[Lower(g)] = v;
                  }
                  for (const auto& a : plan.aggregates) {
                      AggState st;
                      st.func = a.func;
                      st.field = a.field;
                      init.aggs.push_back(st);
                  }
                  it = groups.insert({key, init}).first;
              }
              gd = &it->second;

              for (auto& st : gd->aggs) {
                  if (st.func == "COUNT") {
                      if (st.field == "*" || st.field.empty()) {
                          st.count++;
                      } else {
                          std::string v;
                          if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                              err = "COUNT field not found: " + st.field;
                              return false;
                          }
                          if (!v.empty() && v != "NULL") st.count++;
                      }
                  } else if (st.func == "SUM" || st.func == "AVG") {
                      std::string v;
                      if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                          err = st.func + " field not found: " + st.field;
                          return false;
                      }
                      double num = 0;
                      if (!asNumber(v, num)) {
                          err = st.func + " requires numeric field: " + st.field;
                          return false;
                      }
                      st.sum += num;
                      st.count++;
                  } else if (st.func == "MIN" || st.func == "MAX") {
                      std::string v;
                      if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                          err = st.func + " field not found: " + st.field;
                          return false;
                      }
                      if (!st.hasVal) {
                          st.minVal = v;
                          st.maxVal = v;
                          st.hasVal = true;
                      } else {
                          if (lessValue(v, st.minVal)) st.minVal = v;
                          if (lessValue(st.maxVal, v)) st.maxVal = v;
                      }
                  }
              }
          }

          TableSchema outSchema;
          for (const auto& sel : plan.selectExprs) {
              Field f;
              if (!sel.alias.empty()) f.name = sel.alias;
              else if (sel.isAggregate) {
                  f.name = sel.agg.func + "(" + sel.agg.field + ")";
              } else {
                  f.name = sel.field;
              }
              outSchema.fields.push_back(f);
          }

          std::vector<Record> aggOut;
          for (const auto& kv : groups) {
              const GroupData& gd = kv.second;
              Record rec;
              rec.valid = true;
              size_t aggIndex = 0;
              for (const auto& sel : plan.selectExprs) {
                  if (sel.isAggregate) {
                      const AggState& st = gd.aggs[aggIndex++];
                      if (st.func == "COUNT") rec.values.push_back(std::to_string(st.count));
                      else if (st.func == "SUM") rec.values.push_back(std::to_string(st.sum));
                      else if (st.func == "AVG") {
                          if (st.count == 0) rec.values.push_back("NULL");
                          else rec.values.push_back(std::to_string(st.sum / st.count));
                      } else if (st.func == "MIN") rec.values.push_back(st.hasVal ? st.minVal : "NULL");
                      else if (st.func == "MAX") rec.values.push_back(st.hasVal ? st.maxVal : "NULL");
                      else rec.values.push_back("NULL");
                  } else {
                      auto itv = gd.groupVals.find(Lower(sel.field));
                      if (itv != gd.groupVals.end()) rec.values.push_back(itv->second);
                      else rec.values.push_back("NULL");
                  }
              }
              aggOut.push_back(rec);
          }

          // Apply HAVING filter
          if (!plan.havingConditions.empty()) {
              // Build a temporary schema that maps aggregate expressions to their field names
              TableSchema havingSchema;
              for (const auto& sel : plan.selectExprs) {
                  Field f;
                  if (sel.isAggregate) {
                      // Add field with aggregate expression name like "COUNT(*)"
                      f.name = sel.agg.func + "(" + sel.agg.field + ")";
                      havingSchema.fields.push_back(f);
                  } else {
                      // Add field with actual name
                      f.name = sel.field;
                      havingSchema.fields.push_back(f);
                  }
              }
              
              std::vector<Record> havingFiltered;
              for (const auto& rec : aggOut) {
                  if (MatchConditions(havingSchema, rec, plan.havingConditions, datPath, dbfPath)) {
                      havingFiltered.push_back(rec);
                  }
              }
              aggOut = havingFiltered;
          }

          if (!plan.orderBy.empty()) {
      std::map<std::string, std::string> aliasMap;
      for (const auto& sel : plan.selectExprs) {
          std::string name = sel.alias.empty() ? (sel.isAggregate ? sel.agg.func + "(" + sel.agg.field + ")" : sel.field) : sel.alias;
          if (!sel.alias.empty()) {
              aliasMap[Lower(sel.field)] = name;
              aliasMap[Lower(sel.alias)] = name;
          }
          // Map aggregate expressions like "COUNT(*)" to their field names
          if (sel.isAggregate) {
              std::string exprName = sel.agg.func + "(" + sel.agg.field + ")";
              aliasMap[Lower(exprName)] = name;
          }
      }

              for (const auto& ob : plan.orderBy) {
                  std::string field = ob.first;
                  auto it = aliasMap.find(Lower(field));
                  if (it != aliasMap.end()) field = it->second;
                  if (!FieldExists(outSchema, field)) {
                      err = "ORDER BY field not found: " + ob.first;
                      return false;
                  }
              }

              auto asNumber = [](const std::string& s, double& out) {
                  try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
              };

              auto cmp = [&](const Record& a, const Record& b) {
                  for (const auto& ob : plan.orderBy) {
                      std::string av, bv;
                      GetFieldValueForOrder(outSchema, a, ob.first, aliasMap, av);
                      GetFieldValueForOrder(outSchema, b, ob.first, aliasMap, bv);

                      double an = 0, bn = 0;
                      bool aNum = asNumber(av, an);
                      bool bNum = asNumber(bv, bn);

                      if (aNum && bNum) {
                          if (std::abs(an - bn) < 1e-9) continue;
                          return ob.second ? (an < bn) : (an > bn);
                      }
                      if (av == bv) continue;
                      return ob.second ? (av < bv) : (av > bv);
                  }
                  return false;
              };

              std::sort(aggOut.begin(), aggOut.end(), cmp);
          }

          out = aggOut;
          return true;
      }

      if (!plan.orderBy.empty()) {
          std::map<std::string, std::string> aliasMap;
          for (size_t i = 0; i < plan.projection.size(); ++i) {
              if (i < plan.projectionAliases.size() && !plan.projectionAliases[i].empty()) {
                  aliasMap[Lower(plan.projectionAliases[i])] = plan.projection[i];
              }
          }

          for (const auto& ob : plan.orderBy) {
              std::string field = ob.first;
              auto it = aliasMap.find(Lower(field));
              if (it != aliasMap.end()) field = it->second;
              if (!FieldExists(combinedSchema, field)) {
                  err = "ORDER BY field not found: " + ob.first;
                  return false;
              }
          }

          auto asNumber = [](const std::string& s, double& out) {
              try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
          };

          auto cmp = [&](const Record& a, const Record& b) {
              for (const auto& ob : plan.orderBy) {
                  std::string av, bv;
                  GetFieldValueForOrder(combinedSchema, a, ob.first, aliasMap, av);
                  GetFieldValueForOrder(combinedSchema, b, ob.first, aliasMap, bv);

                  double an = 0, bn = 0;
                  bool aNum = asNumber(av, an);
                  bool bNum = asNumber(bv, bn);

                  if (aNum && bNum) {
                      if (std::abs(an - bn) < 1e-9) continue;
                      return ob.second ? (an < bn) : (an > bn);
                  }
                  if (av == bv) continue;
                  return ob.second ? (av < bv) : (av > bv);
              }
              return false;
          };

          std::sort(matched.begin(), matched.end(), cmp);
      }

      // Handle SELECT list subqueries
      if (!plan.selectExprs.empty()) {
          bool hasSubQuery = false;
          for (const auto& sel : plan.selectExprs) {
              if (sel.isSubQuery) {
                  hasSubQuery = true;
                  break;
              }
          }
          
          if (hasSubQuery) {
              std::vector<Record> finalOut;
              for (const auto& r : matched) {
                  Record outRec;
                  outRec.valid = r.valid;
                  
                  for (const auto& sel : plan.selectExprs) {
                      if (sel.isSubQuery && sel.subQueryPlan) {
                          // Execute subquery for each row
                          std::vector<Record> subResult;
                          std::string subErr;
                          if (!ExecuteSubQuery(datPath, dbfPath, *sel.subQueryPlan, subResult, subErr, &r, &combinedSchema)) {
                              err = "Subquery in SELECT failed: " + subErr;
                              return false;
                          }
                          // Take first value from first row
                          if (!subResult.empty() && !subResult[0].values.empty()) {
                              outRec.values.push_back(subResult[0].values[0]);
                          } else {
                              outRec.values.push_back("NULL");
                          }
                      } else {
                          // Regular field
                          std::string val;
                          if (GetFieldValue(combinedSchema, r, sel.field, val)) {
                              outRec.values.push_back(val);
                          } else {
                              outRec.values.push_back("NULL");
                          }
                      }
                  }
                  finalOut.push_back(outRec);
              }
              out = finalOut;
              return true;
          }
      }

      for (const auto& r : matched) out.push_back(Project(combinedSchema, r, effectiveProjection));
      return true;
  }

  // Handle JOIN (Nested Loop with support for Left/Right)
  auto createCombined = [&](const Record& rA, const Record& rB) {
      Record c; c.valid = true;
      c.values = rA.values;
      c.values.insert(c.values.end(), rB.values.begin(), rB.values.end());
      return c;
  };
  
  auto matchesVar = [&](const Record& rA, const Record& rB, const Record& cmb) {
      if (plan.isNaturalJoin) {
          for (const auto& pr : naturalPairs) {
              std::string lv = (pr.first < rA.values.size()) ? NormalizeValue(rA.values[pr.first]) : "";
              std::string rv = (pr.second < rB.values.size()) ? NormalizeValue(rB.values[pr.second]) : "";
              if (lv.empty() || rv.empty()) return false;
              if (Lower(lv) == "null" || Lower(rv) == "null") return false;
              if (lv != rv) return false;
          }
          return true;
      }
      std::string l, r;
      if (!GetFieldValue(combinedSchema, cmb, plan.joinOnLeft, l)) return false;
      if (!GetFieldValue(combinedSchema, cmb, plan.joinOnRight, r)) return false;
      return l == r;
  };
  
  Record nullR1; for(auto f : schema.fields) nullR1.values.push_back("NULL");
  Record nullR2; for(auto f : schema2.fields) nullR2.values.push_back("NULL");
  
  std::vector<Record> matchedRows;
  if (plan.joinType == JoinType::kInner || plan.joinType == JoinType::kLeft) {
      for (size_t i = 0; i < r1.size(); ++i) {
          const auto& row1 = r1[i];
          if (!row1.valid) continue;
          bool matched = false;
          for (size_t j = 0; j < r2.size(); ++j) {
              const auto& row2 = r2[j];
              if (!row2.valid) continue;
              Record cur = createCombined(row1, row2);
              if (matchesVar(row1, row2, cur)) {
                  if (MatchConditions(combinedSchema, cur, plan.conditions, datPath, dbfPath)) {
                       matched = true;
                       if (lock_manager && txn) {
                           if (!r1o.empty() && i < r1o.size()) {
                               RID rid1{schema.tableName, static_cast<uint64_t>(r1o[i].first)};
                               if (!trackShared(rid1, err)) return false;
                           }
                           if (j < r2o.size()) {
                               RID rid2{schema2.tableName, static_cast<uint64_t>(r2o[j].first)};
                               if (!trackShared(rid2, err)) return false;
                           }
                       }
                       matchedRows.push_back(cur);
                  }
              }
          }
          if (plan.joinType == JoinType::kLeft && !matched) {
               Record cur = createCombined(row1, nullR2);
               if (MatchConditions(combinedSchema, cur, plan.conditions, datPath, dbfPath)) {
                    if (lock_manager && txn) {
                        if (!r1o.empty() && i < r1o.size()) {
                            RID rid1{schema.tableName, static_cast<uint64_t>(r1o[i].first)};
                            if (!trackShared(rid1, err)) return false;
                        }
                    }
                    matchedRows.push_back(cur);
               }
          }
      }
  } 
  else if (plan.joinType == JoinType::kRight) {
      for (size_t j = 0; j < r2.size(); ++j) {
          const auto& row2 = r2[j];
          if (!row2.valid) continue;
          bool matched = false;
          for (size_t i = 0; i < r1.size(); ++i) {
              const auto& row1 = r1[i];
              if (!row1.valid) continue;
              Record cur = createCombined(row1, row2);
              if (matchesVar(row1, row2, cur)) {
                  if (MatchConditions(combinedSchema, cur, plan.conditions, datPath, dbfPath)) {
                       matched = true;
                       if (lock_manager && txn) {
                           if (!r1o.empty() && i < r1o.size()) {
                               RID rid1{schema.tableName, static_cast<uint64_t>(r1o[i].first)};
                               if (!trackShared(rid1, err)) return false;
                           }
                           if (j < r2o.size()) {
                               RID rid2{schema2.tableName, static_cast<uint64_t>(r2o[j].first)};
                               if (!trackShared(rid2, err)) return false;
                           }
                       }
                       matchedRows.push_back(cur);
                  }
              }
          }
          if (!matched) {
               Record cur = createCombined(nullR1, row2);
               if (MatchConditions(combinedSchema, cur, plan.conditions, datPath, dbfPath)) {
                    if (lock_manager && txn) {
                        if (j < r2o.size()) {
                            RID rid2{schema2.tableName, static_cast<uint64_t>(r2o[j].first)};
                            if (!trackShared(rid2, err)) return false;
                        }
                    }
                    matchedRows.push_back(cur);
               }
          }
      }
  }

  bool hasAgg = !plan.aggregates.empty() || !plan.groupBy.empty();
  if (hasAgg) {
      std::map<std::string, bool> groupBySet;
      for (const auto& g : plan.groupBy) groupBySet[Lower(g)] = true;

      for (const auto& sel : plan.selectExprs) {
          if (!sel.isAggregate) {
              if (!sel.field.empty() && sel.field != "*" && !groupBySet[Lower(sel.field)]) {
                  err = "Non-aggregate field not in GROUP BY: " + sel.field;
                  return false;
              }
          }
      }

      struct AggState {
          std::string func;
          std::string field;
          long count = 0;
          double sum = 0;
          std::string minVal;
          std::string maxVal;
          bool hasVal = false;
      };
      struct GroupData {
          std::map<std::string, std::string> groupVals; // lower(field)->value
          std::vector<AggState> aggs;
      };

      auto asNumber = [](const std::string& s, double& out) {
          try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
      };
      auto lessValue = [&](const std::string& a, const std::string& b) {
          double an = 0, bn = 0;
          bool aNum = asNumber(a, an);
          bool bNum = asNumber(b, bn);
          if (aNum && bNum) return an < bn;
          return a < b;
      };

      std::map<std::string, GroupData> groups;
      for (const auto& r : matchedRows) {
          std::string key;
          GroupData* gd = nullptr;
          if (!plan.groupBy.empty()) {
              for (const auto& g : plan.groupBy) {
                  std::string v;
                  if (!GetFieldValue(combinedSchema, r, g, v)) {
                      err = "GROUP BY field not found: " + g;
                      return false;
                  }
                  key += v;
                  key.push_back('\x1f');
              }
          }

          auto it = groups.find(key);
          if (it == groups.end()) {
              GroupData init;
              for (const auto& g : plan.groupBy) {
                  std::string v;
                  GetFieldValue(combinedSchema, r, g, v);
                  init.groupVals[Lower(g)] = v;
              }
              for (const auto& a : plan.aggregates) {
                  AggState st;
                  st.func = a.func;
                  st.field = a.field;
                  init.aggs.push_back(st);
              }
              it = groups.insert({key, init}).first;
          }
          gd = &it->second;

          for (auto& st : gd->aggs) {
              if (st.func == "COUNT") {
                  if (st.field == "*" || st.field.empty()) {
                      st.count++;
                  } else {
                      std::string v;
                      if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                          err = "COUNT field not found: " + st.field;
                          return false;
                      }
                      if (!v.empty() && v != "NULL") st.count++;
                  }
              } else if (st.func == "SUM" || st.func == "AVG") {
                  std::string v;
                  if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                      err = st.func + " field not found: " + st.field;
                      return false;
                  }
                  double num = 0;
                  if (!asNumber(v, num)) {
                      err = st.func + " requires numeric field: " + st.field;
                      return false;
                  }
                  st.sum += num;
                  st.count++;
              } else if (st.func == "MIN" || st.func == "MAX") {
                  std::string v;
                  if (!GetFieldValue(combinedSchema, r, st.field, v)) {
                      err = st.func + " field not found: " + st.field;
                      return false;
                  }
                  if (!st.hasVal) {
                      st.minVal = v;
                      st.maxVal = v;
                      st.hasVal = true;
                  } else {
                      if (lessValue(v, st.minVal)) st.minVal = v;
                      if (lessValue(st.maxVal, v)) st.maxVal = v;
                  }
              }
          }
      }

      TableSchema outSchema;
      for (const auto& sel : plan.selectExprs) {
          Field f;
          if (!sel.alias.empty()) f.name = sel.alias;
          else if (sel.isAggregate) {
              f.name = sel.agg.func + "(" + sel.agg.field + ")";
          } else {
              f.name = sel.field;
          }
          outSchema.fields.push_back(f);
      }

      std::vector<Record> aggOut;
      for (const auto& kv : groups) {
          const GroupData& gd = kv.second;
          Record rec;
          rec.valid = true;
          size_t aggIndex = 0;
          for (const auto& sel : plan.selectExprs) {
              if (sel.isAggregate) {
                  const AggState& st = gd.aggs[aggIndex++];
                  if (st.func == "COUNT") rec.values.push_back(std::to_string(st.count));
                  else if (st.func == "SUM") rec.values.push_back(std::to_string(st.sum));
                  else if (st.func == "AVG") {
                      if (st.count == 0) rec.values.push_back("NULL");
                      else rec.values.push_back(std::to_string(st.sum / st.count));
                  } else if (st.func == "MIN") rec.values.push_back(st.hasVal ? st.minVal : "NULL");
                  else if (st.func == "MAX") rec.values.push_back(st.hasVal ? st.maxVal : "NULL");
                  else rec.values.push_back("NULL");
              } else {
                  auto itv = gd.groupVals.find(Lower(sel.field));
                  if (itv != gd.groupVals.end()) rec.values.push_back(itv->second);
                  else rec.values.push_back("NULL");
              }
          }
          aggOut.push_back(rec);
      }

      // Apply HAVING filter
      if (!plan.havingConditions.empty()) {
          // Build a temporary schema that maps aggregate expressions to their field names
          TableSchema havingSchema;
          for (const auto& sel : plan.selectExprs) {
              Field f;
              if (sel.isAggregate) {
                  // Add field with aggregate expression name like "COUNT(*)"
                  f.name = sel.agg.func + "(" + sel.agg.field + ")";
                  havingSchema.fields.push_back(f);
              } else {
                  // Add field with actual name
                  f.name = sel.field;
                  havingSchema.fields.push_back(f);
              }
          }
          
          std::vector<Record> havingFiltered;
          for (const auto& rec : aggOut) {
              if (MatchConditions(havingSchema, rec, plan.havingConditions, datPath, dbfPath)) {
                  havingFiltered.push_back(rec);
              }
          }
          aggOut = havingFiltered;
      }

      if (!plan.orderBy.empty()) {
          std::map<std::string, std::string> aliasMap;
          for (const auto& sel : plan.selectExprs) {
              std::string name = sel.alias.empty() ? (sel.isAggregate ? sel.agg.func + "(" + sel.agg.field + ")" : sel.field) : sel.alias;
              if (!sel.alias.empty()) {
                  aliasMap[Lower(sel.field)] = name;
                  aliasMap[Lower(sel.alias)] = name;
              }
              if (sel.isAggregate) {
                  std::string exprName = sel.agg.func + "(" + sel.agg.field + ")";
                  aliasMap[Lower(exprName)] = name;
              }
          }

          for (const auto& ob : plan.orderBy) {
              std::string field = ob.first;
              auto it = aliasMap.find(Lower(field));
              if (it != aliasMap.end()) field = it->second;
              if (!FieldExists(outSchema, field)) {
                  err = "ORDER BY field not found: " + ob.first;
                  return false;
              }
          }

          auto asNumber = [](const std::string& s, double& out) {
              try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
          };

          auto cmp = [&](const Record& a, const Record& b) {
              for (const auto& ob : plan.orderBy) {
                  std::string av, bv;
                  GetFieldValueForOrder(outSchema, a, ob.first, aliasMap, av);
                  GetFieldValueForOrder(outSchema, b, ob.first, aliasMap, bv);

                  double an = 0, bn = 0;
                  bool aNum = asNumber(av, an);
                  bool bNum = asNumber(bv, bn);

                  if (aNum && bNum) {
                      if (std::abs(an - bn) < 1e-9) continue;
                      return ob.second ? (an < bn) : (an > bn);
                  }
                  if (av == bv) continue;
                  return ob.second ? (av < bv) : (av > bv);
              }
              return false;
          };

          std::sort(aggOut.begin(), aggOut.end(), cmp);
      }

      out = aggOut;
      return true;
  }

  if (!plan.orderBy.empty()) {
      std::map<std::string, std::string> aliasMap;
      for (size_t i = 0; i < plan.projection.size(); ++i) {
          if (i < plan.projectionAliases.size() && !plan.projectionAliases[i].empty()) {
              aliasMap[Lower(plan.projectionAliases[i])] = plan.projection[i];
          }
      }

      for (const auto& ob : plan.orderBy) {
          std::string field = ob.first;
          auto it = aliasMap.find(Lower(field));
          if (it != aliasMap.end()) field = it->second;
          if (!FieldExists(combinedSchema, field)) {
              err = "ORDER BY field not found: " + ob.first;
              return false;
          }
      }

      auto asNumber = [](const std::string& s, double& out) {
          try { size_t i = 0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
      };

      auto cmp = [&](const Record& a, const Record& b) {
          for (const auto& ob : plan.orderBy) {
              std::string av, bv;
              GetFieldValueForOrder(combinedSchema, a, ob.first, aliasMap, av);
              GetFieldValueForOrder(combinedSchema, b, ob.first, aliasMap, bv);

              double an = 0, bn = 0;
              bool aNum = asNumber(av, an);
              bool bNum = asNumber(bv, bn);

              if (aNum && bNum) {
                  if (std::abs(an - bn) < 1e-9) continue;
                  return ob.second ? (an < bn) : (an > bn);
              }
              if (av == bv) continue;
              return ob.second ? (av < bv) : (av > bv);
          }
          return false;
      };

      std::sort(matchedRows.begin(), matchedRows.end(), cmp);
  }

  for (const auto& r : matchedRows) out.push_back(Project(combinedSchema, r, effectiveProjection));
  return true;
}

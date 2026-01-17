#include "query.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <string>
#include <map>
#include <set>
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

static bool GetFieldValueForOrder(const TableSchema& schema, const Record& rec, const std::string& fieldName,
                                  const std::map<std::string, std::string>& aliasMap, std::string& outVal) {
    std::string name = fieldName;
    auto it = aliasMap.find(Lower(fieldName));
    if (it != aliasMap.end()) name = it->second;
    return GetFieldValue(schema, rec, name, outVal);
}

bool QueryService::MatchConditions(const TableSchema& schema, const Record& rec, const std::vector<Condition>& conds) const {
  auto matchSingle = [&](const Condition& cond) {
    if (cond.fieldName.empty()) return true;
    std::string val;
    if (!GetFieldValue(schema, rec, cond.fieldName, val)) return false;
    val = NormalizeValue(val);
    std::string condVal = NormalizeValue(cond.value);

    // numeric compare when both values look like numbers
    auto asNumber = [](const std::string& s, double& out) {
      try { size_t i=0; out = std::stod(s, &i); return i == s.size(); } catch (...) { return false; }
    };

    if (cond.op == "IN") {
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
      // Fallback to string (dates)
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

  std::vector<Record> r1;
  std::vector<std::pair<long, Record>> r1o;
  
  // Try Index Optimization
  bool indexUsed = false;
  for (const auto& c : plan.conditions) {
      if (c.op == "=" && !c.fieldName.empty()) {
          // Check if field is indexed
           auto it = std::find_if(schema.indexes.begin(), schema.indexes.end(), [&](const IndexDef& d){ return d.fieldName == c.fieldName; });
           if (it != schema.indexes.end()) {
               // Use index name for file path
               std::string idxPath = datPath + "." + schema.tableName + "." + it->name + ".idx";
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

  if (!indexUsed) {
    if (!engine_.ReadRecordsWithOffsets(datPath, schema, r1o, err)) return false;
    for (const auto& p : r1o) r1.push_back(p.second);
  }

  bool isJoin = !plan.joinTable.empty();
  std::vector<Record> r2;
  std::vector<std::pair<long, Record>> r2o;
  TableSchema schema2;
  
  // Combine Schemas (preserving alias info in field names)
  TableSchema combinedSchema;
  std::string t1Prefix = plan.tableAlias.empty() ? schema.tableName : plan.tableAlias;
  
  for (const auto& f : schema.fields) {
      Field nf = f;
      nf.name = t1Prefix + "." + f.name; 
      combinedSchema.fields.push_back(nf);
  }

  isJoin = isJoin && !plan.joinTable.empty();
  
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
  }

  out.clear();
  
  if (!isJoin) {
      std::vector<Record> matched;
      if (!indexUsed) {
          for (const auto& p : r1o) {
              const auto& r = p.second;
              if (!r.valid) continue;
              if (!MatchConditions(combinedSchema, r, plan.conditions)) continue;
              RID rid{schema.tableName, static_cast<uint64_t>(p.first)};
              if (!trackShared(rid, err)) return false;
              matched.push_back(r);
          }
      } else {
          for (const auto& r : r1) {
              if (!r.valid) continue;
              if (!MatchConditions(combinedSchema, r, plan.conditions)) continue;
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

          if (!plan.orderBy.empty()) {
      std::map<std::string, std::string> aliasMap;
      for (const auto& sel : plan.selectExprs) {
          std::string name = sel.alias.empty() ? (sel.isAggregate ? sel.agg.func + "(" + sel.agg.field + ")" : sel.field) : sel.alias;
          if (!sel.alias.empty()) {
              aliasMap[Lower(sel.field)] = name;
              aliasMap[Lower(sel.alias)] = name;
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

      for (const auto& r : matched) out.push_back(Project(combinedSchema, r, plan.projection));
      return true;
  }

  // Handle JOIN (Nested Loop with support for Left/Right)
  auto createCombined = [&](const Record& rA, const Record& rB) {
      Record c; c.valid = true;
      c.values = rA.values;
      c.values.insert(c.values.end(), rB.values.begin(), rB.values.end());
      return c;
  };
  
  auto matchesVar = [&](const Record& cmb) {
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
              if (matchesVar(cur)) {
                  if (MatchConditions(combinedSchema, cur, plan.conditions)) {
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
               if (MatchConditions(combinedSchema, cur, plan.conditions)) {
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
              if (matchesVar(cur)) {
                  if (MatchConditions(combinedSchema, cur, plan.conditions)) {
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
               if (MatchConditions(combinedSchema, cur, plan.conditions)) {
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

  for (const auto& r : matchedRows) out.push_back(Project(combinedSchema, r, plan.projection));
  return true;
}

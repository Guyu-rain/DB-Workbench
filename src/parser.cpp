#include "parser.h"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <cstring>
#include <memory>


namespace {
std::string ToUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

std::vector<std::string> Split(const std::string& s, char delim) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

std::string Trim(std::string s) {
  auto notSpace = [](unsigned char ch) { return !std::isspace(ch); };
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
  s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
  return s;
}

std::vector<std::string> SplitTopLevel(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    int depth = 0;
    for (char ch : s) {
        if (ch == '(') depth++;
        else if (ch == ')') depth = std::max(0, depth - 1);
        if (ch == delim && depth == 0) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(ch);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
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

size_t FindKeywordTopLevel(const std::string& upperSql, const std::string& keyword, size_t startPos = 0) {
    int depth = 0;
    bool inSingle = false;
    bool inDouble = false;
    bool inBacktick = false;
    for (size_t i = startPos; i + keyword.size() <= upperSql.size(); ++i) {
        char c = upperSql[i];
        if (c == '\'' && !inDouble && !inBacktick) inSingle = !inSingle;
        else if (c == '"' && !inSingle && !inBacktick) inDouble = !inDouble;
        else if (c == '`' && !inSingle && !inDouble) inBacktick = !inBacktick;

        if (!inSingle && !inDouble && !inBacktick) {
            if (c == '(') depth++;
            else if (c == ')') depth = std::max(0, depth - 1);
            if (depth == 0 && upperSql.compare(i, keyword.size(), keyword) == 0) return i;
        }
    }
    return std::string::npos;
}

struct JoinMatch {
    size_t pos = std::string::npos;
    size_t keywordLen = 0;
    JoinType type = JoinType::kInner;
    bool natural = false;
};

bool FindLastJoinTopLevel(const std::string& upperSql, size_t startPos, size_t endPos, JoinMatch& out, int& count) {
    int depth = 0;
    bool inSingle = false;
    bool inDouble = false;
    bool inBacktick = false;
    count = 0;
    out = JoinMatch{};

    struct KeywordSpec {
        const char* text;
        JoinType type;
        bool natural;
    };

    static const KeywordSpec keywords[] = {
        {" NATURAL LEFT JOIN ", JoinType::kLeft, true},
        {" NATURAL RIGHT JOIN ", JoinType::kRight, true},
        {" NATURAL INNER JOIN ", JoinType::kInner, true},
        {" NATURAL JOIN ", JoinType::kInner, true},
        {" LEFT JOIN ", JoinType::kLeft, false},
        {" RIGHT JOIN ", JoinType::kRight, false},
        {" INNER JOIN ", JoinType::kInner, false},
        {" JOIN ", JoinType::kInner, false},
    };

    for (size_t i = startPos; i < endPos; ++i) {
        char c = upperSql[i];
        if (c == '\'' && !inDouble && !inBacktick) inSingle = !inSingle;
        else if (c == '"' && !inSingle && !inBacktick) inDouble = !inDouble;
        else if (c == '`' && !inSingle && !inDouble) inBacktick = !inBacktick;

        if (!inSingle && !inDouble && !inBacktick) {
            if (c == '(') depth++;
            else if (c == ')') depth = std::max(0, depth - 1);
            if (depth == 0) {
                for (const auto& kw : keywords) {
                    size_t len = strlen(kw.text);
                    if (i + len <= endPos && upperSql.compare(i, len, kw.text) == 0) {
                        out.pos = i;
                        out.keywordLen = len;
                        out.type = kw.type;
                        out.natural = kw.natural;
                        ++count;
                        i += len - 1;
                        break;
                    }
                }
            }
        }
    }
    return out.pos != std::string::npos;
}

size_t FindMatchingClosingParen(const std::string& s, size_t openPos);

ReferentialAction ParseReferentialActionToken(const std::string& token, bool& ok) {
    std::string up = ToUpper(Trim(token));
    ok = true;
    if (up == "RESTRICT") return ReferentialAction::kRestrict;
    if (up == "CASCADE") return ReferentialAction::kCascade;
    if (up == "SET NULL") return ReferentialAction::kSetNull;
    ok = false;
    return ReferentialAction::kRestrict;
}

bool ParseTrailingAction(std::string& s, ReferentialAction& action) {
    std::string t = Trim(s);
    std::string up = ToUpper(t);
    const std::string setNullToken = "SET NULL";
    if (up.size() >= setNullToken.size() && up.substr(up.size() - setNullToken.size()) == setNullToken) {
        action = ReferentialAction::kSetNull;
        s = Trim(t.substr(0, t.size() - setNullToken.size()));
        return true;
    }
    if (up.size() >= 8 && up.substr(up.size() - 8) == "RESTRICT") {
        action = ReferentialAction::kRestrict;
        s = Trim(t.substr(0, t.size() - 8));
        return true;
    }
    if (up.size() >= 7 && up.substr(up.size() - 7) == "CASCADE") {
        action = ReferentialAction::kCascade;
        s = Trim(t.substr(0, t.size() - 7));
        return true;
    }
    return false;
}

bool ParseForeignKeyClause(const std::string& input, ForeignKeyDef& out, std::string& err) {
    std::string work = Trim(input);
    std::string up = ToUpper(work);
    if (up.find("CONSTRAINT ") == 0) {
        std::string rest = Trim(work.substr(strlen("CONSTRAINT")));
        size_t sp = rest.find(' ');
        if (sp == std::string::npos) { err = "Invalid CONSTRAINT syntax"; return false; }
        out.name = StripIdentQuotes(Trim(rest.substr(0, sp)));
        work = Trim(rest.substr(sp + 1));
        up = ToUpper(work);
    }
    size_t fkPos = up.find("FOREIGN KEY");
    if (fkPos == std::string::npos) return false;
    size_t colsL = work.find('(', fkPos);
    if (colsL == std::string::npos) { err = "FOREIGN KEY missing columns"; return false; }
    size_t colsR = FindMatchingClosingParen(work, colsL);
    if (colsR == std::string::npos) { err = "FOREIGN KEY columns not closed"; return false; }
    std::string colsPart = work.substr(colsL + 1, colsR - colsL - 1);
    for (const auto& c : SplitTopLevel(colsPart, ',')) {
        std::string col = StripIdentQuotes(Trim(c));
        if (!col.empty()) out.columns.push_back(col);
    }
    std::string afterCols = Trim(work.substr(colsR + 1));
    std::string upAfter = ToUpper(afterCols);
    size_t refPos = upAfter.find("REFERENCES");
    if (refPos == std::string::npos) { err = "FOREIGN KEY missing REFERENCES"; return false; }
    std::string refBody = Trim(afterCols.substr(refPos + strlen("REFERENCES")));
    if (refBody.empty()) { err = "REFERENCES missing table"; return false; }
    size_t refColsL = refBody.find('(');
    if (refColsL == std::string::npos) {
        out.refTable = StripIdentQuotes(Trim(refBody));
    } else {
        out.refTable = StripIdentQuotes(Trim(refBody.substr(0, refColsL)));
        size_t refColsR = FindMatchingClosingParen(refBody, refColsL);
        if (refColsR == std::string::npos) { err = "REFERENCES columns not closed"; return false; }
        std::string refCols = refBody.substr(refColsL + 1, refColsR - refColsL - 1);
        for (const auto& c : SplitTopLevel(refCols, ',')) {
            std::string col = StripIdentQuotes(Trim(c));
            if (!col.empty()) out.refColumns.push_back(col);
        }
        refBody = Trim(refBody.substr(refColsR + 1));
    }
    std::string rest = Trim(refBody);
    auto parseAction = [&](const std::string& key, ReferentialAction& outAction, const std::string& label) -> bool {
        std::string upRest = ToUpper(rest);
        size_t pos = upRest.find(key);
        if (pos == std::string::npos) return true;
        std::string tail = Trim(rest.substr(pos + key.size()));
        size_t nextOn = ToUpper(tail).find(" ON ");
        std::string token = (nextOn == std::string::npos) ? tail : Trim(tail.substr(0, nextOn));
        bool ok = false;
        outAction = ParseReferentialActionToken(token, ok);
        if (!ok) { err = "Invalid " + label + " action"; return false; }
        return true;
    };
    if (!parseAction("ON DELETE", out.onDelete, "ON DELETE")) return false;
    if (!parseAction("ON UPDATE", out.onUpdate, "ON UPDATE")) return false;
    return true;
}
// Helper to find matching closing parenthesis safely
size_t FindMatchingClosingParen(const std::string& s, size_t openPos) {
    int depth = 0;
    for (size_t i = openPos; i < s.length(); ++i) {
        if (s[i] == '(') depth++;
        else if (s[i] == ')') {
            depth--;
            if (depth == 0) return i;
        }
    }
    return std::string::npos;
}

// Helper to try parsing a subquery string e.g. "(SELECT ...)"
// Returns nullptr if not a valid subquery
std::shared_ptr<QueryPlan> ParseSubQueryValues(std::string content) {
    content = Trim(content);
    if (content.size() < 2 || content.front() != '(' || content.back() != ')') {
        return nullptr;
    }
    
    // Check if it looks like a SELECT
    std::string inner = content.substr(1, content.size()-2); // strip outer parens
    std::string up = ToUpper(inner);
    if (up.find("SELECT") == 0 || up.find(" SELECT") == 0) {
         Parser p;
         std::string err;
         ParsedCommand cmd = p.Parse(inner, err);
         if (err.empty() && cmd.type == CommandType::kSelect) {
             auto plan = std::make_shared<QueryPlan>(cmd.query);
             // Ensure sourceTable is populated if parsing set tableName
             if (plan->sourceTable.empty()) plan->sourceTable = cmd.tableName;
             return plan;
         }
    }
    return nullptr;
}

// Helper to find op position ignoring parentheses
size_t FindOp(const std::string& upPart, const std::string& op, size_t startPos = 0) {
    int depth = 0;
    for (size_t i = startPos; i < upPart.size(); ++i) {
        if (upPart[i] == '(') {
            depth++;
        } else if (upPart[i] == ')') {
            if (depth > 0) depth--;
        } else if (depth == 0) {
            // Check op match
            if (i + op.size() <= upPart.size()) {
                if (upPart.substr(i, op.size()) == op) return i;
            }
        }
    }
    return std::string::npos;
}

std::vector<Condition> ParseWhereClause(const std::string& whereClause) {
    std::vector<Condition> conditions;
    if (whereClause.empty()) return conditions;

    // Split by " AND " respecting parentheses and BETWEEN clauses
    std::vector<std::string> parts;
    std::string text = whereClause;
    std::string upper = ToUpper(text);
    
    size_t pos = 0;
    size_t lastPos = 0;
    int depth = 0;
    bool inBetween = false;  // Track if we're inside a BETWEEN clause
    
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '(') depth++;
        else if (text[i] == ')') depth--;
        
        // Check for BETWEEN keyword at depth 0
        if (depth == 0 && i + 8 <= text.size()) {
            if (upper.substr(i, 8) == " BETWEEN") {
                inBetween = true;
            }
        }
        
        // check for AND at top level
        if (depth == 0 && i + 5 <= text.size()) {
             if (upper.substr(i, 5) == " AND ") {
                 // If we're in a BETWEEN clause, this AND belongs to BETWEEN
                 if (inBetween) {
                     inBetween = false;  // This AND closes the BETWEEN clause
                 } else {
                     // This is a condition separator
                     parts.push_back(text.substr(lastPos, i - lastPos));
                     i += 4; // skip " AND "
                     lastPos = i + 1;
                 }
             }
        }
    }
    parts.push_back(text.substr(lastPos));

    for (const auto& rawPart : parts) {
        std::string part = Trim(rawPart);
        if (part.empty()) continue;
        std::string upPart = ToUpper(part);

        // Check BETWEEN
        size_t betweenPos = FindOp(upPart, " BETWEEN ");
        if (betweenPos != std::string::npos) {
            Condition c;
            c.fieldName = Trim(part.substr(0, betweenPos));
            c.op = "BETWEEN";
            
            std::string rangeStr = Trim(part.substr(betweenPos + 9)); // Skip " BETWEEN "
            std::string upRange = ToUpper(rangeStr);
            
            // Find " AND " in the range part
            size_t andPos = upRange.find(" AND ");
            if (andPos != std::string::npos) {
                std::string minVal = Trim(rangeStr.substr(0, andPos));
                std::string maxVal = Trim(rangeStr.substr(andPos + 5));
                
                // Remove quotes if present
                if (minVal.size() >= 2 && minVal.front() == '\'' && minVal.back() == '\'') {
                    minVal = minVal.substr(1, minVal.size() - 2);
                }
                if (maxVal.size() >= 2 && maxVal.front() == '\'' && maxVal.back() == '\'') {
                    maxVal = maxVal.substr(1, maxVal.size() - 2);
                }
                
                c.values.push_back(minVal);
                c.values.push_back(maxVal);
            }
            conditions.push_back(c);
            continue;
        }

        // Check NOT LIKE (must check before LIKE)
        size_t notLikePos = FindOp(upPart, " NOT LIKE ");
        if (notLikePos != std::string::npos) {
            Condition c;
            c.fieldName = Trim(part.substr(0, notLikePos));
            c.op = "NOT LIKE";
            std::string pattern = Trim(part.substr(notLikePos + 10)); // Skip " NOT LIKE "
            
            // Remove quotes if present
            if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
                pattern = pattern.substr(1, pattern.size() - 2);
            }
            c.value = pattern;
            conditions.push_back(c);
            continue;
        }

        // Check NOT EXISTS
        size_t notExistsPos = FindOp(upPart, "NOT EXISTS ");
        if (notExistsPos == 0) {  // Must be at the start
            Condition c;
            c.op = "NOT EXISTS";
            c.fieldName = "";  // EXISTS doesn't have a field
            std::string subPart = Trim(part.substr(11)); // Skip "NOT EXISTS "
            
            if (subPart.size() >= 2 && subPart.front() == '(' && subPart.back() == ')') {
                auto sq = ParseSubQueryValues(subPart);
                if (sq) {
                    c.isSubQuery = true;
                    c.subQueryPlan = sq;
                    conditions.push_back(c);
                    continue;
                }
            }
        }

        // Check EXISTS
        size_t existsPos = FindOp(upPart, "EXISTS ");
        if (existsPos == 0) {  // Must be at the start
            Condition c;
            c.op = "EXISTS";
            c.fieldName = "";  // EXISTS doesn't have a field
            std::string subPart = Trim(part.substr(7)); // Skip "EXISTS "
            
            if (subPart.size() >= 2 && subPart.front() == '(' && subPart.back() == ')') {
                auto sq = ParseSubQueryValues(subPart);
                if (sq) {
                    c.isSubQuery = true;
                    c.subQueryPlan = sq;
                    conditions.push_back(c);
                    continue;
                }
            }
        }

        // Check LIKE
        size_t likePos = FindOp(upPart, " LIKE ");
        if (likePos != std::string::npos) {
            Condition c;
            c.fieldName = Trim(part.substr(0, likePos));
            c.op = "LIKE";
            std::string pattern = Trim(part.substr(likePos + 6)); // Skip " LIKE "
            
            // Remove quotes if present
            if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
                pattern = pattern.substr(1, pattern.size() - 2);
            }
            c.value = pattern;
            conditions.push_back(c);
            continue;
        }

        // Check IN
        size_t inPos = FindOp(upPart, " IN "); // Use the safe finder
        
        if (inPos == std::string::npos) {
             // Try check IN( -- handled by FindOp logic? " IN(" contains " IN"?? No.
             // " IN " requires spaces.
             inPos = FindOp(upPart, " IN(");
        }

        if (inPos != std::string::npos) {
             Condition c;
             c.fieldName = Trim(part.substr(0, inPos));
             c.op = "IN";
             
             size_t parenL = part.find('(', inPos);
             size_t parenR = FindMatchingClosingParen(part, parenL);
             
             if (parenL != std::string::npos && parenR != std::string::npos && parenR > parenL) {
                 std::string valContent = part.substr(parenL, parenR - parenL + 1); // keep parens for subquery check
                 
                 // Check if subquery
                 auto sq = ParseSubQueryValues(valContent);
                 if (sq) {
                     c.isSubQuery = true;
                     c.subQueryPlan = sq;
                 } else {
                     std::string valList = valContent.substr(1, valContent.size()-2);
                     auto vals = Split(valList, ',');
                     for(auto& v : vals) {
                         std::string tv = Trim(v);
                         if(tv.size() >= 2 && tv.front()=='\'' && tv.back()=='\'') tv = tv.substr(1, tv.size()-2);
                         c.values.push_back(tv);
                     }
                     c.value = valList; 
                 }
             }
             conditions.push_back(c);
             continue;
        }

        // Standard ops
        std::vector<std::string> ops = { "<=", ">=", "!=", "=", "<", ">", " CONTAINS " }; 
        bool found = false;
        for (const auto& op : ops) {
            size_t p = FindOp(upPart, op); // USE SAFE FIND
            
            if (p != std::string::npos) {
                Condition c;
                c.fieldName = Trim(part.substr(0, p));
                c.op = Trim(op);
                
                std::string rhs = Trim(part.substr(p + op.length()));
                
                // Check subquery on RHS
                if (rhs.size() >= 2 && rhs.front() == '(' && rhs.back() == ')') {
                    auto sq = ParseSubQueryValues(rhs);
                    if (sq) {
                        c.isSubQuery = true;
                        c.subQueryPlan = sq;
                        c.value = "SUBQUERY"; // Placeholder
                    } else {
                        c.value = rhs;
                    }
                } else {
                    c.value = rhs;
                }

                if (!c.isSubQuery && c.value.size() >= 2 && c.value.front() == '\'' && c.value.back() == '\'') {
                    c.value = c.value.substr(1, c.value.size() - 2);
                }
                
                conditions.push_back(c);
                found = true;
                break;
            }
        }
    }
    return conditions;
}

std::string TrimQuotes(std::string s) {
    if (s.size() >= 2) {
        if ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

std::string StripSqlComments(const std::string& sql) {
    std::string out;
    out.reserve(sql.size());
    bool inSingle = false;
    bool inDouble = false;

    for (size_t i = 0; i < sql.size(); ++i) {
        char c = sql[i];
        char n = (i + 1 < sql.size()) ? sql[i + 1] : '\0';

        if (!inSingle && !inDouble) {
            if (c == '-' && n == '-') {
                i += 2;
                while (i < sql.size() && sql[i] != '\n' && sql[i] != '\r') ++i;
                if (i < sql.size()) out.push_back(' ');
                continue;
            }
            if (c == '#') {
                ++i;
                while (i < sql.size() && sql[i] != '\n' && sql[i] != '\r') ++i;
                if (i < sql.size()) out.push_back(' ');
                continue;
            }
            if (c == '/' && n == '*') {
                i += 2;
                while (i + 1 < sql.size() && !(sql[i] == '*' && sql[i + 1] == '/')) ++i;
                if (i + 1 < sql.size()) ++i; // consume '/'
                out.push_back(' ');
                continue;
            }
        }

        if (c == '\'' && !inDouble) inSingle = !inSingle;
        if (c == '"' && !inSingle) inDouble = !inDouble;

        out.push_back(c);
    }

    return out;
}

bool IsAggregateFunc(const std::string& s, std::string& funcOut) {
    std::string up = ToUpper(Trim(s));
    if (up == "COUNT" || up == "SUM" || up == "AVG" || up == "MIN" || up == "MAX") {
        funcOut = up;
        return true;
    }
    return false;
}

}

ParsedCommand Parser::Parse(const std::string& rawSql, std::string& err) {
  std::string noComment = StripSqlComments(rawSql);

  // Normalize: Trim and replace internal whitespace with spaces to handle newlines
  std::string sql = Trim(noComment);
  
  // Remove trailing semicolon if any
  if (!sql.empty() && sql.back() == ';') {
      sql.pop_back();
      sql = Trim(sql);
  }

  for (char &c : sql) {
      if (std::isspace(static_cast<unsigned char>(c))) c = ' ';
  }
  // Collapse consecutive spaces to a single space for robust keyword parsing.
  {
      std::string compact;
      compact.reserve(sql.size());
      bool lastSpace = false;
      for (char c : sql) {
          if (c == ' ') {
              if (!lastSpace) compact.push_back(c);
              lastSpace = true;
          } else {
              compact.push_back(c);
              lastSpace = false;
          }
      }
      sql = Trim(compact);
  }

  ParsedCommand cmd;
  std::string upper = ToUpper(sql);
  // CREATE DATABASE xxx
  if (upper.find("CREATE DATABASE") == 0) {
      cmd.type = CommandType::kCreateDatabase;

      std::string namePart = sql.substr(strlen("CREATE DATABASE"));
      cmd.dbName = Trim(namePart);

      if (cmd.dbName.empty()) {
          err = "Database name is required";
      }

      return cmd;
  }

  // USE database
  if (upper.find("USE ") == 0) {
      cmd.type = CommandType::kUseDatabase;

      std::string namePart = sql.substr(strlen("USE"));
      cmd.dbName = Trim(namePart);

      if (cmd.dbName.empty()) {
          err = "Database name is required";
      }

      return cmd;
  }

  // DROP DATABASE xxx
  if (upper.find("DROP DATABASE") == 0) {
      cmd.type = CommandType::kDropDatabase;
      std::string namePart = Trim(sql.substr(strlen("DROP DATABASE")));
      if (ParseTrailingAction(namePart, cmd.action)) cmd.actionSpecified = true;
      cmd.dbName = Trim(namePart);
      if (cmd.dbName.empty()) {
          err = "Database name is required";
      }
      return cmd;
  }
  
  // BACKUP DATABASE dbName TO 'path'
  if (upper.find("BACKUP DATABASE") == 0) {
      cmd.type = CommandType::kBackup;
      std::string rest = Trim(sql.substr(strlen("BACKUP DATABASE")));
      
      auto toPos = ToUpper(rest).find(" TO ");
      if (toPos == std::string::npos) {
          err = "Syntax error: expected TO";
          return cmd;
      }
      
      cmd.dbName = Trim(rest.substr(0, toPos));
      cmd.backupPath = Trim(TrimQuotes(Trim(rest.substr(toPos + 4))));
      
      if (cmd.dbName.empty() || cmd.backupPath.empty()) {
          err = "Database name and path required";
      }
      return cmd;
  }

  // RESTORE DATABASE dbName FROM 'backup'
  if (upper.find("RESTORE DATABASE") == 0) {
      cmd.type = CommandType::kRestore;
      std::string rest = Trim(sql.substr(strlen("RESTORE DATABASE")));

      auto fromPos = ToUpper(rest).find(" FROM ");
      if (fromPos == std::string::npos) {
          err = "Syntax error: expected FROM";
          return cmd;
      }

      cmd.dbName = Trim(rest.substr(0, fromPos));
      cmd.backupPath = Trim(TrimQuotes(Trim(rest.substr(fromPos + 6))));

      if (cmd.dbName.empty() || cmd.backupPath.empty()) {
          err = "Database name and backup required";
      }
      return cmd;
  }

    // TCL: BEGIN / START TRANSACTION
    if (upper == "BEGIN" || upper == "BEGIN TRANSACTION" || upper == "START TRANSACTION") {
        cmd.type = CommandType::kBegin;
        return cmd;
    }
  if (upper == "START TRANSACTION") {
      cmd.type = CommandType::kBegin;
      return cmd;
  }

  // TCL: COMMIT / ROLLBACK
  if (upper == "COMMIT") {
      cmd.type = CommandType::kCommit;
      return cmd;
  }
  if (upper == "ROLLBACK") {
      cmd.type = CommandType::kRollback;
      return cmd;
  }

  // TCL: SAVEPOINT
  if (upper.find("SAVEPOINT") == 0) {
      cmd.type = CommandType::kSavepoint;
      cmd.savepointName = Trim(sql.substr(strlen("SAVEPOINT")));
      if (cmd.savepointName.empty()) err = "SAVEPOINT name required";
      return cmd;
  }

  // TCL: ROLLBACK TO [SAVEPOINT] name
  if (upper.find("ROLLBACK TO") == 0) {
      cmd.type = CommandType::kRollbackTo;
      std::string rest = Trim(sql.substr(strlen("ROLLBACK TO")));
      std::string upRest = ToUpper(rest);
      if (upRest.find("SAVEPOINT") == 0) {
          rest = Trim(rest.substr(strlen("SAVEPOINT")));
      }
      cmd.savepointName = rest;
      if (cmd.savepointName.empty()) err = "SAVEPOINT name required";
      return cmd;
  }

  // TCL: RELEASE SAVEPOINT name
  if (upper.find("RELEASE SAVEPOINT") == 0) {
      cmd.type = CommandType::kRelease;
      cmd.savepointName = Trim(sql.substr(strlen("RELEASE SAVEPOINT")));
      if (cmd.savepointName.empty()) err = "SAVEPOINT name required";
      return cmd;
  }

  // TCL: CHECKPOINT
  if (upper == "CHECKPOINT") {
      cmd.type = CommandType::kCheckpoint;
      return cmd;
  }

  // DCL: CREATE USER
  if (upper.find("CREATE USER") == 0) {
      cmd.type = CommandType::kCreateUser;
      size_t byPos = upper.find(" IDENTIFIED BY ");
      if (byPos == std::string::npos) {
          err = "Syntax error: expected IDENTIFIED BY";
          return cmd;
      }
      std::string userPart = Trim(sql.substr(11, byPos - 11));
      std::string passPart = Trim(sql.substr(byPos + 15));
      
      // Strip quotes if present
      if (userPart.size()>=2 && ((userPart.front()=='\'' && userPart.back()=='\'') || (userPart.front()=='"' && userPart.back()=='"')))
          userPart = userPart.substr(1, userPart.size()-2);
      if (passPart.size()>=2 && ((passPart.front()=='\'' && passPart.back()=='\'') || (passPart.front()=='"' && passPart.back()=='"')))
          passPart = passPart.substr(1, passPart.size()-2);

      cmd.username = userPart;
      cmd.password = passPart;
      return cmd;
  }

  // DCL: DROP USER
  if (upper.find("DROP USER") == 0) {
      cmd.type = CommandType::kDropUser;
      std::string userPart = Trim(sql.substr(9));
      if (userPart.size()>=2 && ((userPart.front()=='\'' && userPart.back()=='\'') || (userPart.front()=='"' && userPart.back()=='"')))
          userPart = userPart.substr(1, userPart.size()-2);
      cmd.username = userPart;
      return cmd;
  }

  // DCL: GRANT
  if (upper.find("GRANT") == 0) {
      cmd.type = CommandType::kGrant;
      size_t onPos = upper.find(" ON ");
      size_t toPos = upper.find(" TO ");
      if (onPos == std::string::npos || toPos == std::string::npos) {
          err = "Syntax error: usage GRANT <privs> ON <table> TO <user>";
          return cmd;
      }
      
      std::string privStr = Trim(sql.substr(5, onPos - 5));
      std::string tableStr = Trim(sql.substr(onPos + 4, toPos - (onPos + 4)));
      std::string userStr = Trim(sql.substr(toPos + 4));

      cmd.privileges = Split(privStr, ',');
      for(auto& s : cmd.privileges) {
          s = Trim(s);
          if (ToUpper(s) == "ALL") { std::vector<std::string> all = {"SELECT","INSERT","UPDATE","DELETE","CREATE","DROP"}; cmd.privileges = all; break; }
      }
      
      cmd.tableName = tableStr;
      
      if (userStr.size()>=2 && ((userStr.front()=='\'' && userStr.back()=='\'') || (userStr.front()=='"' && userStr.back()=='"')))
          userStr = userStr.substr(1, userStr.size()-2);
      cmd.username = userStr;
      return cmd;
  }

  // DCL: REVOKE
  if (upper.find("REVOKE") == 0) {
      cmd.type = CommandType::kRevoke;
      size_t onPos = upper.find(" ON ");
      size_t fromPos = upper.find(" FROM ");
      if (onPos == std::string::npos || fromPos == std::string::npos) {
          err = "Syntax error: usage REVOKE <privs> ON <table> FROM <user>";
          return cmd;
      }
      std::string privStr = Trim(sql.substr(6, onPos - 6));
      std::string tableStr = Trim(sql.substr(onPos + 4, fromPos - (onPos + 4)));
      std::string userStr = Trim(sql.substr(fromPos + 6));
      
      cmd.privileges = Split(privStr, ',');
      for(auto& s : cmd.privileges) {
          s = Trim(s);
          if (ToUpper(s) == "ALL") { std::vector<std::string> all = {"SELECT","INSERT","UPDATE","DELETE","CREATE","DROP"}; cmd.privileges = all; break; }
      }

      cmd.tableName = tableStr;

      if (userStr.size()>=2 && ((userStr.front()=='\'' && userStr.back()=='\'') || (userStr.front()=='"' && userStr.back()=='"')))
          userStr = userStr.substr(1, userStr.size()-2);
      cmd.username = userStr;
      return cmd;
  }

  // CREATE [UNIQUE] INDEX idxName ON tableName (fieldName)
  if (upper.find("CREATE") == 0 && upper.find("INDEX") != std::string::npos) {
      if (upper.find("CREATE INDEX") == 0) {
           cmd.type = CommandType::kCreateIndex;
           cmd.isUnique = false;
      } else if (upper.find("CREATE UNIQUE INDEX") == 0) {
           cmd.type = CommandType::kCreateIndex;
           cmd.isUnique = true;
      } else {
          // Not an index command or handled elsewhere
      }

      if (cmd.type == CommandType::kCreateIndex) {
          std::string prefix = cmd.isUnique ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
          std::string rest = sql.substr(strlen(prefix.c_str()));
          
          auto onPos = ToUpper(rest).find(" ON ");
          if (onPos == std::string::npos) {
              err = "Syntax error: expected ON";
              return cmd;
          }
          
          cmd.indexName = Trim(rest.substr(0, onPos)); 
          
          std::string afterOn = rest.substr(onPos + 4); 
          auto parenL = afterOn.find('(');
          auto parenR = afterOn.rfind(')');
          
          if (parenL == std::string::npos || parenR == std::string::npos || parenR < parenL) {
              err = "Syntax error: expected (column)";
              return cmd;
          }
          
          cmd.tableName = Trim(afterOn.substr(0, parenL));
          cmd.fieldName = Trim(afterOn.substr(parenL + 1, parenR - parenL - 1));
          
          return cmd;
      }
  }

  // ALTER TABLE
  if (upper.find("ALTER TABLE") == 0) {
      cmd.type = CommandType::kAlter;
      std::string rest = Trim(sql.substr(strlen("ALTER TABLE")));
      size_t firstSpace = rest.find(' ');
      if (firstSpace == std::string::npos) { err = "Incomplete ALTER TABLE"; return cmd; }
      
      cmd.tableName = StripIdentQuotes(Trim(rest.substr(0, firstSpace)));
      std::string action = Trim(rest.substr(firstSpace + 1));
      std::string upAction = ToUpper(action);

      // ADD ...
      if (upAction.find("ADD") == 0) {
           // ADD INDEX
           if (upAction.find("ADD INDEX") == 0) {
               cmd.alterOp = AlterOperation::kAddIndex;
               std::string body = Trim(action.substr(9));
               
               size_t openParen = body.find('(');
               if (openParen == std::string::npos) { err="Missing ( for INDEX"; return cmd; }
               
               if (openParen > 0) {
                  cmd.indexName = Trim(body.substr(0, openParen));
               }
               
               size_t closeParen = body.find(')', openParen);
               if (closeParen == std::string::npos) { err="Missing ) for INDEX"; return cmd; }
               
               cmd.fieldName = Trim(body.substr(openParen + 1, closeParen - openParen - 1));
               return cmd;
           }
           
           // ADD CONSTRAINT / ADD FOREIGN KEY
           if (upAction.find("ADD CONSTRAINT") == 0 || upAction.find("ADD FOREIGN KEY") == 0) {
                cmd.alterOp = AlterOperation::kAddConstraint;
                std::string fkBody = Trim(action.substr(3));
                ForeignKeyDef fk;
                if (!ParseForeignKeyClause(fkBody, fk, err)) return cmd;
                cmd.fkDef = fk;
                return cmd;
           }
           
           // ADD COLUMN
           size_t offset = 3; // "ADD"
           if (upAction.find("ADD COLUMN") == 0) offset = 10;
           
           cmd.alterOp = AlterOperation::kAddColumn;
           std::string colDef = Trim(action.substr(offset));
           
           // Check for AFTER / FIRST
           size_t afterPos = ToUpper(colDef).find(" AFTER ");
           size_t firstPos = ToUpper(colDef).find(" FIRST");
           
           if (afterPos != std::string::npos) {
               cmd.extraInfo = Trim(colDef.substr(afterPos + 7));
               colDef = Trim(colDef.substr(0, afterPos));
           } else if (firstPos != std::string::npos) {
               cmd.extraInfo = "FIRST";
               colDef = Trim(colDef.substr(0, firstPos));
           }
           
           auto parts = Split(colDef, ' ');
           if (parts.size() < 2) { err = "Invalid field definition"; return cmd; }
           
           cmd.columnDef.name = parts[0];
           cmd.columnDef.type = parts[1];
           for (size_t i = 2; i < parts.size(); ++i) {
                  std::string p = ToUpper(parts[i]);
                  if (p == "PRIMARY" && i+1 < parts.size() && ToUpper(parts[i+1])=="KEY") {
                      cmd.columnDef.isKey = true; cmd.columnDef.nullable = false; ++i;
                  } else if (p == "NOT" && i+1 < parts.size() && ToUpper(parts[i+1])=="NULL") {
                      cmd.columnDef.nullable = false; ++i;
                  }
           }
           return cmd;
      }
      
      // DROP ...
      if (upAction.find("DROP") == 0) {
           if (upAction.find("DROP COLUMN") == 0) {
               cmd.alterOp = AlterOperation::kDropColumn;
               cmd.fieldName = Trim(action.substr(11));
               return cmd;
           }
           if (upAction.find("DROP INDEX") == 0) {
               cmd.alterOp = AlterOperation::kDropIndex;
               cmd.indexName = Trim(action.substr(10));
               return cmd;
           }
           if (upAction.find("DROP FOREIGN KEY") == 0) {
               cmd.alterOp = AlterOperation::kDropConstraint;
               cmd.indexName = Trim(action.substr(16));
               return cmd;
           }
           if (upAction.find("DROP CONSTRAINT") == 0) {
               cmd.alterOp = AlterOperation::kDropConstraint;
               cmd.indexName = Trim(action.substr(15));
               return cmd;
           }
           // Fallback for DROP colName
           cmd.alterOp = AlterOperation::kDropColumn;
           cmd.fieldName = Trim(action.substr(4));
           return cmd;
      }
      
      // MODIFY ...
      if (upAction.find("MODIFY") == 0) {
           cmd.alterOp = AlterOperation::kModifyColumn;
           size_t offset = 6;
           if (upAction.find("MODIFY COLUMN") == 0) offset = 13;
           std::string colDef = Trim(action.substr(offset));
           
           auto parts = Split(colDef, ' ');
           if (parts.size() < 2) { err = "Invalid field definition"; return cmd; }
           
           cmd.columnDef.name = parts[0];
           cmd.columnDef.type = parts[1];
           for (size_t i = 2; i < parts.size(); ++i) {
                  std::string p = ToUpper(parts[i]);
                  if (p == "PRIMARY" && i+1 < parts.size() && ToUpper(parts[i+1])=="KEY") {
                      cmd.columnDef.isKey = true; cmd.columnDef.nullable = false; ++i;
                  } else if (p == "NOT" && i+1 < parts.size() && ToUpper(parts[i+1])=="NULL") {
                      cmd.columnDef.nullable = false; ++i;
                  }
           }
           return cmd;
      }

      // RENAME ...
      if (upAction.find("RENAME") == 0) {
          if (upAction.find("RENAME COLUMN") == 0) {
              cmd.alterOp = AlterOperation::kRenameColumn;
              std::string body = Trim(action.substr(13));
              size_t toPos = ToUpper(body).find(" TO ");
              if (toPos == std::string::npos) { err = "RENAME COLUMN missing TO"; return cmd; }
              cmd.fieldName = Trim(body.substr(0, toPos)); 
              cmd.newName = Trim(body.substr(toPos + 4));
              return cmd;
          }
          if (upAction.find("RENAME TO") == 0) {
               cmd.alterOp = AlterOperation::kRenameTable;
               cmd.newName = Trim(action.substr(9));
               return cmd;
          }
      }
      
      err = "Unknown ALTER operation";
      return cmd;
  }

  // DROP INDEX indexName ON tableName
  // We treat indexName as fieldName for simplicity in this implementation
  if (upper.find("DROP INDEX") == 0) {
      cmd.type = CommandType::kDropIndex;
      std::string rest = sql.substr(strlen("DROP INDEX"));
      
      auto onPos = ToUpper(rest).find(" ON ");
      if (onPos == std::string::npos) {
          err = "Syntax error: expected ON";
          return cmd;
      }
      
      cmd.fieldName = Trim(rest.substr(0, onPos)); // Using indexName as fieldName
      cmd.tableName = Trim(rest.substr(onPos + 4));
      
      return cmd;
  }
  
  // SHOW INDEX FROM tableName
  if (upper.find("SHOW INDEX") == 0) {
      cmd.type = CommandType::kShowIndexes;
      std::string rest = sql.substr(strlen("SHOW INDEX"));
      
      // Handle "FROM"
      auto fromPos = ToUpper(rest).find(" FROM ");
      if (fromPos == std::string::npos) {
          // Maybe it was "SHOW INDEXES FROM"?
          if (ToUpper(rest).find("ES FROM ") != std::string::npos) {
               // handle logic if needed, but simple "SHOW INDEX FROM" is standard MySQL
          }
          err = "Syntax error: expected FROM";
          return cmd;
      }
      
      cmd.tableName = Trim(rest.substr(fromPos + 6));
      return cmd;
  }

  // SHOW TABLES [FROM db]
  if (upper.find("SHOW TABLES") == 0) {
      cmd.type = CommandType::kShowTables;
      std::string rest = Trim(sql.substr(strlen("SHOW TABLES")));
      std::string upRest = ToUpper(rest);
      if (upRest.find("FROM ") == 0) {
          cmd.dbName = Trim(rest.substr(5));
      }
      return cmd;
  }

  // CREATE [OR REPLACE] VIEW view_name [(col,...)] AS SELECT ...
  if (upper.find("CREATE VIEW") == 0 || upper.find("CREATE OR REPLACE VIEW") == 0) {
      cmd.type = CommandType::kCreateView;
      cmd.viewOrReplace = (upper.find("CREATE OR REPLACE VIEW") == 0);
      size_t prefixLen = cmd.viewOrReplace ? strlen("CREATE OR REPLACE VIEW") : strlen("CREATE VIEW");
      std::string rest = Trim(sql.substr(prefixLen));
      auto asPos = ToUpper(rest).find(" AS ");
      if (asPos == std::string::npos) { err = "CREATE VIEW missing AS"; return cmd; }
      std::string namePart = Trim(rest.substr(0, asPos));
      std::string body = Trim(rest.substr(asPos + 4));
      if (body.empty()) { err = "CREATE VIEW missing SELECT body"; return cmd; }

      size_t lp = namePart.find('(');
      if (lp != std::string::npos) {
          size_t rp = FindMatchingClosingParen(namePart, lp);
          if (rp == std::string::npos) { err = "View column list not closed"; return cmd; }
          std::string cols = namePart.substr(lp + 1, rp - lp - 1);
          for (const auto& c : SplitTopLevel(cols, ',')) {
              std::string col = StripIdentQuotes(Trim(c));
              if (!col.empty()) cmd.viewColumns.push_back(col);
          }
          namePart = Trim(namePart.substr(0, lp));
      }
      cmd.viewName = StripIdentQuotes(namePart);
      cmd.viewSql = body;

      Parser subParser;
      std::string subErr;
      ParsedCommand sub = subParser.Parse(body, subErr);
      if (!subErr.empty() || sub.type != CommandType::kSelect) {
          err = "CREATE VIEW requires a SELECT statement";
          return cmd;
      }
      cmd.viewQuery = sub.query;
      return cmd;
  }
  
  // ====== REPLACE the whole CREATE TABLE branch in Parser::Parse() ======
  if (upper.find("CREATE TABLE") == 0) {
      cmd.type = CommandType::kCreate;

      // Support:
      // 1) CREATE TABLE t (id int, name char[32])
      // 2) CREATE TABLE t (id int primary key, name char(32) not null) INTO dbname
      auto intoPos = upper.find("INTO");
      std::string createBody = (intoPos == std::string::npos)
          ? sql.substr(strlen("CREATE TABLE"))
          : sql.substr(strlen("CREATE TABLE"), intoPos - strlen("CREATE TABLE"));

      auto parenL = createBody.find('(');
      auto parenR = createBody.rfind(')');
      if (parenL == std::string::npos || parenR == std::string::npos || parenR <= parenL) {
          err = "Invalid field list";
          return cmd;
      }

      cmd.tableName = Trim(createBody.substr(0, parenL));
      std::string fieldList = createBody.substr(parenL + 1, parenR - parenL - 1);

      auto fields = SplitTopLevel(fieldList, ',');
      for (auto& raw : fields) {
          std::string fstr = Trim(raw);
          if (fstr.empty()) continue;
          ForeignKeyDef fk;
          std::string fkErr;
          if (ParseForeignKeyClause(fstr, fk, fkErr)) {
              cmd.schema.foreignKeys.push_back(fk);
              continue;
          }

          // Tokenize by spaces, but keep type token like char(32) or char[32] together
          auto parts = Split(fstr, ' ');
          if (parts.size() < 2) continue;

          Field field;
          field.name = parts[0];
          field.type = parts[1];
          field.size = 0;
          field.isKey = false;
          field.nullable = true;
          field.valid = true;

          // Parse constraints (very simple)
          // supports: PRIMARY KEY, NOT NULL
          for (size_t i = 2; i < parts.size(); ++i) {
              std::string p = ToUpper(parts[i]);
              if (p == "PRIMARY") {
                  if (i + 1 < parts.size() && ToUpper(parts[i + 1]) == "KEY") {
                      field.isKey = true;
                      field.nullable = false;
                      ++i;
                  }
              }
              else if (p == "NOT") {
                  if (i + 1 < parts.size() && ToUpper(parts[i + 1]) == "NULL") {
                      field.nullable = false;
                      ++i;
                  }
              }
          }

          cmd.schema.fields.push_back(field);
      }

      cmd.schema.tableName = cmd.tableName;

      if (intoPos != std::string::npos) {
          std::string afterInto = sql.substr(intoPos + strlen("INTO"));
          cmd.dbName = Trim(afterInto);
      }
      else {
          cmd.dbName = "default";
      }

      return cmd;
  }


    if (upper.find("INSERT INTO") == 0) {
        cmd.type = CommandType::kInsert;
        // INSERT INTO table VALUES(a,b,c) [IN db]
        // Also supports: INSERT INTO table (col1, col2) VALUES ...
        
        auto valuesPos = ToUpper(sql).find("VALUES");
        if (valuesPos == std::string::npos) {
            err = "Invalid INSERT: missing VALUES";
            return cmd;
        }

        std::string tablePart = sql.substr(strlen("INSERT INTO"), valuesPos - strlen("INSERT INTO"));
        tablePart = Trim(tablePart);

        // Check if tablePart contains column list like "users (id, name)"
        auto parenOpen = tablePart.find('(');
        if (parenOpen != std::string::npos) {
            // Extract just the table name
            cmd.tableName = Trim(tablePart.substr(0, parenOpen));
            // Note: We currently ignore the column list and assume values are in schema order.
            // A full implementation would parse columns and reorder 'values' accordingly.
        } else {
            cmd.tableName = tablePart;
        }

        // ... VALUES ...

        size_t currentPos = valuesPos;
        while (currentPos < sql.size()) {
            size_t parenL = sql.find('(', currentPos);
            if (parenL == std::string::npos) break;

            size_t parenR = sql.find(')', parenL); 
            if (parenR == std::string::npos) {
                err = "Missing closing parenthesis";
                return cmd;
            }

            std::string valueList = sql.substr(parenL + 1, parenR - parenL - 1);
            auto vals = Split(valueList, ',');
            Record rec;
            for (auto& v : vals) rec.values.push_back(TrimQuotes(Trim(v)));
            cmd.records.push_back(rec);
            
            currentPos = parenR + 1;
            
            size_t nextComma = sql.find(',', currentPos);
            size_t nextIn = ToUpper(sql).find(" IN ", currentPos);
            
            if (nextIn != std::string::npos && (nextComma == std::string::npos || nextIn < nextComma)) {
                break;
            }

            if (nextComma != std::string::npos) {
                 bool isCommaNext = true;
                 for(size_t k = currentPos; k < nextComma; ++k) {
                     if(!isspace(sql[k])) { isCommaNext = false; break; }
                 }
                 if(isCommaNext) {
                     currentPos = nextComma + 1;
                     continue;
                 }
            }
            break;
        }

        std::string suffix = sql.substr(currentPos);
        auto inPos = ToUpper(suffix).find(" IN ");
        if (inPos != std::string::npos) {
             cmd.dbName = Trim(suffix.substr(inPos + 4));
        }

        if(cmd.records.empty()) {
            err = "No values found";
        }
        return cmd;
    }

    if (upper.find("DELETE FROM") == 0) {
        cmd.type = CommandType::kDelete;
        // DELETE FROM table WHERE field = value
        std::string rest = Trim(sql.substr(strlen("DELETE FROM")));
        if (ParseTrailingAction(rest, cmd.action)) cmd.actionSpecified = true;
        auto wherePos = ToUpper(rest).find("WHERE");
        
        if (wherePos == std::string::npos) {
             cmd.tableName = Trim(rest);
        } else {
             cmd.tableName = Trim(rest.substr(0, wherePos));
             std::string condPart = rest.substr(wherePos + strlen("WHERE"));
             cmd.conditions = ParseWhereClause(condPart);
        }
        return cmd;
    }

    if (upper.find("UPDATE") == 0) {
        cmd.type = CommandType::kUpdate;
        // UPDATE table SET a=v, b=v WHERE field=val
        auto setPos = upper.find("SET");
        if (setPos == std::string::npos) {
            err = "UPDATE missing SET";
            return cmd;
        }
        cmd.tableName = Trim(sql.substr(strlen("UPDATE"), setPos - strlen("UPDATE")));
        
        std::string afterSet = sql.substr(setPos + strlen("SET"));
        auto wherePos = ToUpper(afterSet).find("WHERE");
        std::string assignPart = (wherePos == std::string::npos) ? afterSet : afterSet.substr(0, wherePos);
        
        auto pairs = Split(assignPart, ',');
        for (auto& p : pairs) {
            auto eq = p.find('=');
            if (eq != std::string::npos) {
                cmd.assignments.push_back({Trim(p.substr(0, eq)), TrimQuotes(Trim(p.substr(eq + 1)))});
            }
        }

        if (wherePos != std::string::npos) {
             std::string condPart = afterSet.substr(wherePos + strlen("WHERE"));
             cmd.conditions = ParseWhereClause(condPart);
        }
        return cmd;
    }

    if (upper.find("DROP TABLE") == 0) {
        cmd.type = CommandType::kDrop;
        std::string rest = Trim(sql.substr(strlen("DROP TABLE")));
        if (ParseTrailingAction(rest, cmd.action)) cmd.actionSpecified = true;
        cmd.tableName = StripIdentQuotes(Trim(rest));
        return cmd;
    }

    if (upper.find("DROP VIEW") == 0) {
        cmd.type = CommandType::kDropView;
        std::string rest = Trim(sql.substr(strlen("DROP VIEW")));
        std::string upRest = ToUpper(rest);
        if (upRest.find("IF EXISTS") == 0) {
            cmd.ifExists = true;
            rest = Trim(rest.substr(strlen("IF EXISTS")));
        }
        cmd.viewName = StripIdentQuotes(rest);
        if (cmd.viewName.empty() && !cmd.ifExists) {
            err = "View name is required";
        }
        return cmd;
    }
    
    if (upper.find("RENAME TABLE") == 0) {
        cmd.type = CommandType::kRename;
        // RENAME TABLE old TO new
        std::string rest = sql.substr(strlen("RENAME TABLE"));
        auto toPos = ToUpper(rest).find(" TO ");
        if (toPos == std::string::npos) {
             err = "RENAME syntax: RENAME TABLE old TO new";
             return cmd;
        }
        cmd.tableName = StripIdentQuotes(Trim(rest.substr(0, toPos)));
        cmd.newName = StripIdentQuotes(Trim(rest.substr(toPos + 4)));
        return cmd;
    }

    if (upper.find("SELECT") == 0) {
        cmd.type = CommandType::kSelect;

        std::string upperSql = ToUpper(sql);
        size_t fromPos = FindKeywordTopLevel(upperSql, " FROM ");
        if (fromPos == std::string::npos) { err = "Missing FROM"; return cmd; }

        // 1. Parse Projection and Aliases
        std::string projStr = sql.substr(6, fromPos - 6);
        auto projParts = Split(projStr, ',');
        for (auto& p : projParts) {
            std::string cur = Trim(p);
            std::string upperCur = ToUpper(cur);
            size_t asPos = upperCur.find(" AS ");
            std::string expr;
            std::string alias;
            if (asPos != std::string::npos) {
                expr = Trim(cur.substr(0, asPos));
                alias = Trim(cur.substr(asPos + 4));
            } else {
                // If substring " " exists after trimming, might be implicit alias.
                size_t spacePos = cur.rfind(' ');
                if (spacePos != std::string::npos) {
                    expr = Trim(cur.substr(0, spacePos));
                    alias = Trim(cur.substr(spacePos + 1));
                } else {
                    expr = cur;
                }
            }

            // Detect aggregate function: FUNC(expr)
            bool isAgg = false;
            bool isSubQ = false;
            AggregateExpr agg;
            std::shared_ptr<QueryPlan> subQ;
            
            {
                size_t lp = expr.find('(');
                size_t rp = expr.rfind(')');
                if (lp != std::string::npos && rp != std::string::npos && rp > lp) {
                    std::string funcName;
                    if (IsAggregateFunc(expr.substr(0, lp), funcName)) {
                        isAgg = true;
                        agg.func = funcName;
                        agg.field = Trim(expr.substr(lp + 1, rp - lp - 1));
                        if (agg.field.empty()) agg.field = "*";
                        agg.alias = alias;
                    }
                }
            }
            
            // Check for subquery in SELECT list
            if (!isAgg && expr.size() >= 2 && expr.front() == '(' && expr.back() == ')') {
                auto sq = ParseSubQueryValues(expr);
                if (sq) {
                    isSubQ = true;
                    subQ = sq;
                }
            }

            SelectExpr sel;
            sel.isAggregate = isAgg;
            sel.alias = alias;
            sel.isSubQuery = isSubQ;
            sel.subQueryPlan = subQ;
            
            if (isAgg) {
                sel.agg = agg;
                sel.field = expr;
                cmd.query.aggregates.push_back(agg);
            } else if (isSubQ) {
                sel.field = expr;
                // Don't add to projection for subquery
            } else {
                sel.field = expr;
                cmd.query.projection.push_back(expr);
                cmd.query.projectionAliases.push_back(alias);
            }
            cmd.query.selectExprs.push_back(sel);
        }

        // 2. Parse FROM and JOIN
        size_t startRest = fromPos + 6;
        size_t joinPos = std::string::npos;
        size_t wherePos = FindKeywordTopLevel(upperSql, " WHERE ", startRest);
        size_t groupPos = FindKeywordTopLevel(upperSql, " GROUP BY ", startRest);
        size_t havingPos = FindKeywordTopLevel(upperSql, " HAVING ", startRest);
        size_t orderPos = FindKeywordTopLevel(upperSql, " ORDER BY ", startRest);
        size_t bareJoin = std::string::npos;
        size_t endFrom = sql.size();
        if (wherePos != std::string::npos && wherePos < endFrom) endFrom = wherePos;
        if (groupPos != std::string::npos && groupPos < endFrom) endFrom = groupPos;
        if (havingPos != std::string::npos && havingPos < endFrom) endFrom = havingPos;
        if (orderPos != std::string::npos && orderPos < endFrom) endFrom = orderPos;

        JoinMatch lastJoin;
        int joinCount = 0;
        bool hasJoin = FindLastJoinTopLevel(upperSql, startRest, endFrom, lastJoin, joinCount);
        bool hasMultiJoin = hasJoin && joinCount > 1;

        size_t naturalJoin = FindKeywordTopLevel(upperSql, " NATURAL JOIN ", startRest);
        size_t naturalLeft = FindKeywordTopLevel(upperSql, " NATURAL LEFT JOIN ", startRest);
        size_t naturalRight = FindKeywordTopLevel(upperSql, " NATURAL RIGHT JOIN ", startRest);
        size_t naturalInner = FindKeywordTopLevel(upperSql, " NATURAL INNER JOIN ", startRest);
        size_t leftJoin = FindKeywordTopLevel(upperSql, " LEFT JOIN ", startRest);
        size_t rightJoin = FindKeywordTopLevel(upperSql, " RIGHT JOIN ", startRest);
        size_t innerJoin = FindKeywordTopLevel(upperSql, " INNER JOIN ", startRest);
        size_t justJoin = FindKeywordTopLevel(upperSql, " JOIN ", startRest);

        // Find the effective JOIN clause (ignoring keywords inside WHERE if any - simple check)
        auto isValidJoin = [&](size_t pos) { 
            if (pos == std::string::npos) return false;
            if (wherePos != std::string::npos && pos > wherePos) return false;
            if (groupPos != std::string::npos && pos > groupPos) return false;
            if (havingPos != std::string::npos && pos > havingPos) return false;
            if (orderPos != std::string::npos && pos > orderPos) return false;
            return true;
        };

        if (isValidJoin(naturalLeft)) {
            joinPos = naturalLeft;
            cmd.query.joinType = JoinType::kLeft;
            cmd.query.isNaturalJoin = true;
            bareJoin = naturalLeft + 13; // " NATURAL LEFT" -> point to " JOIN"
        } else if (isValidJoin(naturalRight)) {
            joinPos = naturalRight;
            cmd.query.joinType = JoinType::kRight;
            cmd.query.isNaturalJoin = true;
            bareJoin = naturalRight + 14;
        } else if (isValidJoin(naturalInner)) {
            joinPos = naturalInner;
            cmd.query.joinType = JoinType::kInner;
            cmd.query.isNaturalJoin = true;
            bareJoin = naturalInner + 14;
        } else if (isValidJoin(naturalJoin)) {
            joinPos = naturalJoin;
            cmd.query.joinType = JoinType::kInner;
            cmd.query.isNaturalJoin = true;
            bareJoin = naturalJoin + 8;
        } else if (isValidJoin(leftJoin)) {
            joinPos = leftJoin;
            cmd.query.joinType = JoinType::kLeft;
            bareJoin = leftJoin + 5; // " LEFT" -> point to " JOIN"
        } else if (isValidJoin(rightJoin)) {
            joinPos = rightJoin;
            cmd.query.joinType = JoinType::kRight;
            bareJoin = rightJoin + 6;
        } else if (isValidJoin(innerJoin)) {
            joinPos = innerJoin;
            cmd.query.joinType = JoinType::kInner;
            bareJoin = innerJoin + 6;
        } else if (isValidJoin(justJoin)) {
            joinPos = justJoin;
            cmd.query.joinType = JoinType::kInner;
            bareJoin = justJoin; // points to " JOIN"
        }

        // T1
        size_t t1End = sql.size();
        if (joinPos != std::string::npos) t1End = joinPos;
        if (wherePos != std::string::npos && wherePos < t1End) t1End = wherePos;
        if (groupPos != std::string::npos && groupPos < t1End) t1End = groupPos;
        if (havingPos != std::string::npos && havingPos < t1End) t1End = havingPos;
        if (orderPos != std::string::npos && orderPos < t1End) t1End = orderPos;
        std::string t1Clause = Trim(sql.substr(startRest, t1End - startRest));
        
        // T1 Alias
        {
             // Check for Subquery: (SELECT ... ) [AS Alias]
             bool isSub = false;
             if (t1Clause.size() > 2 && t1Clause.front() == '(') {
                 size_t closeP = FindMatchingClosingParen(t1Clause, 0);
                 if (closeP != std::string::npos) {
                     std::string inner = t1Clause.substr(0, closeP + 1);
                     auto sq = ParseSubQueryValues(inner);
                     if (sq) {
                        isSub = true;
                        cmd.query.sourceSubQuery = sq;
                        cmd.tableName = ""; // No physical table
                        
                        // Parse Alias after closeP
                        std::string remainder = Trim(t1Clause.substr(closeP + 1));
                        if (!remainder.empty()) {
                            std::string upRem = ToUpper(remainder);
                            if (upRem.find("AS ") == 0) {
                                cmd.query.sourceAlias = Trim(remainder.substr(3));
                            } else {
                                cmd.query.sourceAlias = remainder;
                            }
                            cmd.query.tableAlias = cmd.query.sourceAlias; 
                        } else {
                            // FROM subquery must have an alias (SQL standard requirement)
                            err = "Subquery in FROM clause must have an alias";
                            return cmd;
                        }
                     }
                 }
             }

             if (!isSub) {
                 std::string upT1 = ToUpper(t1Clause);
                 size_t asPos = upT1.find(" AS ");
                 if (asPos != std::string::npos) {
                     cmd.tableName = Trim(t1Clause.substr(0, asPos));
                     cmd.query.tableAlias = Trim(t1Clause.substr(asPos + 4));
                 } else {
                     size_t sp = t1Clause.rfind(' ');
                     if (sp != std::string::npos) {
                         cmd.tableName = Trim(t1Clause.substr(0, sp));
                         cmd.query.tableAlias = Trim(t1Clause.substr(sp + 1));
                     } else {
                         cmd.tableName = t1Clause;
                     }
                 }
                 cmd.query.sourceTable = cmd.tableName; // Ensure QueryPlan has source table info
                 cmd.query.sourceAlias = cmd.query.tableAlias;
             }
        }

        // T2 & Join Logic
        if (hasMultiJoin) {
            std::string leftClause = Trim(sql.substr(startRest, lastJoin.pos - startRest));
            std::string rightClauseRaw = sql.substr(lastJoin.pos, endFrom - lastJoin.pos);
            std::string subSql = "SELECT * FROM " + leftClause;
            Parser subParser;
            std::string subErr;
            ParsedCommand subCmd = subParser.Parse(subSql, subErr);
            if (!subErr.empty() || subCmd.type != CommandType::kSelect) {
                err = subErr.empty() ? "Invalid derived join source" : subErr;
                return cmd;
            }
            cmd.query.sourceSubQuery = std::make_shared<QueryPlan>(subCmd.query);
            cmd.tableName.clear();
            cmd.query.sourceTable.clear();
            cmd.query.tableAlias.clear();
            cmd.query.sourceAlias.clear();

            cmd.query.joinType = lastJoin.type;
            cmd.query.isNaturalJoin = lastJoin.natural;

            std::string upperRight = ToUpper(rightClauseRaw);
            if (cmd.query.isNaturalJoin) {
                std::string t2Clause = Trim(rightClauseRaw.substr(lastJoin.keywordLen));
                std::string upT2 = ToUpper(t2Clause);
                size_t asPos = upT2.find(" AS ");
                if (asPos != std::string::npos) {
                    cmd.query.joinTable = Trim(t2Clause.substr(0, asPos));
                    cmd.query.joinTableAlias = Trim(t2Clause.substr(asPos + 4));
                } else {
                    size_t sp = t2Clause.rfind(' ');
                    if (sp != std::string::npos) {
                        cmd.query.joinTable = Trim(t2Clause.substr(0, sp));
                        cmd.query.joinTableAlias = Trim(t2Clause.substr(sp + 1));
                    } else {
                        cmd.query.joinTable = t2Clause;
                    }
                }
            } else {
                size_t onPos = FindKeywordTopLevel(upperRight, " ON ", 0);
                if (onPos == std::string::npos) { err = "JOIN missing ON"; return cmd; }
                std::string t2Clause = Trim(rightClauseRaw.substr(lastJoin.keywordLen, onPos - lastJoin.keywordLen));

                std::string upT2 = ToUpper(t2Clause);
                size_t asPos = upT2.find(" AS ");
                if (asPos != std::string::npos) {
                    cmd.query.joinTable = Trim(t2Clause.substr(0, asPos));
                    cmd.query.joinTableAlias = Trim(t2Clause.substr(asPos + 4));
                } else {
                    size_t sp = t2Clause.rfind(' ');
                    if (sp != std::string::npos) {
                        cmd.query.joinTable = Trim(t2Clause.substr(0, sp));
                        cmd.query.joinTableAlias = Trim(t2Clause.substr(sp + 1));
                    } else {
                        cmd.query.joinTable = t2Clause;
                    }
                }

                std::string onCond = Trim(rightClauseRaw.substr(onPos + 4));
                auto eq = onCond.find('=');
                if (eq != std::string::npos) {
                    cmd.query.joinOnLeft = Trim(onCond.substr(0, eq));
                    cmd.query.joinOnRight = Trim(onCond.substr(eq + 1));
                } else {
                    err = "Invalid JOIN ON (e.g. T1.id = T2.id)"; return cmd;
                }
            }
        } else if (joinPos != std::string::npos) {
            size_t startT2 = bareJoin + 6; // After " JOIN "
            size_t onPos = FindKeywordTopLevel(upperSql, " ON ", startT2);
            if (cmd.query.isNaturalJoin) {
                size_t t2End = sql.size();
                if (wherePos != std::string::npos && wherePos < t2End) t2End = wherePos;
                if (groupPos != std::string::npos && groupPos < t2End) t2End = groupPos;
                if (havingPos != std::string::npos && havingPos < t2End) t2End = havingPos;
                if (orderPos != std::string::npos && orderPos < t2End) t2End = orderPos;
                std::string t2Clause = Trim(sql.substr(startT2, t2End - startT2));
                
                {
                    std::string upT2 = ToUpper(t2Clause);
                    size_t asPos = upT2.find(" AS ");
                    if (asPos != std::string::npos) {
                        cmd.query.joinTable = Trim(t2Clause.substr(0, asPos));
                        cmd.query.joinTableAlias = Trim(t2Clause.substr(asPos + 4));
                    } else {
                        size_t sp = t2Clause.rfind(' ');
                        if (sp != std::string::npos) {
                            cmd.query.joinTable = Trim(t2Clause.substr(0, sp));
                            cmd.query.joinTableAlias = Trim(t2Clause.substr(sp + 1));
                        } else {
                            cmd.query.joinTable = t2Clause;
                        }
                    }
                }
            } else {
                if (onPos == std::string::npos) { err = "JOIN missing ON"; return cmd; }
                if (wherePos != std::string::npos && onPos > wherePos) { err = "ON clause after WHERE?"; return cmd; }
                if (groupPos != std::string::npos && onPos > groupPos) { err = "ON clause after GROUP BY?"; return cmd; }
                if (havingPos != std::string::npos && onPos > havingPos) { err = "ON clause after HAVING?"; return cmd; }
                if (orderPos != std::string::npos && onPos > orderPos) { err = "ON clause after ORDER BY?"; return cmd; }

                std::string t2Clause = Trim(sql.substr(startT2, onPos - startT2));
            
             // T2 Alias
            {
                 std::string upT2 = ToUpper(t2Clause);
                 size_t asPos = upT2.find(" AS ");
                 if (asPos != std::string::npos) {
                     cmd.query.joinTable = Trim(t2Clause.substr(0, asPos));
                     cmd.query.joinTableAlias = Trim(t2Clause.substr(asPos + 4));
                 } else {
                     size_t sp = t2Clause.rfind(' ');
                     if (sp != std::string::npos) {
                         cmd.query.joinTable = Trim(t2Clause.substr(0, sp));
                         cmd.query.joinTableAlias = Trim(t2Clause.substr(sp + 1));
                     } else {
                         cmd.query.joinTable = t2Clause;
                     }
                 }
            }
            
            size_t onEnd = sql.size();
            if (wherePos != std::string::npos && wherePos < onEnd) onEnd = wherePos;
            if (groupPos != std::string::npos && groupPos < onEnd) onEnd = groupPos;
            if (havingPos != std::string::npos && havingPos < onEnd) onEnd = havingPos;
            if (orderPos != std::string::npos && orderPos < onEnd) onEnd = orderPos;
            std::string onCond = Trim(sql.substr(onPos + 4, onEnd - (onPos + 4)));
            auto eq = onCond.find('=');
            if (eq != std::string::npos) {
                cmd.query.joinOnLeft = Trim(onCond.substr(0, eq));
                cmd.query.joinOnRight = Trim(onCond.substr(eq + 1));
            } else {
                err = "Invalid JOIN ON (e.g. T1.id = T2.id)"; return cmd;
            }
            }
        }

        // 3. WHERE content
        if (wherePos != std::string::npos) {
          size_t whereEnd = sql.size();
          if (groupPos != std::string::npos && groupPos > wherePos) whereEnd = groupPos;
          if (havingPos != std::string::npos && havingPos > wherePos) whereEnd = havingPos;
          if (orderPos != std::string::npos && orderPos > wherePos) whereEnd = orderPos;
          std::string condPart = sql.substr(wherePos + 7, whereEnd - (wherePos + 7));
          cmd.query.conditions = ParseWhereClause(condPart);
        }

        // 4. GROUP BY content
        if (groupPos != std::string::npos) {
            size_t groupEnd = sql.size();
            if (havingPos != std::string::npos && havingPos > groupPos) groupEnd = havingPos;
            if (orderPos != std::string::npos && orderPos > groupPos) groupEnd = orderPos;
            std::string groupPart = Trim(sql.substr(groupPos + 10, groupEnd - (groupPos + 10)));
            auto groupFields = Split(groupPart, ',');
            for (auto& raw : groupFields) {
                std::string part = Trim(raw);
                if (!part.empty()) cmd.query.groupBy.push_back(part);
            }
        }

        // 4.5. HAVING content
        if (havingPos != std::string::npos) {
            size_t havingEnd = sql.size();
            if (orderPos != std::string::npos && orderPos > havingPos) havingEnd = orderPos;
            std::string havingPart = Trim(sql.substr(havingPos + 8, havingEnd - (havingPos + 8)));
            cmd.query.havingConditions = ParseWhereClause(havingPart);
        }

        // 5. ORDER BY content
        if (orderPos != std::string::npos) {
            std::string orderPart = Trim(sql.substr(orderPos + 10));
            auto orderFields = Split(orderPart, ',');
            for (auto& raw : orderFields) {
                std::string part = Trim(raw);
                if (part.empty()) continue;
                std::string up = ToUpper(part);
                bool asc = true;
                if (up.size() >= 5 && up.rfind(" DESC") == up.size() - 5) {
                    asc = false;
                    part = Trim(part.substr(0, part.size() - 5));
                } else if (up.size() >= 4 && up.rfind(" ASC") == up.size() - 4) {
                    asc = true;
                    part = Trim(part.substr(0, part.size() - 4));
                }
                if (!part.empty()) cmd.query.orderBy.push_back({part, asc});
            }
        }
        return cmd;
    }

  err = "Unsupported or unrecognized SQL";
  return cmd;
}

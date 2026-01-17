#pragma once
#include <string>
#include <vector>

// Field definition
struct Field {
  std::string name;      // field name
  std::string type;      // field type, e.g. "int" or "char[10]"
  int size = 0;          // byte length or n for char[n]
  bool isKey = false;    // primary key flag
  bool nullable = true;  // allow NULL
  bool valid = true;     // soft-delete flag for column
};

struct IndexDef {
    std::string name;
    std::string fieldName;
    bool isUnique = false;
};

// Table schema
struct TableSchema {
  std::string tableName;
  std::vector<Field> fields;
  std::vector<IndexDef> indexes; // Fields that have an index
};

// Single record
struct Record {
  bool valid = true;                 // record valid flag (soft delete)
  std::vector<std::string> values;   // values aligned with fields
};

// Condition (simple equality/contains)
struct Condition {
  std::string fieldName;
  std::string op;       // supported: "=", "!=", "CONTAINS", "IN"
  std::string value;
  std::vector<std::string> values; // for IN operator
};

struct AggregateExpr {
  std::string func;   // COUNT, SUM, AVG, MIN, MAX
  std::string field;  // field name or "*"
  std::string alias;  // output alias
};

struct SelectExpr {
  bool isAggregate = false;
  std::string field;    // column name or expression
  AggregateExpr agg;    // aggregate detail when isAggregate = true
  std::string alias;    // output alias
};

enum class JoinType { kInner, kLeft, kRight };

// Query plan (basic)
struct QueryPlan {
  std::vector<std::string> projection;  // projected fields; empty means all
  std::vector<std::string> projectionAliases; // Aliases for projected fields
  std::vector<Condition> conditions;    // WHERE conditions
  std::vector<std::pair<std::string, bool>> orderBy; // (field, asc)
  std::vector<std::string> groupBy;     // GROUP BY columns
  std::vector<AggregateExpr> aggregates; // Aggregates in SELECT
  std::vector<SelectExpr> selectExprs;  // SELECT list in order

  // Join support
  std::string joinTable;       // Table to join with
  std::string joinOnLeft;      // field in primary table
  std::string joinOnRight;     // field in join table
  JoinType joinType = JoinType::kInner;
  
  std::string tableAlias;      // Alias for main table
  std::string joinTableAlias;  // Alias for joined table
};

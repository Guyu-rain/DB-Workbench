#pragma once
#include <string>
#include <vector>
#include "db_types.h"

enum class CommandType {
  kUnknown,
  kCreate,
  kCreateDatabase,
  kUseDatabase,
  kDropDatabase,
  kInsert,
  kSelect,
  kDelete,
  kUpdate,
  kRename,
  kDrop,
  kCreateIndex,
  kDropIndex,
  kShowIndexes,
  kShowTables,
  kAlter,
  kCreateView,
  kDropView,
  kBegin,
  kCommit,
  kRollback,
  kSavepoint,
  kRollbackTo,
  kRelease,
  kCreateUser,
  kDropUser,
  kGrant,
  kRevoke,
  kCheckpoint,
  kBackup
};

enum class AlterOperation {
    kNone,
    kAddColumn,
    kDropColumn,
    kModifyColumn,
    kRenameColumn,
    kRenameTable,
    kAddIndex,
    kDropIndex,
    kAddConstraint,
    kDropConstraint
};

struct ParsedCommand {
  CommandType type = CommandType::kUnknown;
  std::string tableName;
  std::string dbName;
  TableSchema schema;                 // for CREATE
  std::vector<Record> records;        // for INSERT
  QueryPlan query;                    // for SELECT
  std::vector<Condition> conditions;  // WHERE clause (UPDATE/DELETE)
  std::vector<std::pair<std::string, std::string>> assignments;  // UPDATE set list
  std::string newName;                // for RENAME
  std::string backupPath;             // for BACKUP
  
  std::string username;
  std::string password;
  std::vector<std::string> privileges;
  
  std::string indexName;              // for INDEX ops
  std::string fieldName;              // for INDEX ops, DROP COLUMN
  bool isUnique = false;              // for CREATE INDEX
  std::string savepointName;          // for SAVEPOINT
  ReferentialAction action = ReferentialAction::kRestrict;
  bool actionSpecified = false;
  ForeignKeyDef fkDef;

  // Alter Table specific
  AlterOperation alterOp = AlterOperation::kNone;
  Field columnDef;                    // For ADD/MODIFY COLUMN
  std::string extraInfo;              // For AFTER column name, etc.

  // View definition
  std::string viewName;
  QueryPlan viewQuery;
  std::vector<std::string> viewColumns;
  std::string viewSql;
  bool viewOrReplace = false;
  bool ifExists = false;
};

class Parser {
 public:
  ParsedCommand Parse(const std::string& sql, std::string& err);
};

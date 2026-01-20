#pragma once
#include <string>
#include <vector>
#include "db_types.h"
#include "storage_engine.h"

class DDLService {
 public:
  explicit DDLService(StorageEngine& engine) : engine_(engine) {}

  bool CreateTable(const std::string& dbfPath, const std::string& datPath, const TableSchema& schema, std::string& err);
  bool RenameTable(const std::string& dbfPath, const std::string& datPath, const std::string& oldName, const std::string& newName, std::string& err);
  bool DropTable(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, ReferentialAction action, std::string& err);
  
  // Alter Table Logic
  bool AddColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const Field& newField, const std::string& afterCol, std::string& err); // if afterCol == "FIRST", put first
  bool DropColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& colName, std::string& err);
  bool ModifyColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const Field& newField, std::string& err);
  bool RenameColumn(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& oldName, const std::string& newName, std::string& err);

  // Index Management
  bool CreateIndex(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& fieldName, const std::string& indexName, bool isUnique, std::string& err);
  bool DropIndex(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& fieldName, std::string& err);
  bool ListIndexes(const std::string& dbfPath, const std::string& tableName, std::vector<IndexDef>& outIndexes, std::string& err);
  bool RebuildIndexes(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, std::string& err);
  bool AddForeignKey(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, ForeignKeyDef fk, std::string& err);
  bool DropForeignKey(const std::string& dbfPath, const std::string& datPath, const std::string& tableName, const std::string& fkName, std::string& err);

  // View management
  bool CreateView(const std::string& dbfPath, const std::string& datPath, const std::string& viewName, const std::string& viewSql, const QueryPlan& plan,
                  const std::vector<std::string>& columnNames, bool orReplace, std::string& err);
  bool DropView(const std::string& dbfPath, const std::string& datPath, const std::string& viewName, bool ifExists, std::string& err);

 private:
  StorageEngine& engine_;
  std::string GetIndexPath(const std::string& datPath, const std::string& tableName, const std::string& fieldName);
};

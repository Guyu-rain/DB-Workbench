#pragma once
#include <string>
#include <vector>
#include <map>
#include "db_types.h"

// Binary IO for .dbf (schema) and .dat (data)
class StorageEngine {
 public:
  
  // Create empty database files: xxx.dbf / xxx.dat
  bool CreateDatabase(const std::string& dbName, std::string& err);

  // Drop database files
  bool DropDatabase(const std::string& dbName, std::string& err);

  // Read all table schemas from dbf
  bool LoadSchemas(const std::string& dbfPath, std::vector<TableSchema>& outSchemas, std::string& err);

  // Helper to load single schema
  bool LoadSchema(const std::string& dbfPath, const std::string& tableName, TableSchema& outSchema, std::string& err);

  // Overwrite dbf with all schemas
  bool SaveSchemas(const std::string& dbfPath, const std::vector<TableSchema>& schemas, std::string& err);

  // Append one schema at file end
  bool AppendSchema(const std::string& dbfPath, const TableSchema& schema, std::string& err);

  // Append one record for a table, returns offset in file
  bool AppendRecord(const std::string& datPath, const TableSchema& schema, const Record& record, long& outOffset, std::string& err);
  
  // Append multiple records for a table
  bool AppendRecords(const std::string& datPath, const TableSchema& schema, const std::vector<Record>& records, std::string& err);

  // Read all records of a table
  bool ReadRecords(const std::string& datPath, const TableSchema& schema, std::vector<Record>& outRecords, std::string& err);

  // Read all records with their offsets (for Index Building)
  bool ReadRecordsWithOffsets(const std::string& datPath, const TableSchema& schema, std::vector<std::pair<long, Record>>& outRecords, std::string& err);

  // Read single record at specific offset (Random Access)
  bool ReadRecordAt(const std::string& datPath, const TableSchema& schema, long offset, Record& outRecord, std::string& err);

  // Read raw record bytes at offset (valid flag + fields)
  bool ReadRecordBytesAt(const std::string& datPath, const TableSchema& schema, long offset, std::vector<uint8_t>& outBytes, std::string& err);

  // Write raw record bytes at offset
  bool WriteRecordBytesAt(const std::string& datPath, long offset, const std::vector<uint8_t>& bytes, std::string& err);

  // Compute offset for next append record (single-record block)
  bool ComputeAppendRecordOffset(const std::string& datPath, const TableSchema& schema, long& outOffset, std::string& err);

  // Write insert block header + record at offset
  bool WriteInsertBlockAt(const std::string& datPath, const TableSchema& schema, long recordOffset, const std::vector<uint8_t>& recordBytes, std::string& err);

  // Serialize record to bytes (valid flag + fields)
  bool SerializeRecord(const TableSchema& schema, const Record& record, std::vector<uint8_t>& outBytes, std::string& err) const;

  // Overwrite all records of a table
  bool SaveRecords(const std::string& datPath, const TableSchema& schema, const std::vector<Record>& records, std::string& err);

  // Index IO
  bool LoadIndex(const std::string& indexPath, std::map<std::string, long>& outIndex, std::string& err);
  bool SaveIndex(const std::string& indexPath, const std::map<std::string, long>& index, std::string& err);

 private:
  // basic read/write helpers
  bool WriteString(std::ofstream& ofs, const std::string& s);
  bool ReadString(std::ifstream& ifs, std::string& s);
};

#include "storage_engine.h"
#include <fstream>
#include <iostream>
#include <cstdio>
// ******* ������������ͷ�ļ� *******
#include <map>          // �ṩ std::map
#include <vector>       // �ṩ std::vector����Ȼ storage_engine.h �Ѱ�������������ʽ��������ȫ��
#include <string>
#include <filesystem>
namespace fs = std::filesystem;       // �ṩ std::string��ͬ�ϣ�

bool StorageEngine::BackupDatabase(const std::string& dbName, const std::string& destPath, std::string& err) {
    namespace fs = std::filesystem;
    try {
        if (!fs::exists(destPath)) {
            if (!fs::create_directories(destPath)) {
                err = "Failed to create directory: " + destPath;
                return false;
            }
        }
    } catch (const fs::filesystem_error& e) {
        err = "Filesystem error: " + std::string(e.what());
        return false;
    }

    // Identify files to copy: dbName.*
    // Also indexes for tables in this DB: dbName.Table.*
    // WAL: dbName.wal
    // Using directory iteration
    
    try {
        fs::path currentDir = fs::current_path(); // Assumes data files are in CWD
        // If data files are elsewhere, we need that path. 
        // Based on CreateDatabase/CreateDatabase usage, it uses simple filenames, so CWD.
        
        for (const auto& entry : fs::directory_iterator(currentDir)) {
            if (entry.is_regular_file()) {
                std::string fname = entry.path().filename().string();
                // Check prefix
                if (fname.find(dbName + ".") == 0) {
                     fs::copy_file(entry.path(), fs::path(destPath) / fname, fs::copy_options::overwrite_existing);
                }
            }
        }
    } catch (const std::exception& e) {
        err = "Backup failed: " + std::string(e.what());
        return false;
    }
    
    return true;
}

namespace {
    constexpr char kTableSep = '~';

    bool WriteUInt32(std::ofstream& ofs, uint32_t v) {
        ofs.write(reinterpret_cast<const char*>(&v), sizeof(uint32_t));
        return static_cast<bool>(ofs);
    }

    bool ReadUInt32(std::ifstream& ifs, uint32_t& v) {
        ifs.read(reinterpret_cast<char*>(&v), sizeof(uint32_t));
        return static_cast<bool>(ifs);
    }
}

bool StorageEngine::CreateDatabase(const std::string& dbName, std::string& err) {
    std::string dbf = dbName + ".dbf";
    std::string dat = dbName + ".dat";

    // ����Ƿ��Ѵ���?
    {
        std::ifstream ifs(dbf, std::ios::binary);
        if (ifs.good()) {
            err = "Database already exists";
            return false;
        }
    }

    // ���� dbf
    {
        std::ofstream ofs(dbf, std::ios::binary);
        if (!ofs) {
            err = "Failed to create dbf file";
            return false;
        }
        // Ŀǰ��д�κ����ݣ�����������������
    }

    // ���� dat
    {
        std::ofstream ofs(dat, std::ios::binary);
        if (!ofs) {
            err = "Failed to create dat file";
            return false;
        }
    }

    return true;
}

bool StorageEngine::DropDatabase(const std::string& dbName, std::string& err) {
    std::string dbf = dbName + ".dbf";
    std::string dat = dbName + ".dat";
    
    bool ok = true;
    if (std::remove(dbf.c_str()) != 0) {
        // err = "Failed to delete .dbf file";
        // maybe it didn't exist, ignore or warn
    }
    if (std::remove(dat.c_str()) != 0) {
        // err = "Failed to delete .dat file";
    }
    return true;
}

bool StorageEngine::WriteString(std::ofstream& ofs, const std::string& s) {
    if (!WriteUInt32(ofs, static_cast<uint32_t>(s.size()))) return false;
    ofs.write(s.data(), static_cast<std::streamsize>(s.size()));
    return static_cast<bool>(ofs);
}

bool StorageEngine::ReadString(std::ifstream& ifs, std::string& s) {
    uint32_t len = 0;
    if (!ReadUInt32(ifs, len)) return false;
    s.resize(len);
    // std::string::data is const in older standards; write into the buffer via &s[0].
    if (len > 0) ifs.read(&s[0], static_cast<std::streamsize>(len));
    return static_cast<bool>(ifs);
}

bool StorageEngine::LoadSchema(const std::string& dbfPath, const std::string& tableName, TableSchema& outSchema, std::string& err) {
    std::vector<TableSchema> schemas;
    if (!LoadSchemas(dbfPath, schemas, err)) return false;
    for (const auto& s : schemas) {
        if (s.tableName == tableName) {
            outSchema = s;
            return true;
        }
    }
    err = "Table not found: " + tableName;
    return false;
}

bool StorageEngine::LoadSchemas(const std::string& dbfPath,
    std::vector<TableSchema>& schemas,
    std::string& err) {
    schemas.clear();

    std::ifstream ifs(dbfPath, std::ios::binary);
    if (!ifs.is_open()) {
        // dbf ������ = û���κα�
        return true;
    }

    while (ifs.peek() != EOF) {
        char sep;
        ifs.read(&sep, 1);
        if (!ifs) break;

        if (sep != kTableSep) {
            err = "Invalid table separator in dbf";
            return false;
        }

        TableSchema schema;

        // table name
        if (!ReadString(ifs, schema.tableName)) {
            err = "Failed to read table name";
            return false;
        }

        uint32_t fieldCount = 0;
        if (!ReadUInt32(ifs, fieldCount)) {
            err = "Failed to read field count";
            return false;
        }

        for (uint32_t i = 0; i < fieldCount; ++i) {
            Field f;
            if (!ReadString(ifs, f.name)) return false;
            if (!ReadString(ifs, f.type)) return false;
            uint32_t sz = 0;
            if (!ReadUInt32(ifs, sz)) return false;
            f.size = static_cast<int>(sz);

            char flags[3];
            ifs.read(flags, 3);
            if (!ifs) return false;

            f.isKey = flags[0];
            f.nullable = flags[1];
            f.valid = flags[2];

            schema.fields.push_back(f);
        }

        // Read Indexes
        uint32_t idxCount = 0;
        // Try read. If fails (EOF or old format), we might assume 0, but 'ReadUInt32' moves cursor.
        // For simplicity in this task, we assume file format matches.
        if (ReadUInt32(ifs, idxCount)) {
            for (uint32_t k = 0; k < idxCount; ++k) {
                IndexDef idx;
                if (!ReadString(ifs, idx.name)) return false;
                // Support backward compatibility or assume new format?
                // If we want to be safe, we might fail here on old DB files.
                // Assuming new format.
                if (!ReadString(ifs, idx.fieldName)) return false;
                char u = 0;
                ifs.read(&u, 1);
                idx.isUnique = (u != 0);
                schema.indexes.push_back(idx);
            }
        } else {
            // checking if EOF caused this
            if (ifs.eof()) {
                // acceptable for last table in old format? 
                // But ReadUInt32 would set failbit.
                ifs.clear(); 
            }
        }

        schemas.push_back(schema);
    }

    return true;
}



bool StorageEngine::SaveSchemas(const std::string& dbfPath, const std::vector<TableSchema>& schemas, std::string& err) {
    std::ofstream ofs(dbfPath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        err = "Cannot open dbf file for writing: " + dbfPath;
        return false;
    }
    for (const auto& schema : schemas) {
        ofs.write(&kTableSep, 1);
        if (!WriteString(ofs, schema.tableName)) return false;
        if (!WriteUInt32(ofs, static_cast<uint32_t>(schema.fields.size()))) return false;
        for (const auto& f : schema.fields) {
            if (!WriteString(ofs, f.name)) return false;
            if (!WriteString(ofs, f.type)) return false;
            if (!WriteUInt32(ofs, static_cast<uint32_t>(f.size))) return false;
            char flags[3] = { static_cast<char>(f.isKey), static_cast<char>(f.nullable), static_cast<char>(f.valid) };
            ofs.write(flags, 3);
        }
        
        // Save Indexes
        if (!WriteUInt32(ofs, static_cast<uint32_t>(schema.indexes.size()))) return false;
        for (const auto& idx : schema.indexes) {
            if (!WriteString(ofs, idx.name)) return false;
            if (!WriteString(ofs, idx.fieldName)) return false;
            char u = idx.isUnique ? 1 : 0;
            ofs.write(&u, 1);
        }
    }
    return static_cast<bool>(ofs);
}

bool StorageEngine::AppendSchema(const std::string& dbfPath, const TableSchema& schema, std::string& err) {
    // Load existing schemas then overwrite to avoid duplicates
    std::vector<TableSchema> schemas;
    if (!LoadSchemas(dbfPath, schemas, err)) {
        schemas.clear();  // treat as empty when file missing
    }
    schemas.push_back(schema);
    return SaveSchemas(dbfPath, schemas, err);
}

// Helper to read fields
static bool ReadFields(std::ifstream& ifs, const TableSchema& schema, Record& rec) {
    // Valid flag
    char valid;
    ifs.read(&valid, 1);
    if (!ifs) return false;
    rec.valid = (valid != 0);

    for (const auto& f : schema.fields) {
        std::string s;
        // Re-use StorageEngine::ReadString logic (but we are outside class or need helper)
        uint32_t len = 0;
        ifs.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        if (!ifs) return false;
        s.resize(len);
        if (len > 0) ifs.read(&s[0], len);
        if (!ifs) return false;
        rec.values.push_back(s);
    }
    return true;
}

bool StorageEngine::AppendRecord(const std::string& datPath, const TableSchema& schema, const Record& record, long& outOffset, std::string& err) {
    if (record.values.size() != schema.fields.size()) {
        err = "Record field count mismatch";
        return false;
    }

    std::ofstream ofs(datPath, std::ios::binary | std::ios::app);
    if (!ofs.is_open()) {
        err = "Cannot open dat file for append: " + datPath;
        return false;
    }

    // Write Block Header
    ofs.write(&kTableSep, 1);
    if (!WriteString(ofs, schema.tableName)) return false;
    if (!WriteUInt32(ofs, 1)) return false; // 1 Record
    if (!WriteUInt32(ofs, static_cast<uint32_t>(schema.fields.size()))) return false;

    // Save Offset
    outOffset = static_cast<long>(ofs.tellp());

    // Write Record
    char valid = record.valid ? 1 : 0;
    ofs.write(&valid, 1);
    for (const auto& val : record.values) {
        if (!WriteString(ofs, val)) return false;
    }

    return true;
}

// Rewriting AppendRecords to use Append-Only mode (writing one block with multiple records)
// NOTE: This does NOT return offsets. Use AppendRecord loop if you need offsets.
bool StorageEngine::AppendRecords(const std::string& datPath, const TableSchema& schema, const std::vector<Record>& newRecords, std::string& err) {
    if (newRecords.empty()) return true;

    std::ofstream ofs(datPath, std::ios::binary | std::ios::app);
    if (!ofs.is_open()) {
        err = "Cannot open dat file for append: " + datPath;
        return false;
    }

    // Write Block Header
    ofs.write(&kTableSep, 1);
    if (!WriteString(ofs, schema.tableName)) return false;
    if (!WriteUInt32(ofs, static_cast<uint32_t>(newRecords.size()))) return false;
    if (!WriteUInt32(ofs, static_cast<uint32_t>(schema.fields.size()))) return false;

    for (const auto& r : newRecords) {
        if (r.values.size() != schema.fields.size()) {
            err = "Record field count mismatch";
            return false;
        }
        char valid = r.valid ? 1 : 0;
        ofs.write(&valid, 1);
        for (const auto& val : r.values) {
            if (!WriteString(ofs, val)) return false;
        }
    }
    return true;
}

bool StorageEngine::ReadRecordAt(const std::string& datPath, const TableSchema& schema, long offset, Record& outRecord, std::string& err) {
    std::ifstream ifs(datPath, std::ios::binary);
    if (!ifs.is_open()) {
        err = "Cannot open dat file: " + datPath;
        return false;
    }
    
    ifs.seekg(offset);
    if (!ifs) {
        err = "Seek failed";
        return false;
    }

    outRecord.values.clear();
    if (!ReadFields(ifs, schema, outRecord)) {
        err = "Read fields failed";
        return false;
    }
    return true;
}

// Index IO
bool StorageEngine::LoadIndex(const std::string& indexPath, std::map<std::string, long>& outIndex, std::string& err) {
    outIndex.clear();
    std::ifstream ifs(indexPath, std::ios::binary);
    if (!ifs.is_open()) return true; // No index (new), not error

    while (ifs.peek() != EOF) {
        std::string key;
        // Read key
        if (!ReadString(ifs, key)) break;
        // Read offset
        uint32_t off = 0; 
        if (!ReadUInt32(ifs, off)) break;
        outIndex[key] = static_cast<long>(off);
    }
    return true;
}

bool StorageEngine::SaveIndex(const std::string& indexPath, const std::map<std::string, long>& index, std::string& err) {
    std::ofstream ofs(indexPath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        err = "Cannot write index file";
        return false;
    }
    for (const auto& kv : index) {
        if (!WriteString(ofs, kv.first)) return false;
        if (!WriteUInt32(ofs, static_cast<uint32_t>(kv.second))) return false;
    }
    return true;
}


bool StorageEngine::ReadRecordsWithOffsets(const std::string& datPath, const TableSchema& schema, std::vector<std::pair<long, Record>>& outRecords, std::string& err) {
    std::ifstream ifs(datPath, std::ios::binary);
    if (!ifs.is_open()) {
        err = "Cannot open dat file: " + datPath;
        return false;
    }

    outRecords.clear();
    while (ifs.peek() != EOF) {
        char sep;
        ifs.read(&sep, 1);
        if (!ifs) break;
        if (sep != kTableSep) {
            err = "Invalid separator in dat";
            return false;
        }
        std::string tableName;
        if (!ReadString(ifs, tableName)) return false;
        uint32_t recordCount = 0;
        uint32_t fieldCount = 0;
        if (!ReadUInt32(ifs, recordCount)) return false;
        if (!ReadUInt32(ifs, fieldCount)) return false;

        // Skip logic for unrelated tables
        if (tableName != schema.tableName) {
            for (uint32_t i = 0; i < recordCount; ++i) {
                 // Skip valid byte
                 ifs.ignore(1);
                 for (uint32_t j = 0; j < fieldCount; ++j) {
                     // Read and discard string
                     uint32_t len = 0;
                     ifs.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
                     if (len > 0) ifs.ignore(len); 
                 }
            }
            continue;
        }

        // Read records
        for (uint32_t i = 0; i < recordCount; ++i) {
            long offset = static_cast<long>(ifs.tellg());
            Record rec;
            if (!ReadFields(ifs, schema, rec)) {
                err = "Failed reading record in Loop";
                return false;
            }
            if (rec.valid) {
                outRecords.push_back({offset, rec});
            }
        }
    }
    return true;
}

bool StorageEngine::ReadRecords(const std::string& datPath, const TableSchema& schema, std::vector<Record>& outRecords, std::string& err) {
    std::ifstream ifs(datPath, std::ios::binary);
    if (!ifs.is_open()) {
        err = "Cannot open dat file: " + datPath;
        return false;
    }

    outRecords.clear();
    while (ifs.peek() != EOF) {
        char sep;
        ifs.read(&sep, 1);
        if (!ifs) break;
        if (sep != kTableSep) {
            err = "Invalid separator in dat";
            return false;
        }
        std::string tableName;
        if (!ReadString(ifs, tableName)) return false;
        uint32_t recordCount = 0;
        uint32_t fieldCount = 0;
        if (!ReadUInt32(ifs, recordCount)) return false;
        if (!ReadUInt32(ifs, fieldCount)) return false;

        if (tableName != schema.tableName) {
            // skip unrelated table
            for (uint32_t i = 0; i < recordCount; ++i) {
                char validFlag;
                ifs.read(&validFlag, 1);
                for (uint32_t j = 0; j < fieldCount; ++j) {
                    std::string dummy;
                    if (!ReadString(ifs, dummy)) return false;
                }
            }
            continue;
        }

        for (uint32_t i = 0; i < recordCount; ++i) {
            Record rec;
            char validFlag;
            ifs.read(&validFlag, 1);
            rec.valid = validFlag;
            for (uint32_t j = 0; j < fieldCount; ++j) {
                std::string val;
                if (!ReadString(ifs, val)) return false;
                rec.values.push_back(val);
            }
            outRecords.push_back(rec);
        }
    }
    return true;
}

// ******* ���ǹؼ����޸ĺ��� *******
bool StorageEngine::SaveRecords(const std::string& datPath, const TableSchema& schema,
    const std::vector<Record>& records, std::string& err) {
    // 1. �Ƶ���Ӧ�� .dbf ·��
    std::string dbfPath = datPath.substr(0, datPath.find_last_of('.')) + ".dbf";

    // 2. ��ȡ���б��ṹ
    std::vector<TableSchema> allSchemas;
    if (!LoadSchemas(dbfPath, allSchemas, err)) {
        allSchemas.clear(); // �ļ�����������Ϊ��
    }

    // 3. ��ȡ���б������ݣ���ǰ���ݿ������б���
    std::map<std::string, std::vector<Record>> allData;  // ******* allData ���� *******
    for (const auto& s : allSchemas) {
        std::vector<Record> recs;
        // ���Ե�������ȡ���󣬼�������������
        std::string ignoreErr;
        if (ReadRecords(datPath, s, recs, ignoreErr)) {
            allData[s.tableName] = recs;
        }
        else {
            allData[s.tableName] = {}; // ����ʧ������Ϊ�ձ�
        }
    }

    // 4. ���µ�ǰ��������
    allData[schema.tableName] = records;

    // 5. д�����б����ݣ�����д�����������б���
    std::ofstream ofs(datPath, std::ios::binary | std::ios::trunc);
    if (!ofs.is_open()) {
        err = "Cannot open dat file for writing: " + datPath;
        return false;
    }

    for (const auto& tableSchema : allSchemas) {
        const std::string& tableName = tableSchema.tableName;
        const auto& tableRecords = allData[tableName];

        ofs.write(&kTableSep, 1);
        if (!WriteString(ofs, tableName)) return false;
        if (!WriteUInt32(ofs, static_cast<uint32_t>(tableRecords.size()))) return false;
        if (!WriteUInt32(ofs, static_cast<uint32_t>(tableSchema.fields.size()))) return false;

        for (const auto& rec : tableRecords) {
            char validFlag = rec.valid ? 1 : 0;
            ofs.write(&validFlag, 1);
            for (size_t i = 0; i < tableSchema.fields.size(); ++i) {
                const std::string& val = (i < rec.values.size()) ? rec.values[i] : "";
                if (!WriteString(ofs, val)) return false;
            }
        }
    }

    return static_cast<bool>(ofs);
}
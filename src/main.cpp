#include <iostream>
#include <vector>
#include "api_server.h"
#include "db_types.h"
#include "ddl.h"
#include "dml.h"
#include "query.h"
#include "storage_engine.h"
#include "txn/log_manager.h"
#include "txn/txn_manager.h"
#include "txn/recovery.h"
#include "txn/lock_manager.h"
#include <windows.h>



namespace {

    bool EnsureBootstrap(StorageEngine& engine, DDLService& ddl, DMLService& dml,
        const std::string& dbf, const std::string& dat, TableSchema& outSchema) {
        std::string err;
        std::vector<TableSchema> schemas;
        engine.LoadSchemas(dbf, schemas, err);

        auto it = std::find_if(schemas.begin(), schemas.end(), [](const TableSchema& s) {
            return s.tableName == "Users";
            });

        if (it == schemas.end()) {
            TableSchema schema;
            schema.tableName = "Users";
            schema.fields = {
                {"Id", "int", 4, true, false, true},
                {"Name", "char[32]", 32, false, false, true},
                {"Age", "int", 4, false, true, true},
                {"Role", "char[16]", 16, false, true, true},
                {"Status", "char[16]", 16, false, true, true},
            };
            if (!ddl.CreateTable(dbf, dat, schema, err)) {
                std::cerr << "CreateTable failed: " << err << "\n";
                return false;
            }
            outSchema = schema;
        }
        else {
            outSchema = *it;
        }

        std::vector<Record> rows;
        engine.ReadRecords(dat, outSchema, rows, err);
        return true;
    }

} // namespace

int main() {
    StorageEngine engine;
    DDLService ddl(engine);
    DMLService dml(engine);
    QueryService query(engine);

    
    const std::string dbf = "MyDB.dbf";
    const std::string dat = "MyDB.dat";

    TableSchema schema;
    if (!EnsureBootstrap(engine, ddl, dml, dbf, dat, schema)) {
        return 1;
    }

    
    TxnId maxTxn = 0;
    LSN maxLsn = 0;
    WIN32_FIND_DATAA findData;
    HANDLE hFind = FindFirstFileA("*.dbf", &findData);
    if (hFind != INVALID_HANDLE_VALUE) {
        do {
            std::string filename = findData.cFileName;
            if (filename.size() <= 4) continue;
            std::string dbName = filename.substr(0, filename.size() - 4);
            std::string recErr;
            LSN dbMaxLsn = 0;
            TxnId t = Recovery::Run(engine, dbName, recErr, &dbMaxLsn);
            if (t > maxTxn) maxTxn = t;
            if (dbMaxLsn > maxLsn) maxLsn = dbMaxLsn;
        } while (FindNextFileA(hFind, &findData));
        FindClose(hFind);
    }

    LogManager log("MyDB");
    TxnManager txn_manager(engine, log);
    LockManager lock_manager;
    txn_manager.SetNextTxnId(maxTxn + 1);
    log.SetNextLsn(maxLsn + 1);

    ApiServer server(engine, ddl, dml, query, log, txn_manager, lock_manager, dbf, dat);
    server.Run(8080);

    return 0;
}

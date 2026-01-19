#include <iostream>
#include <vector>
#include <algorithm>
#include <string>
#include <filesystem>
#include <cstdlib>

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

namespace fs = std::filesystem;

namespace {

bool EnsureBootstrap(StorageEngine& engine, DDLService& ddl, DMLService& dml,
                     const std::string& dbf, const std::string& dat,
                     TableSchema& outSchema) {
    std::string err;
    std::vector<TableSchema> schemas;
    engine.LoadSchemas(dbf, schemas, err);

    auto it = std::find_if(schemas.begin(), schemas.end(),
                           [](const TableSchema& s) { return s.tableName == "Users"; });

    if (it == schemas.end()) {
        TableSchema schema;
        schema.tableName = "Users";
        schema.fields = {
            {"Id", "int", 4, true,  false, true},
            {"Name", "char[32]", 32, false, false, true},
            {"Age", "int", 4, false, true,  true},
            {"Role", "char[16]", 16, false, true,  true},
            {"Status","char[16]", 16, false, true,  true},
        };

        if (!ddl.CreateTable(dbf, dat, schema, err)) {
            std::cerr << "CreateTable failed: " << err << "\n";
            return false;
        }
        outSchema = schema;
    } else {
        outSchema = *it;
    }

    // 读取一次，触发基础数据文件检查/初始化（按你 engine 的实现而定）
    std::vector<Record> rows;
    engine.ReadRecords(dat, outSchema, rows, err);
    (void)rows;

    return true;
}

// 提取 "xxx.dbf" -> "xxx"
static std::string DbNameFromDbfPath(const fs::path& p) {
    // stem() 会去掉最后一个扩展名：MyDB.dbf -> MyDB
    return p.stem().string();
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

    // --- Cross-platform recovery scan: iterate current directory for *.dbf ---
    TxnId maxTxn = 0;
    LSN   maxLsn = 0;

    try {
        const fs::path cwd = fs::current_path();

        for (const auto& entry : fs::directory_iterator(cwd)) {
            if (!entry.is_regular_file()) continue;

            const fs::path p = entry.path();
            if (p.extension() != ".dbf") continue;

            const std::string dbName = DbNameFromDbfPath(p); // "xxx"
            std::string recErr;
            LSN dbMaxLsn = 0;

            TxnId t = Recovery::Run(engine, dbName, recErr, &dbMaxLsn);

            if (!recErr.empty()) {
                std::cerr << "[Recovery] db=" << dbName << " warning: " << recErr << "\n";
            }

            if (t > maxTxn) maxTxn = t;
            if (dbMaxLsn > maxLsn) maxLsn = dbMaxLsn;
        }
    } catch (const std::exception& e) {
        std::cerr << "[Recovery] scan failed: " << e.what() << "\n";
        // 扫描失败不一定要退出，取决于你想要的策略；这里选择继续启动
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

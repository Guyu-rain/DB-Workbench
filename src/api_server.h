#pragma once
#include <string>
#include <vector>
#include "ddl.h"
#include "dml.h"
#include "http_server.h"
#include "json_utils.h"
#include "query.h"
#include "auth.h"
#include "storage_engine.h"
#include "txn/txn_manager.h"
#include "txn/log_manager.h"
#include "txn/lock_manager.h"
#include <map>

class ApiServer {
public:
    struct SessionContext {
        Txn* current_txn = nullptr;
        bool autocommit = true;
        IsolationLevel isolation = IsolationLevel::READ_COMMITTED;
    };

    ApiServer(StorageEngine& engine, DDLService& ddl, DMLService& dml, QueryService& query,
        LogManager& log, TxnManager& txn_manager, LockManager& lock_manager,
        const std::string& dbfPath, const std::string& datPath);
    void Run(uint16_t port = 8080);

private:
    StorageEngine& engine_;
    DDLService& ddl_;
    DMLService& dml_;
    QueryService& query_;
    LogManager& log_;
    TxnManager& txn_manager_;
    LockManager& lock_manager_;
    AuthManager auth_; // Added AuthManager

    std::string dbfPath_;
    std::string datPath_;

    std::string currentDbf_;
    std::string currentDat_;
    std::string currentDbName_;
    std::map<std::string, SessionContext> sessions_;

    std::string DataPath(const std::string& table) const;
    bool LoadSchema(const std::string& table, TableSchema& out, std::string& err);
    std::vector<TableSchema> ListSchemas();
    std::vector<Record> LoadAll(const TableSchema& schema, const std::string& dataPath, std::string& err);
    std::string BuildSql(const std::string& schemaName, const std::string& table, const std::string& filter, int limit) const;
    std::string SerializeRows(const TableSchema& schema, const std::vector<Record>& rows) const;
    std::string Error(const std::string& msg, int code = 400) const;
    std::string Success(const std::string& body) const;
    bool EnsureDefaultDb(std::string& err);
    /*std::string Error(const std::string& msg, int code = 400) const;
    std::string Success(const std::string& body = "") const;*/


    // handlers
    void HandleQuery(const HttpRequest& req, HttpResponse& resp);
    void HandleInsert(const HttpRequest& req, HttpResponse& resp);
    void HandleUpdate(const HttpRequest& req, HttpResponse& resp);
    void HandleDelete(const HttpRequest& req, HttpResponse& resp);
    void HandleUseDatabase(const HttpRequest& req, HttpResponse& resp);
    void HandleCreateTable(const HttpRequest& req, HttpResponse& resp);
    void HandleListTables(const HttpRequest& req, HttpResponse& resp);
    void HandleIndex(const HttpRequest& req, HttpResponse& resp);
    void HandleSchemas(const HttpRequest& req, HttpResponse& resp); // GET /api/schemas
    void HandleSchema(const HttpRequest& req, HttpResponse& resp);  // POST /api/schema
    void HandleSqlConsole(const HttpRequest& req, HttpResponse& resp);
    void HandleExecuteSql(const HttpRequest& req, HttpResponse& resp);
    void HandleLogin(const HttpRequest& req, HttpResponse& resp);
    void HandleLoginPage(const HttpRequest& req, HttpResponse& resp);
    std::string CheckAuth(const HttpRequest& req, HttpResponse& resp);
    SessionContext& GetSession(const std::string& token);
    bool CommitTxn(SessionContext& session, std::string& err);
    bool RollbackTxn(SessionContext& session, std::string& err);
    void HandleCreateDatabase(const HttpRequest& req, HttpResponse& resp); // ��������
    void HandleListDatabases(const HttpRequest& req, HttpResponse& resp); // ��������
};

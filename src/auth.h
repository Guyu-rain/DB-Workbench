#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include "storage_engine.h"
#include "dml.h"
#include "ddl.h"

// Simple AuthManager using "system" database
class AuthManager {
public:
    AuthManager(StorageEngine& engine, DDLService& ddl, DMLService& dml);

    // Initialize system tables if not exist
    bool Init(std::string& err);

    bool Login(const std::string& user, const std::string& pass, std::string& token, std::string& err);
    
    // Check if token is valid and return associated username
    // Returns empty string if invalid
    std::string ValidateToken(const std::string& token);

    // DCL Operations
    bool CreateUser(const std::string& user, const std::string& pass, std::string& err);
    bool DropUser(const std::string& user, std::string& err);
    bool Grant(const std::string& user, const std::string& table, const std::vector<std::string>& privs, std::string& err);
    bool Revoke(const std::string& user, const std::string& table, const std::vector<std::string>& privs, std::string& err);
    
    // Check Permission
    // accessType: SELECT, INSERT, UPDATE, DELETE, CREATE, DROP...
    bool CheckPermission(const std::string& user, const std::string& table, const std::string& accessType);

private:
    StorageEngine& engine_;
    DDLService& ddl_;
    DMLService& dml_;

    std::unordered_map<std::string, std::string> token_user_;

    const std::string kSystemDb = "system";
    const std::string kUserTable = "_users";
    const std::string kPrivTable = "_privileges";
    
    std::string GetSystemDbf();
    std::string GetSystemDat();
};

#include "auth.h"
#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <cctype>
#include <chrono>
#include "path_utils.h"

namespace {
std::string TrimString(std::string s) {
    auto notSpace = [](unsigned char ch) { return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

std::string StripTrailingSemicolon(std::string s) {
    if (!s.empty() && s.back() == ';') s.pop_back();
    return s;
}

std::string StripQuotes(std::string s) {
    if (s.size() >= 2) {
        char f = s.front();
        char b = s.back();
        if ((f == '\'' && b == '\'') || (f == '"' && b == '"')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

std::string NormalizeIdent(const std::string& s) {
    return StripQuotes(StripTrailingSemicolon(TrimString(s)));
}

std::string NormalizeAccess(const std::string& s) {
    std::string out = NormalizeIdent(s);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return out;
}

std::string GeneratePrivId() {
    static uint64_t seq = 0;
    using namespace std::chrono;
    uint64_t now = static_cast<uint64_t>(duration_cast<microseconds>(
        system_clock::now().time_since_epoch()).count());
    return std::to_string(now) + "_" + std::to_string(++seq) + "_" + std::to_string(std::rand());
}

std::string GenerateToken(const std::string& user) {
    static uint64_t seq = 0;
    using namespace std::chrono;
    uint64_t now = static_cast<uint64_t>(duration_cast<microseconds>(
        system_clock::now().time_since_epoch()).count());
    return "session_" + user + "_" + std::to_string(now) + "_" + std::to_string(++seq) + "_" + std::to_string(std::rand());
}
}

AuthManager::AuthManager(StorageEngine& engine, DDLService& ddl, DMLService& dml)
    : engine_(engine), ddl_(ddl), dml_(dml) {}

std::string AuthManager::GetSystemDbf() { return dbms_paths::DbfPath(kSystemDb); }
std::string AuthManager::GetSystemDat() { return dbms_paths::DatPath(kSystemDb); }

bool AuthManager::Init(std::string& err) {
    // Check if DB exists
    std::string dbf = GetSystemDbf();
    std::string dat = GetSystemDat(); 
    
    // Creating DB if not exists (Assume engine handles check)
    engine_.CreateDatabase(kSystemDb, err); // Ignore "already exists" error

    // Create Users Table
    // _users: username(KEY, 64), password(64)
    Field fUser; fUser.name="username"; fUser.type="char"; fUser.size=64; fUser.isKey=true; fUser.nullable=false; fUser.valid=true;
    Field fPass; fPass.name="password"; fPass.type="char"; fPass.size=64; fPass.isKey=false;fPass.nullable=false; fPass.valid=true;
    
    // Attempt Create Table (DDL handles "exists")
    // Note: DDL::CreateTable requires DBF path, DAT path.
    // However, DDLService::CreateTable logic might fail if table exists. 
    // We can try loading schema first.
    TableSchema s1;
    if (!engine_.LoadSchema(dbf, kUserTable, s1, err)) {
       // Create it
       TableSchema newUserSchema;
       newUserSchema.tableName = kUserTable;
       newUserSchema.fields = {fUser, fPass};
       if (!ddl_.CreateTable(dbf, dat, newUserSchema, err)) {
           // If error is "Table already exists", simpler to ignore
       } else {
           // Insert default admin
           CreateUser("admin", "admin", err);
       }
    }

    // Create Privileges Table
    // _privileges: id(KEY, INT), username(64), table(64), access(32)
    // Actually we don't have AUTO_INCREMENT, so "id" must be managed manually or composite key?
    // Let's us ID as string UUID or random for now, or composite check.
    // Simplification: No PK for _privileges or use "username" and allow duplicates in engine? 
    // Engine supports non-unique tables? Yes, but PK support is limited.
    // Let's use a "uuid" field.
    Field fId; fId.name="uuid"; fId.type="char"; fId.size=36; fId.isKey=true; fId.nullable=false; fId.valid=true;
    Field fpUser; fpUser.name="username"; fpUser.type="char"; fpUser.size=64; fpUser.isKey=false; fpUser.nullable=false; fpUser.valid=true;
    Field fpTable; fpTable.name="tablename"; fpTable.type="char"; fpTable.size=64; fpTable.isKey=false; fpTable.nullable=false; fpTable.valid=true;
    Field fpAccess; fpAccess.name="access"; fpAccess.type="char"; fpAccess.size=32; fpAccess.isKey=false; fpAccess.nullable=false; fpAccess.valid=true;

    TableSchema s2;
    if (!engine_.LoadSchema(dbf, kPrivTable, s2, err)) {
       TableSchema newPrivSchema;
       newPrivSchema.tableName = kPrivTable;
       newPrivSchema.fields = {fId, fpUser, fpTable, fpAccess};
       ddl_.CreateTable(dbf, dat, newPrivSchema, err);
    }
    
    return true;
}

bool AuthManager::CreateUser(const std::string& user, const std::string& pass, std::string& err) {
    if (user.empty()) { err = "Username empty"; return false; }
    
    // Check exist
    TableSchema schema;
    engine_.LoadSchema(GetSystemDbf(), kUserTable, schema, err);
    
    std::vector<Condition> conds;
    Condition c; c.fieldName="username"; c.op="="; c.value=user; conds.push_back(c);
    
    // Since we don't have a "SelectCount", we read.
    std::vector<Record> recs;
    engine_.ReadRecords(GetSystemDat(), schema, recs, err);
    bool exists = false;
    for(auto& r : recs) { if(r.valid && dml_.Match(schema, r, conds)) exists = true; }
    
    if (exists) { err = "User already exists"; return false; }
    
    Record r; r.valid=true; r.values.push_back(user); r.values.push_back(pass);
    return dml_.Insert(GetSystemDat(), schema, {r}, err);
}

bool AuthManager::DropUser(const std::string& user, std::string& err) {
    if (user == "admin") { err = "Cannot drop admin"; return false; }

    TableSchema schema;
    engine_.LoadSchema(GetSystemDbf(), kUserTable, schema, err);
    Condition c; c.fieldName="username"; c.op="="; c.value=user;
    if (!dml_.Delete(GetSystemDat(), schema, {c}, err)) return false;

    // Drop privileges
    TableSchema pSchema;
    if(engine_.LoadSchema(GetSystemDbf(), kPrivTable, pSchema, err)) {
        Condition pc; pc.fieldName="username"; pc.op="="; pc.value=user;
         dml_.Delete(GetSystemDat(), pSchema, {pc}, err);
    }
    return true;
}

bool AuthManager::Grant(const std::string& user, const std::string& table, const std::vector<std::string>& privs, std::string& err) {
    std::string normUser = NormalizeIdent(user);
    std::string normTable = NormalizeIdent(table);
    // Check user exists
    TableSchema uSchema;
    engine_.LoadSchema(GetSystemDbf(), kUserTable, uSchema, err);
    std::vector<Record> urecs;
    engine_.ReadRecords(GetSystemDat(), uSchema, urecs, err);
    bool uFound = false;
    for(auto& r : urecs) { if (r.valid && NormalizeIdent(r.values[0]) == normUser) uFound=true; }
    if (!uFound) { err = "User does not exist"; return false; }

    TableSchema pSchema;
    engine_.LoadSchema(GetSystemDbf(), kPrivTable, pSchema, err);

    for (const auto& priv : privs) {
        std::string normPriv = NormalizeAccess(priv);
        // Check if already has? Ignoring for now, just append. (Deduplication would be better)
        // Simplification: Insert naive.
        Record r; r.valid=true;
        r.values.push_back(GeneratePrivId());
        r.values.push_back(normUser);
        r.values.push_back(normTable);
        r.values.push_back(normPriv);
        if (!dml_.Insert(GetSystemDat(), pSchema, {r}, err)) return false;
    }
    return true;
}

bool AuthManager::Revoke(const std::string& user, const std::string& table, const std::vector<std::string>& privs, std::string& err) {
    std::string normUser = NormalizeIdent(user);
    std::string normTable = NormalizeIdent(table);
    TableSchema pSchema;
    engine_.LoadSchema(GetSystemDbf(), kPrivTable, pSchema, err);
    
    for (const auto& priv : privs) {
        std::string normPriv = NormalizeAccess(priv);
        std::vector<Condition> conds;
        { Condition c; c.fieldName="username"; c.op="="; c.value=normUser; conds.push_back(c); }
        { Condition c; c.fieldName="tablename"; c.op="="; c.value=normTable; conds.push_back(c); }
        { Condition c; c.fieldName="access"; c.op="="; c.value=normPriv; conds.push_back(c); }
        
        dml_.Delete(GetSystemDat(), pSchema, conds, err);
    }
    return true;
}

bool AuthManager::CheckPermission(const std::string& user, const std::string& table, const std::string& accessType) {
    std::string normUser = NormalizeIdent(user);
    std::string normTable = NormalizeIdent(table);
    std::string need = NormalizeAccess(accessType);
    if (normUser == "admin") return true; // Superuser
    
    // Check _privileges
    std::string err;
    TableSchema pSchema;
    if(!engine_.LoadSchema(GetSystemDbf(), kPrivTable, pSchema, err)) return false;

    std::vector<Record> recs;
    if(!engine_.ReadRecords(GetSystemDat(), pSchema, recs, err)) return false;
    
    for(const auto& r : recs) {
        if (!r.valid) continue;
        // username, tablename, access
        // indices: 1, 2, 3
        std::string ru = NormalizeIdent(r.values[1]);
        std::string rt = NormalizeIdent(r.values[2]);
        std::string ra = NormalizeAccess(r.values[3]);
        if (ru == normUser && (rt == normTable || rt == "*")) {
            if (ra == need || ra == "ALL") return true; 
        }
    }
    return false;
}

bool AuthManager::Login(const std::string& user, const std::string& pass, std::string& token, std::string& err) {
    TableSchema uSchema;
    if(!engine_.LoadSchema(GetSystemDbf(), kUserTable, uSchema, err)) { err="System/User table missing"; return false; }
    
    std::vector<Condition> conds;
    { Condition c; c.fieldName="username"; c.op="="; c.value=user; conds.push_back(c); }
    
    std::vector<Record> recs;
    engine_.ReadRecords(GetSystemDat(), uSchema, recs, err);
    
    for(const auto& r : recs) {
        if (r.valid && dml_.Match(uSchema, r, conds)) {
            // Check password
            if (r.values[1] == pass) {
                // Generate token
                token = GenerateToken(user);
                token_user_[token] = user;
                return true;
            }
        }
    }
    err = "Invalid credentials";
    return false;
}

std::string AuthManager::ValidateToken(const std::string& token) {
    auto it = token_user_.find(token);
    if (it != token_user_.end()) return it->second;
    return std::string();
}

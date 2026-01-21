#include "api_server.h"
#include "parser.h"
#include <algorithm>
#include <string>
#include <cctype>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <sstream>
#include <set>
#include <filesystem>
#include <cstdlib>
#include "path_utils.h"

#if defined(_WIN32)
  #ifndef NOMINMAX
  #define NOMINMAX
  #endif
  #ifndef WIN32_LEAN_AND_MEAN
  #define WIN32_LEAN_AND_MEAN
  #endif
  #include <windows.h>
#elif defined(__APPLE__)
  #include <mach-o/dyld.h>
  #include <limits.h>
#else
  #include <unistd.h>
  #include <limits.h>
#endif

namespace fs = std::filesystem;

namespace {
std::string ToLower(std::string s) {
std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
return s;
}

bool IsLockTimeout(const std::string& err) {
    return err.find("Lock timeout") != std::string::npos;
}

std::string ToUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

std::string Trim(std::string s) {
    auto notSpace = [](unsigned char ch) { return !std::isspace(ch); };
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), notSpace));
    s.erase(std::find_if(s.rbegin(), s.rend(), notSpace).base(), s.end());
    return s;
}

std::vector<std::string> Split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        // ע�⣺���ﲻҪ����������� "a,,b" ���� a,b����������
        if (!item.empty()) out.push_back(item);
    }
    return out;
}


std::string JsonEscape(const std::string& s) {
std::string out;
out.reserve(s.size() + 4);
for (char c : s) {
    switch (c) {
    case '"': out += "\\\""; break;
    case '\\': out += "\\\\"; break;
    case '\n': out += "\\n"; break;
    case '\r': out += "\\r"; break;
    case '\t': out += "\\t"; break;
    default: out.push_back(c);
    }
}
return out;
}

// ====== REPLACE ParseFilter() ======
std::vector<Condition> ParseFilter(const std::string& filter) {
    std::vector<Condition> conds;
    if (filter.empty()) return conds;

    // Support:
    //   name = "Alice"
    //   age >= 18
    //   status != suspended
    //   name contains Bob
    // Also supports simple AND: "age >= 18 AND status = active"
    auto upper = ToUpper(filter);

    // Split by AND (very simple)
    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        size_t pos = upper.find("AND", start);
        if (pos == std::string::npos) { parts.push_back(filter.substr(start)); break; }
        parts.push_back(filter.substr(start, pos - start));
        start = pos + 3;
    }

    std::regex r(R"(\s*([A-Za-z_]\w*)\s*(=|!=|>=|<=|>|<)\s*('([^']*)'|\"([^\"]*)\"|([^\s]+))\s*)");
    for (auto& p : parts) {
        std::string s = Trim(p);
        if (s.empty()) continue;

        // contains syntax: "field contains value"
        {
            std::regex cr(R"(\s*([A-Za-z_]\w*)\s+CONTAINS\s+(.+)\s*)", std::regex_constants::icase);
            std::smatch cm;
            if (std::regex_match(s, cm, cr)) {
                Condition c;
                c.fieldName = cm[1];
                c.op = "CONTAINS";
                c.value = Trim(cm[2]);
                // strip quotes if any
                if (c.value.size() >= 2 && ((c.value.front() == '"' && c.value.back() == '"') || (c.value.front() == '\'' && c.value.back() == '\''))) {
                    c.value = c.value.substr(1, c.value.size() - 2);
                }
                conds.push_back(c);
                continue;
            }
        }

        std::smatch m;
        if (std::regex_match(s, m, r)) {
            Condition c;
            c.fieldName = m[1];
            c.op = m[2];
            if (m[4].matched) c.value = m[4];      // '...'
            else if (m[5].matched) c.value = m[5]; // "..."
            else c.value = m[6];                   // bare token
            conds.push_back(c);
        }
    }
    return conds;
}

int InferSizeFromType(const std::string& rawType, std::string& canonicalType, std::string& err) {
    std::string t = ToLower(rawType);
    t = std::regex_replace(t, std::regex("\\s+"), "");

    // Integers
    if (t == "int" || t == "integer") { canonicalType = "int"; return 4; }
    if (t == "bigint" || t == "long") { canonicalType = "bigint"; return 8; }
    if (t == "smallint" || t == "short") { canonicalType = "smallint"; return 2; }
    if (t == "tinyint") { canonicalType = "tinyint"; return 1; }

    // Floating Points
    if (t == "double" || t == "float" || t == "real" || t == "decimal" || t == "numeric") {
        canonicalType = "double";
        return 8; 
    }

    // Boolean
    if (t == "bool" || t == "boolean") { canonicalType = "boolean"; return 1; }

    // Date/Time
    if (t == "date") { canonicalType = "date"; return 10; }
    if (t == "datetime") { canonicalType = "datetime"; return 19; }
    if (t == "timestamp") { canonicalType = "timestamp"; return 19; }

    // Strings: char[n], varchar[n], text
    if (t == "text") { canonicalType = "text"; return 65535; }
    
    // char[32] / char(32) / varchar(32)
    {
        std::smatch m;
        // Match char[n], char(n), varchar(n)
        if (std::regex_match(t, m, std::regex(R"(^(char|varchar|string)(\[|\()(\d+)(\]|\))$)"))) {
            canonicalType = "char[" + m[3].str() + "]";
            return std::stoi(m[3].str());
        }
    }

    // Fallback for implicitly sized strings? 
    // Let's stick to strict validation or default.
    // err = "Unsupported field type: " + rawType + " (supported: int, double, date, char[n], etc.)";
    // canonicalType = rawType;
    // return 0;
    
    // Allow unknown types as generic strings for flexibility, default 255
    canonicalType = rawType; // keep original if unknown
    return 255; 
}

bool BuildSchemaFromCreateSql(const std::string& sql, TableSchema& out, std::string& err) {
    Parser p;
    ParsedCommand pc = p.Parse(sql, err);
    if (!err.empty()) return false;
    if (pc.type != CommandType::kCreate) {
        err = "SQL is not a CREATE TABLE statement";
        return false;
    }
    if (pc.schema.tableName.empty()) {
        err = "Missing table name";
        return false;
    }
    if (pc.schema.fields.empty()) {
        err = "CREATE TABLE must include at least 1 field";
        return false;
    }

    out = pc.schema;

    // infer sizes & normalize types
    for (auto& f : out.fields) {
        std::string canonical;
        std::string typeErr;
        int sz = InferSizeFromType(f.type, canonical, typeErr);
        if (sz <= 0) { err = typeErr; return false; }
        f.type = canonical;
        f.size = sz;
    }

    // If no explicit key: prefer "Id"; otherwise use first field.
    bool hasKey = false;
    for (const auto& f : out.fields) if (f.isKey) { hasKey = true; break; }
    if (!hasKey) {
        size_t keyIdx = 0;
        for (size_t i = 0; i < out.fields.size(); ++i) {
            if (ToLower(out.fields[i].name) == "id") { keyIdx = i; break; }
        }
        out.fields[keyIdx].isKey = true;
        out.fields[keyIdx].nullable = false;
        if (keyIdx != 0) std::swap(out.fields[0], out.fields[keyIdx]);
    }
    return true;
}

static std::string ReadFileIfExists(const fs::path& path) {
std::ifstream ifs(path, std::ios::binary);
if (!ifs.is_open()) return {};
std::ostringstream oss;
oss << ifs.rdbuf();
return oss.str();
}

static fs::path GetExecutableDir() {
#if defined(_WIN32)
char buf[MAX_PATH];
DWORD n = GetModuleFileNameA(nullptr, buf, MAX_PATH);
if (n == 0) return fs::current_path();
return fs::path(std::string(buf, n)).parent_path();

#elif defined(__APPLE__)
uint32_t size = 0;
_NSGetExecutablePath(nullptr, &size);
std::string tmp(size, '\0');
if (_NSGetExecutablePath(tmp.data(), &size) != 0) return fs::current_path();
return fs::weakly_canonical(fs::path(tmp)).parent_path();

#else
char buf[PATH_MAX];
ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
if (n <= 0) return fs::current_path();
buf[n] = '\0';
return fs::path(buf).parent_path();
#endif
}

// 核心：稳定读取 docs/ 下的文件，不依赖 cwd
static std::string ReadDocFile(const std::string& filename) {
// 1) 环境变量最优先（便于部署/运行配置）
if (const char* env = std::getenv("DBMS_DOCS_DIR"); env && *env) {
    fs::path base(env);
    auto s = ReadFileIfExists(base / filename);
    if (!s.empty()) return s;
}

// 2) 可执行文件目录旁边的 ./docs（你 CMake copy 到 build/docs 就靠它）
{
    fs::path base = GetExecutableDir() / "docs";
    auto s = ReadFileIfExists(base / filename);
    if (!s.empty()) return s;
}

// 3) 当前工作目录 ./docs
{
    fs::path base = fs::current_path() / "docs";
    auto s = ReadFileIfExists(base / filename);
    if (!s.empty()) return s;
}

// 4) 兼容旧逻辑：../docs, ../../docs
{
    fs::path base1 = fs::current_path().parent_path() / "docs";
    auto s1 = ReadFileIfExists(base1 / filename);
    if (!s1.empty()) return s1;

    fs::path base2 = fs::current_path().parent_path().parent_path() / "docs";
    auto s2 = ReadFileIfExists(base2 / filename);
    if (!s2.empty()) return s2;
}

return {};
}

static std::string ReadFileIfExistsPath(const fs::path& p) {
    std::ifstream ifs(p, std::ios::binary);
    if (!ifs.is_open()) return {};
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

int InferSizeFromType(const std::string& typeRaw) {
    std::string t = typeRaw;
    // normalize spaces
    t.erase(std::remove_if(t.begin(), t.end(), [](unsigned char c) { return std::isspace(c); }), t.end());

    std::string upper = ToUpper(t);
    // Integer types
    if (upper == "INT" || upper.find("INT") == 0 || upper == "INTEGER") return 4;
    if (upper == "BIGINT" || upper == "LONG") return 8;
    if (upper == "SMALLINT" || upper == "TINYINT") return 2;

    // Floating point types
    if (upper == "FLOAT" || upper == "DOUBLE" || upper == "WNUMBER") return 8;

    // Boolean
    if (upper == "BOOL" || upper == "BOOLEAN") return 1;

    // Date/Time
    if (upper == "DATE") return 10; // YYYY-MM-DD
    if (upper == "DATETIME") return 19; // YYYY-MM-DD HH:MM:SS
    if (upper == "TIMESTAMP") return 8;

    // String types
    // char[32] / varchar[32]
    auto lb = t.find('[');
    auto rb = t.find(']');
    if (lb != std::string::npos && rb != std::string::npos && rb > lb + 1) {
        try { return std::stoi(t.substr(lb + 1, rb - lb - 1)); }
        catch (...) {}
    }

    // char(32) / varchar(32)
    auto lp = t.find('(');
    auto rp = t.find(')');
    if (lp != std::string::npos && rp != std::string::npos && rp > lp + 1) {
        try { return std::stoi(t.substr(lp + 1, rp - lp - 1)); }
        catch (...) {}
    }

    // Default varchar size if not specified? 
    if (upper.find("VARCHAR") == 0 || upper.find("CHAR") == 0 || upper == "STRING") {
        return 255;
    }

    return 0; // unknown
}
}

ApiServer::ApiServer(StorageEngine& engine,
    DDLService& ddl,
    DMLService& dml,
    QueryService& query,
    LogManager& log,
    TxnManager& txn_manager,
    LockManager& lock_manager,
    const std::string& dbfPath,
    const std::string& datPath)
    : engine_(engine),
    ddl_(ddl),
    dml_(dml),
    query_(query),
    log_(log),
    txn_manager_(txn_manager),
    lock_manager_(lock_manager),
    auth_(engine, ddl, dml), // Init Auth
    dbfPath_(dbfPath),
    datPath_(datPath),
    currentDbf_(dbfPath),
    currentDat_(datPath) {
    std::string base = std::filesystem::path(dbfPath).stem().string();
    currentDbName_ = base.empty() ? "default" : base;
    std::string err;
    auth_.Init(err); 
    }

void ApiServer::HandleIndex(const HttpRequest& req, HttpResponse& resp) {
    (void)req;
    std::string html = ReadDocFile("workbench.html");
    if (html.empty()) {
        resp.status = 404;
        resp.contentType = "text/plain; charset=utf-8";
        resp.body = "Missing workbench.html";
        return;
    }
    resp.status = 200;
    resp.contentType = "text/html; charset=utf-8";
    resp.body = html;
}

void ApiServer::HandleSqlConsole(const HttpRequest& req, HttpResponse& resp) {
  (void)req;
  std::string html = ReadDocFile("sql_console.html");
  if (html.empty()) {
    resp.status = 404;
    resp.contentType = "text/plain; charset=utf-8";
    resp.body = "Missing sql_console.html";
    return;
  }
  resp.status = 200;
  resp.contentType = "text/html; charset=utf-8";
  resp.body = html;
}

void ApiServer::Run(uint16_t port) {
  SimpleHttpServer server;
  // Login
  server.Post("/api/login", [this](const HttpRequest& req, HttpResponse& resp) { HandleLogin(req, resp); });
  // Login Page
  server.Get("/login.html", [this](const HttpRequest& req, HttpResponse& resp) { HandleLoginPage(req, resp); });


  server.Post("/api/query", [this](const HttpRequest& req, HttpResponse& resp) { HandleQuery(req, resp); });
  server.Post("/api/insert", [this](const HttpRequest& req, HttpResponse& resp) { HandleInsert(req, resp); });
  server.Post("/api/update", [this](const HttpRequest& req, HttpResponse& resp) { HandleUpdate(req, resp); });
  server.Post("/api/delete", [this](const HttpRequest& req, HttpResponse& resp) { HandleDelete(req, resp); });
  server.Post("/api/create_table", [this](const HttpRequest& req, HttpResponse& resp) { HandleCreateTable(req, resp); });
  server.Get("/api/tables", [this](const HttpRequest& req, HttpResponse& resp) { HandleListTables(req, resp); });
  server.Get("/api/schemas", [this](const HttpRequest& req, HttpResponse& resp) { HandleSchemas(req, resp); });
  server.Post("/api/schemas", [this](const HttpRequest& req, HttpResponse& resp) { HandleSchema(req, resp); });
  // Add SQL console endpoint
  server.Get("/sql", [this](const HttpRequest& req, HttpResponse& resp) { HandleSqlConsole(req, resp); });
  server.Post("/api/sql", [this](const HttpRequest& req, HttpResponse& resp) { HandleExecuteSql(req, resp); });

  server.Get("/", [this](const HttpRequest& req, HttpResponse& resp) { HandleIndex(req, resp); });
  server.Post("/api/use_database",[this](const HttpRequest& req, HttpResponse& resp) {HandleUseDatabase(req, resp);});
  server.Post("/api/create_database", [this](const HttpRequest& req, HttpResponse& resp) {
      HandleCreateDatabase(req, resp);
      });
  server.Get("/api/databases", [this](const HttpRequest& req, HttpResponse& resp) {
      HandleListDatabases(req, resp);
      });
  server.Start(port);
}

std::string ApiServer::DataPath(const std::string& table) const {
  std::string lower = ToLower(table);
  /*if (lower == "users") return currentDat_;
  return datPath_ + "_" + lower + ".dat";*/
  return currentDat_;

}

bool ApiServer::LoadSchema(const std::string& table, TableSchema& out, std::string& err) {
  std::vector<TableSchema> schemas;
  if (!engine_.LoadSchemas(currentDbf_, schemas, err)) return false;
  auto needle = ToLower(table);
  for (const auto& s : schemas) {
    if (ToLower(s.tableName) == needle) { out = s; return true; }
  }
  err = "Table not found";
  return false;
}

std::vector<TableSchema> ApiServer::ListSchemas() {
  std::vector<TableSchema> schemas;
  std::string err;
  engine_.LoadSchemas(currentDbf_, schemas, err);
  return schemas;
}

std::vector<Record> ApiServer::LoadAll(const TableSchema& schema, const std::string& dataPath, std::string& err) {
  std::vector<Record> out;
  engine_.ReadRecords(dataPath, schema, out, err);
  return out;
}

std::string ApiServer::BuildSql(const std::string& schemaName, const std::string& table, const std::string& filter, int limit) const {
  std::ostringstream oss;
  oss << "SELECT * FROM " << schemaName << "." << table;
  if (!filter.empty()) oss << " WHERE " << filter;
  oss << " LIMIT " << limit << ";";
  return oss.str();
}

std::string ApiServer::SerializeRows(const TableSchema& schema, const std::vector<Record>& rows) const {
  std::ostringstream oss;
  oss << '[';
  for (size_t i = 0; i < rows.size(); ++i) {
    if (i) oss << ',';
    oss << '{';
    for (size_t f = 0; f < schema.fields.size() && f < rows[i].values.size(); ++f) {
      if (f) oss << ',';
      std::string key = ToLower(schema.fields[f].name);
      oss << '"' << JsonEscape(key) << '"' << ':' << '"' << JsonEscape(rows[i].values[f]) << '"';
    }
    oss << '}';
  }
  oss << ']';
  return oss.str();
}

std::string ApiServer::Error(const std::string& msg, int code) const {
  (void)code;
  return std::string("{\"ok\":false,\"error\":\"") + JsonEscape(msg) + "\"}";
}

std::string ApiServer::Success(const std::string& body) const {
  std::string trimmed = body;
  if (!trimmed.empty() && trimmed.front() == ',') trimmed.erase(trimmed.begin());
  return "{\"ok\":true" + trimmed + "}";
}

bool ApiServer::EnsureDefaultDb(std::string& err) {
  if (currentDbName_ != "MyDB") return true;

  if (!dbms_paths::EnsureDbDir(currentDbName_, err)) return false;

  std::vector<TableSchema> schemas;
  if (!engine_.LoadSchemas(currentDbf_, schemas, err)) return false;

  auto it = std::find_if(schemas.begin(), schemas.end(),
                         [](const TableSchema& s) { return s.tableName == "Users"; });
  if (it != schemas.end()) return true;

  TableSchema schema;
  schema.tableName = "Users";
  schema.fields = {
      {"Id", "int", 4, true,  false, true},
      {"Name", "char[32]", 32, false, false, true},
      {"Age", "int", 4, false, true,  true},
      {"Role", "char[16]", 16, false, true,  true},
      {"Status","char[16]", 16, false, true,  true},
  };

  return ddl_.CreateTable(currentDbf_, currentDat_, schema, err);
}

void ApiServer::HandleExecuteSql(const HttpRequest& req, HttpResponse& resp) {
    std::string err;
    if (!EnsureDefaultDb(err)) { resp.status = 500; resp.body = Error(err, 500); return; }
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty() || !root.IsObject()) {
        resp.status = 400; resp.body = Error("Invalid JSON"); return;
    }
    
    std::string fullSql = root.Get("sql") ? root.Get("sql")->AsString("") : "";
    if (fullSql.empty()) { resp.status = 400; resp.body = Error("Empty SQL"); return; }

    // Strip SQL comments: --, #, /* */
    {
        std::string noComment;
        noComment.reserve(fullSql.size());
        bool inSingle = false;
        bool inDouble = false;

        for (size_t i = 0; i < fullSql.size(); ++i) {
            char c = fullSql[i];
            char n = (i + 1 < fullSql.size()) ? fullSql[i + 1] : '\0';

            if (!inSingle && !inDouble) {
                if (c == '-' && n == '-') {
                    i += 2;
                    while (i < fullSql.size() && fullSql[i] != '\n' && fullSql[i] != '\r') ++i;
                    if (i < fullSql.size()) noComment.push_back('\n');
                    continue;
                }
                if (c == '#') {
                    ++i;
                    while (i < fullSql.size() && fullSql[i] != '\n' && fullSql[i] != '\r') ++i;
                    if (i < fullSql.size()) noComment.push_back('\n');
                    continue;
                }
                if (c == '/' && n == '*') {
                    i += 2;
                    while (i + 1 < fullSql.size() && !(fullSql[i] == '*' && fullSql[i + 1] == '/')) ++i;
                    if (i + 1 < fullSql.size()) ++i; // consume '/'
                    noComment.push_back('\n');
                    continue;
                }
            }

            if (c == '\'' && !inDouble) inSingle = !inSingle;
            if (c == '"' && !inSingle) inDouble = !inDouble;

            noComment.push_back(c);
        }
        fullSql.swap(noComment);
    }
    
    // Split SQL by semicolon
    std::vector<std::string> statements;
    std::string current;
    bool inQuote = false;
    char quoteChar = 0;
    
    for (char c : fullSql) {
        if (inQuote) {
            current += c;
            if (c == quoteChar) {
                 inQuote = false;
            }
        } else {
            if (c == '\'' || c == '"') {
                inQuote = true;
                quoteChar = c;
                current += c;
            } else if (c == ';') {
                if (!current.empty()) {
                    statements.push_back(current);
                    current.clear();
                }
            } else {
                current += c;
            }
        }
    }
    if (!current.empty()) statements.push_back(current);

    // Remove empty statements
    std::vector<std::string> validStatements;
    for (auto& s : statements) {
        std::string trimmed = s; 
        while(!trimmed.empty() && isspace((unsigned char)trimmed.front())) trimmed.erase(0, 1);
        while(!trimmed.empty() && isspace((unsigned char)trimmed.back())) trimmed.pop_back();
        if (!trimmed.empty()) validStatements.push_back(trimmed);
    }
    
    if (validStatements.empty()) {
        resp.status = 200; resp.body = "{\"ok\":true,\"message\":\"No commands to execute\"}";
        return;
    }

    std::string lastResultBody;
    int lastStatus = 200;
    std::string lastDbName;
    
    // Auth Check
    std::string user = CheckAuth(req, resp);
    if (user.empty()) return;
    std::string token;
    auto tokIt = req.headers.find("authorization");
    if (tokIt != req.headers.end()) token = tokIt->second;
    SessionContext& session = GetSession(token);

    for (const auto& sql : validStatements) {
        err.clear();
        Parser p;
        ParsedCommand cmd = p.Parse(sql, err);
        if (!err.empty()) { resp.status = 400; resp.body = Error("Error in statement: " + sql + " => " + err); return; }

        if (cmd.type == CommandType::kBegin) {
            if (session.current_txn) { resp.status=400; resp.body=Error("Transaction already active"); return; }
            Txn* txn = txn_manager_.Begin(currentDbName_, err);
            if (!txn) { resp.status=500; resp.body=Error(err); return; }
            session.current_txn = txn;
            session.autocommit = false;
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Transaction started\"}";
            continue;
        }
        if (cmd.type == CommandType::kCommit) {
            if (!CommitTxn(session, err)) { resp.status=400; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Transaction committed\"}";
            continue;
        }
        if (cmd.type == CommandType::kRollback) {
            if (!RollbackTxn(session, err)) { resp.status=400; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Transaction rolled back\"}";
            continue;
        }
        if (cmd.type == CommandType::kSavepoint) {
            if (!session.current_txn) { resp.status=400; resp.body=Error("No active transaction"); return; }
            if (!txn_manager_.Savepoint(session.current_txn, cmd.savepointName, err)) { resp.status=400; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Savepoint created\"}";
            continue;
        }
        if (cmd.type == CommandType::kRollbackTo) {
            if (!session.current_txn) { resp.status=400; resp.body=Error("No active transaction"); return; }
            if (!txn_manager_.RollbackTo(session.current_txn, cmd.savepointName, err)) { resp.status=400; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Rolled back to savepoint\"}";
            continue;
        }
        if (cmd.type == CommandType::kRelease) {
            if (!session.current_txn) { resp.status=400; resp.body=Error("No active transaction"); return; }
            if (!txn_manager_.ReleaseSavepoint(session.current_txn, cmd.savepointName, err)) { resp.status=400; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Savepoint released\"}";
            continue;
        }

        if (cmd.type == CommandType::kCheckpoint) {
            if (session.current_txn) { resp.status=400; resp.body=Error("CHECKPOINT not allowed in active transaction"); return; }
            log_.SetDbName(currentDbName_);
            LogRecord rec;
            rec.txn_id = 0;
            rec.type = LogType::CHECKPOINT;
            LSN lsn = log_.Append(rec, err);
            if (lsn == 0) { resp.status=500; resp.body=Error(err); return; }
            if (!log_.Flush(lsn, err)) { resp.status=500; resp.body=Error(err); return; }
            if (!log_.TruncateWithBackup(err)) { resp.status=500; resp.body=Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Checkpoint created\"}";
            continue;
        }

        if (cmd.type == CommandType::kUseDatabase) {
           // Should check permission? "USE" generally allowed if you can connect? 
           // Let's assume yes.
           std::string db = cmd.dbName;
           if (db.empty()) { resp.status=400; resp.body=Error("Missing database name"); return; }
           if (session.current_txn) { resp.status=400; resp.body=Error("Cannot change database during active transaction"); return; }
           
            currentDbf_ = dbms_paths::DbfPath(db);
            currentDat_ = dbms_paths::DatPath(db);
    currentDbName_ = db;
    log_.SetDbName(db);
            log_.SetDbName(db);
            
            lastDbName = currentDbName_;
            lastStatus = 200;
            lastResultBody = "{\"ok\":true,\"message\":\"Switched to database " + JsonEscape(db) + "\"}";
            continue;
        }

        if (cmd.type == CommandType::kBackup) {
            if (user != "admin") {
                resp.status = 403;
                resp.body = Error("Permission denied: Only admin can backup database");
                return;
            }
            if (!engine_.BackupDatabase(cmd.dbName, cmd.backupPath, err)) {
                resp.status = 500;
                resp.body = Error("Backup failed: " + err);
                return;
            }
            lastStatus = 200;
            lastResultBody = "{\"ok\":true,\"message\":\"Database " + JsonEscape(cmd.dbName) + " backed up to " + JsonEscape(cmd.backupPath) + "\"}";
            continue;
        }
        
        // --- DCL Commands ---
        if (cmd.type == CommandType::kCreateUser) {
             if (!auth_.CheckPermission(user, "", "SUPER")) { // Only admin can create user? Or special priv?
                  // Hack: only 'admin' user can do user management
                  if (user != "admin") { resp.status=403; resp.body = Error("Permission denied: Only admin can create users"); return; }
             }
             if (!auth_.CreateUser(cmd.username, cmd.password, err)) {
                 resp.status = 400; resp.body = Error(err); return;
             }
             lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"User created\"}";
             continue;
        }
        if (cmd.type == CommandType::kDropUser) {
             if (user != "admin") { resp.status=403; resp.body = Error("Permission denied"); return; }
             if (!auth_.DropUser(cmd.username, err)) { resp.status=400; resp.body=Error(err); return; }
             lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"User dropped\"}";
             continue;
        }
        if (cmd.type == CommandType::kGrant) {
            // GRANT ALL ON table TO user
            // Who can grant? Admin or owner.
            if (user != "admin") { 
                // Check if user has GRANT option or owns table? Simplification: Only admin.
                resp.status=403; resp.body = Error("Permission denied"); return; 
            }
            if (!auth_.Grant(cmd.username, cmd.tableName, cmd.privileges, err)) {
                resp.status=400; resp.body=Error(err); return;
            }
             lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Privileges granted\"}";
             continue;
        }
        if (cmd.type == CommandType::kRevoke) {
            if (user != "admin") { resp.status=403; resp.body = Error("Permission denied"); return; }
            if (!auth_.Revoke(cmd.username, cmd.tableName, cmd.privileges, err)) {
                resp.status=400; resp.body=Error(err); return;
            }
             lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Privileges revoked\"}";
             continue;
        }
        // --------------------

        // Perform Permission Checks for other commands
        std::string accessNeeded = "";
        std::string targetTable = cmd.tableName;
        
        switch(cmd.type) {
            case CommandType::kSelect: accessNeeded="SELECT"; targetTable=cmd.tableName.empty() ? cmd.query.joinTable : cmd.tableName; break; 
            case CommandType::kInsert: accessNeeded="INSERT"; break;
            case CommandType::kUpdate: accessNeeded="UPDATE"; break;
            case CommandType::kDelete: accessNeeded="DELETE"; break;
            case CommandType::kCreate: accessNeeded="CREATE"; break; // Table?
            case CommandType::kDrop:   accessNeeded="DROP"; break;
            case CommandType::kAlter:  accessNeeded="ALTER"; break; // Custom priv
            case CommandType::kCheckpoint: accessNeeded="CREATE"; break;
            case CommandType::kCreateIndex: accessNeeded="INDEX"; break;
            case CommandType::kDropIndex: accessNeeded="INDEX"; break;
            // ...
            default: break; 
        }
        
        if (!accessNeeded.empty()) {
             // For SELECT join, need both? 
             if (!auth_.CheckPermission(user, targetTable, accessNeeded)) {
                 resp.status = 403; 
                 resp.body = Error("Permission denied: User '" + user + "' needs '" + accessNeeded + "' on '" + targetTable + "'"); 
                 return;
             }
        }

        if (cmd.type == CommandType::kCreateDatabase) {
           if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
           if (!engine_.CreateDatabase(cmd.dbName, err)) {
                resp.status = 400; resp.body = Error(err); return;
           } else {
                lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Database created\"}";
           }
           continue;
        }

        if (cmd.type == CommandType::kDropDatabase) {
           if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
           if (cmd.actionSpecified && cmd.action == ReferentialAction::kRestrict) {
                std::string dbf = dbms_paths::DbfPath(cmd.dbName);
                std::vector<TableSchema> schemas;
                if (engine_.LoadSchemas(dbf, schemas, err)) {
                    bool hasFk = false;
                    for (const auto& s : schemas) {
                        if (!s.foreignKeys.empty()) { hasFk = true; break; }
                    }
                    if (hasFk) {
                        resp.status = 400; resp.body = Error("DROP DATABASE RESTRICT blocked by foreign keys");
                        return;
                    }
                }
           }
           if (!engine_.DropDatabase(cmd.dbName, err)) {
                resp.status = 400; resp.body = Error(err); return;
           } else {
                if (currentDbName_ == cmd.dbName) {
                    std::string base = std::filesystem::path(dbfPath_).stem().string();
                    currentDbName_ = base.empty() ? "default" : base;
                    currentDbf_ = dbfPath_;
                    currentDat_ = datPath_;
                    log_.SetDbName(currentDbName_);
                }
                lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Database dropped\"}";
           }
           continue;
        }

        if (cmd.type == CommandType::kCreate) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            std::string dataPath = currentDat_;
            if (!ddl_.CreateTable(currentDbf_, dataPath, cmd.schema, err)) {
                 resp.status = 400; resp.body = Error(err); return;
            } else {
                 lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Table created\"}";
            }
            continue;
        }

        if (cmd.type == CommandType::kCreateView) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            if (!ddl_.CreateView(currentDbf_, currentDat_, cmd.viewName, cmd.viewSql, cmd.viewQuery, cmd.viewColumns, cmd.viewOrReplace, err)) {
                resp.status = 400; resp.body = Error(err); return;
            } else {
                lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"View created\"}";
            }
            continue;
        }
        
        if (cmd.type == CommandType::kInsert) {
            TableSchema schema;
            if (!LoadSchema(cmd.tableName, schema, err)) { resp.status=400; resp.body=Error(err); return; }
            if (schema.isView) { resp.status=400; resp.body=Error("Cannot insert into a view"); return; }
            
            for (const auto& rec : cmd.records) {
                if (rec.values.size() != schema.fields.size()) {
                     resp.status = 400; resp.body = Error("Column count mismatch"); return;
                }
            }

            bool implicit = false;
            if (!session.current_txn) {
                if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
                session.current_txn = txn_manager_.Begin(currentDbName_, err);
                if (!session.current_txn) { resp.status=500; resp.body=Error(err); return; }
                implicit = true;
            }

              if (!dml_.Insert(currentDat_, currentDbf_, schema, cmd.records, err, session.current_txn, &log_, &lock_manager_)) {
                   if (implicit) {
                       RollbackTxn(session, err);
                   } else if (session.current_txn && IsLockTimeout(err)) {
                       std::string rbErr;
                       RollbackTxn(session, rbErr);
                   }
                   resp.status = 500; resp.body = Error(err); return;
              } else if (implicit) {
                   if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err); return; }
              }

            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Insert successful\"}";
            continue;
        }
        
        if (cmd.type == CommandType::kSelect) {
            TableSchema schema;
            if (!LoadSchema(cmd.tableName, schema, err)) { resp.status=400; resp.body=Error(err); return; }
            
            std::vector<Record> out;
            bool implicit = false;
            if (!session.current_txn) {
                if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
                session.current_txn = txn_manager_.Begin(currentDbName_, err);
                if (!session.current_txn) { resp.status=500; resp.body=Error(err); return; }
                implicit = true;
            }
            if (!query_.Select(currentDat_, currentDbf_, schema, cmd.query, out, err, session.current_txn, &lock_manager_)) {
                 if (implicit) {
                     RollbackTxn(session, err);
                 } else if (session.current_txn && IsLockTimeout(err)) {
                     std::string rbErr;
                     RollbackTxn(session, rbErr);
                 }
                 resp.status=500; resp.body=Error(err); return;
            }
            if (implicit) {
                if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err); return; }
            }
            
            if (out.size() > 100) out.resize(100);
            
            TableSchema displaySchema = schema;
            if (!cmd.query.aggregates.empty() || !cmd.query.groupBy.empty()) {
                displaySchema.fields.clear();
                for (const auto& sel : cmd.query.selectExprs) {
                    Field f;
                    if (!sel.alias.empty()) f.name = sel.alias;
                    else if (sel.isAggregate) f.name = sel.agg.func + "(" + sel.agg.field + ")";
                    else f.name = sel.field;
                    displaySchema.fields.push_back(f);
                }
            } else if (!cmd.query.projection.empty()) {
                bool isStar = (cmd.query.projection.size() == 1 && cmd.query.projection[0] == "*");
                if (!isStar) {
                    displaySchema.fields.clear();
                    for (size_t i = 0; i < cmd.query.projection.size(); ++i) {
                        Field f;
                        std::string alias;
                        if (i < cmd.query.projectionAliases.size()) alias = cmd.query.projectionAliases[i];
                        f.name = alias.empty() ? cmd.query.projection[i] : alias; 
                        displaySchema.fields.push_back(f);
                    }
                } else if (!cmd.query.joinTable.empty()) {
                     TableSchema schema2;
                     if (LoadSchema(cmd.query.joinTable, schema2, err)) {
                          displaySchema.fields.clear();
                          std::set<std::string> seen;
                          for(const auto& f : schema.fields) {
                              Field nf = f; nf.name = schema.tableName + "." + f.name;
                              displaySchema.fields.push_back(nf);
                              std::string base = f.name;
                              std::transform(base.begin(), base.end(), base.begin(),
                                             [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
                              seen.insert(base);
                          }
                          for(const auto& f : schema2.fields) {
                              if (cmd.query.isNaturalJoin) {
                                  std::string base = f.name;
                                  std::transform(base.begin(), base.end(), base.begin(),
                                                 [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
                                  if (seen.count(base)) continue;
                                  seen.insert(base);
                              }
                              Field nf = f; nf.name = schema2.tableName + "." + f.name;
                              displaySchema.fields.push_back(nf);
                          }
                     }
                }
            }
            
            std::string rowsJson = SerializeRows(displaySchema, out);
            
            std::string fieldsJson = "[";
            for (size_t i = 0; i < displaySchema.fields.size(); ++i) {
                if (i > 0) fieldsJson += ",";
                fieldsJson += "{\"name\":\"" + JsonEscape(displaySchema.fields[i].name) + "\",";
                fieldsJson += "\"type\":\"" + JsonEscape(displaySchema.fields[i].type) + "\"}";
            }
            fieldsJson += "]";

            lastStatus = 200;
            lastResultBody = "{\"ok\":true,\"fields\":" + fieldsJson + ",\"rows\":" + rowsJson + "}";
            continue;
        }
        
        if (cmd.type == CommandType::kDelete) {
            TableSchema schema;
            if (!LoadSchema(cmd.tableName, schema, err)) { resp.status=400; resp.body=Error(err); return; }
            if (schema.isView) { resp.status=400; resp.body=Error("Cannot delete from a view"); return; }

            bool implicit = false;
            if (!session.current_txn) {
                if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
                session.current_txn = txn_manager_.Begin(currentDbName_, err);
                if (!session.current_txn) { resp.status=500; resp.body=Error(err); return; }
                implicit = true;
            }

            if (!dml_.Delete(currentDat_, currentDbf_, schema, cmd.conditions, cmd.action, cmd.actionSpecified, err, session.current_txn, &log_, &lock_manager_)) {
                if (implicit) {
                    RollbackTxn(session, err);
                } else if (session.current_txn && IsLockTimeout(err)) {
                    std::string rbErr;
                    RollbackTxn(session, rbErr);
                }
                resp.status = 500; resp.body = Error(err); return;
            } else if (implicit) {
                if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err); return; }
            }

            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Delete successful\"}";
            continue;
        }
        
        if (cmd.type == CommandType::kUpdate) {
            TableSchema schema;
            if (!LoadSchema(cmd.tableName, schema, err)) { resp.status=400; resp.body=Error(err); return; }
            if (schema.isView) { resp.status=400; resp.body=Error("Cannot update a view"); return; }

            bool implicit = false;
            if (!session.current_txn) {
                if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
                session.current_txn = txn_manager_.Begin(currentDbName_, err);
                if (!session.current_txn) { resp.status=500; resp.body=Error(err); return; }
                implicit = true;
            }

              if (!dml_.Update(currentDat_, currentDbf_, schema, cmd.conditions, cmd.assignments, err, session.current_txn, &log_, &lock_manager_)) {
                  if (implicit) {
                      RollbackTxn(session, err);
                  } else if (session.current_txn && IsLockTimeout(err)) {
                      std::string rbErr;
                      RollbackTxn(session, rbErr);
                  }
                  resp.status = 500; resp.body = Error(err); return;
              } else if (implicit) {
                  if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err); return; }
              }

            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Update successful\"}";
            continue;
        }
        
        if (cmd.type == CommandType::kDrop) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            std::string dataPath = currentDat_;
            if (!ddl_.DropTable(currentDbf_, dataPath, cmd.tableName, cmd.actionSpecified ? cmd.action : ReferentialAction::kRestrict, err)) {
                 resp.status=400; resp.body=Error(err); return;
            } else {
                 lastStatus=200; lastResultBody="{\"ok\":true,\"message\":\"Table dropped\"}";
            }
            continue;
        }

        if (cmd.type == CommandType::kDropView) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            if (!ddl_.DropView(currentDbf_, currentDat_, cmd.viewName, cmd.ifExists, err)) {
                resp.status=400; resp.body=Error(err); return;
            } else {
                lastStatus=200; lastResultBody="{\"ok\":true,\"message\":\"View dropped\"}";
            }
            continue;
        }
        
        if (cmd.type == CommandType::kRename) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            if (!ddl_.RenameTable(currentDbf_, currentDat_, cmd.tableName, cmd.newName, err)) {
                resp.status=400; resp.body=Error(err); return;
            } else {
                lastStatus=200; lastResultBody="{\"ok\":true,\"message\":\"Table renamed\"}";
            }
            continue;
        }

        if (cmd.type == CommandType::kCreateIndex) {
             if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
             if (!ddl_.CreateIndex(currentDbf_, currentDat_, cmd.tableName, cmd.fieldName, cmd.indexName, cmd.isUnique, err)) {
                 resp.status = 400; resp.body = Error(err); return;
             } else {
                 lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Index created successfully\"}";
             }
             continue;
        }

        if (cmd.type == CommandType::kDropIndex) {
             if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
             if (!ddl_.DropIndex(currentDbf_, currentDat_, cmd.tableName, cmd.fieldName, err)) {
                 resp.status = 400; resp.body = Error(err); return;
             } else {
                 lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Index dropped successfully\"}";
             }
             continue;
        }

        if (cmd.type == CommandType::kAlter) {
            if (session.current_txn) { resp.status=400; resp.body=Error("DDL not allowed in active transaction"); return; }
            std::string dataPath = currentDat_;
            bool ok = false;
            
            switch(cmd.alterOp) {
                case AlterOperation::kAddColumn: {
                    Field f = cmd.columnDef;
                    std::string canonical; std::string typeErr;
                    int sz = InferSizeFromType(f.type, canonical, typeErr);
                    if (sz <= 0) { resp.status = 400; resp.body = Error(typeErr); return; }
                    f.type = canonical; f.size = sz;
                    f.valid = true; // Ensure valid
                    
                    ok = ddl_.AddColumn(currentDbf_, dataPath, cmd.tableName, f, cmd.extraInfo, err);
                    break;
                }
                case AlterOperation::kDropColumn:
                    ok = ddl_.DropColumn(currentDbf_, dataPath, cmd.tableName, cmd.fieldName, err);
                    break;
                case AlterOperation::kModifyColumn: {
                     Field f = cmd.columnDef;
                     std::string canonical; std::string typeErr;
                     int sz = InferSizeFromType(f.type, canonical, typeErr);
                     if (sz <= 0) { resp.status = 400; resp.body = Error(typeErr); return; }
                     f.type = canonical; f.size = sz;
                     ok = ddl_.ModifyColumn(currentDbf_, dataPath, cmd.tableName, f, err);
                     break;
                }
                case AlterOperation::kRenameColumn:
                    ok = ddl_.RenameColumn(currentDbf_, dataPath, cmd.tableName, cmd.fieldName, cmd.newName, err);
                    break;
                case AlterOperation::kRenameTable:
                    ok = ddl_.RenameTable(currentDbf_, dataPath, cmd.tableName, cmd.newName, err);
                    break;
                case AlterOperation::kAddIndex:
                     ok = ddl_.CreateIndex(currentDbf_, dataPath, cmd.tableName, cmd.fieldName, cmd.indexName, false, err);
                     break;
                case AlterOperation::kDropIndex:
                     ok = ddl_.DropIndex(currentDbf_, dataPath, cmd.tableName, cmd.indexName, err);
                     break;
                case AlterOperation::kAddConstraint:
                     ok = ddl_.AddForeignKey(currentDbf_, dataPath, cmd.tableName, cmd.fkDef, err);
                     break;
                case AlterOperation::kDropConstraint:
                     ok = ddl_.DropForeignKey(currentDbf_, dataPath, cmd.tableName, cmd.indexName, err);
                     break;
                default:
                    err = "Unsupported ALTER operation";
                    ok = false;
            }
            
            if (!ok) { resp.status = 400; resp.body = Error(err); return; }
            lastStatus = 200; lastResultBody = "{\"ok\":true,\"message\":\"Table altered successfully\"}";
            continue;
        }

        if (cmd.type == CommandType::kShowIndexes) {
            std::vector<TableSchema> schemas;
            if (!engine_.LoadSchemas(currentDbf_, schemas, err)) {
                 resp.status = 400; resp.body = Error(err); return;
            }
            
            auto it = std::find_if(schemas.begin(), schemas.end(), [&](const TableSchema& s){ return s.tableName == cmd.tableName; });
            if (it == schemas.end()) {
                resp.status = 400; resp.body = Error("Table not found"); return;
            }
            const TableSchema& schema = *it;

            // Use "rows" format to trigger frontend renderTable
            std::string json = "{\"ok\":true,\"rows\":[";
            
            int count = 0;
            for(const auto& idxDef : schema.indexes) {
                // Find field info to determine PK and Nullable
                auto fit = std::find_if(schema.fields.begin(), schema.fields.end(), [&](const Field& f){ return f.name == idxDef.fieldName; });
                bool isKey = false;
                bool nullable = true; 
                if (fit != schema.fields.end()) {
                    isKey = fit->isKey;
                    nullable = fit->nullable;
                }

                if (count > 0) json += ",";
                
                std::string nonUnique = idxDef.isUnique ? "0" : "1";
                // Naming convention: PRIMARY for keys, idx_table_field for others
                std::string keyName = idxDef.name;
                std::string seq = "1"; // Only single column index supported
                std::string colName = idxDef.fieldName;
                std::string nullVal = nullable ? "YES" : "";

                json += "{\"Table\":\"" + schema.tableName + "\",";
                json += "\"Non_unique\":" + nonUnique + ",";
                json += "\"Key_name\":\"" + keyName + "\",";
                json += "\"Seq_in_index\":" + seq + ",";
                json += "\"Column_name\":\"" + colName + "\",";
                json += "\"Null\":\"" + nullVal + "\"}";

                count++;
            }
            json += "]}";
            
            lastStatus = 200; 
            lastResultBody = json;
            continue;
        }

        if (cmd.type == CommandType::kShowTables) {
            std::string dbf = currentDbf_;
            if (!cmd.dbName.empty()) {
                dbf = dbms_paths::DbfPath(cmd.dbName);
            }
            std::vector<TableSchema> schemas;
            if (!engine_.LoadSchemas(dbf, schemas, err)) {
                resp.status = 400; resp.body = Error(err); return;
            }
            std::string json = "{\"ok\":true,\"fields\":[{\"name\":\"Tables\",\"type\":\"string\"}],\"rows\":[";
            for (size_t i = 0; i < schemas.size(); ++i) {
                if (i > 0) json += ",";
                json += "[\"" + JsonEscape(schemas[i].tableName) + "\"]";
            }
            json += "]}";
            lastStatus = 200;
            lastResultBody = json;
            continue;
        }

        resp.status = 400;
        resp.body = Error("Unknown or unsupported command");
        return;
    }

    resp.status = lastStatus;
    if (lastDbName.empty()) lastDbName = currentDbName_;
    if (lastResultBody.empty()) {
        resp.body = "{\"ok\":true,\"db\":\"" + JsonEscape(lastDbName) + "\"}";
    } else {
        if (lastResultBody.back() == '}') lastResultBody.pop_back();
        lastResultBody += ",\"db\":\"" + JsonEscape(lastDbName) + "\"}";
        resp.body = lastResultBody;
    }
}

void ApiServer::HandleCreateDatabase(const HttpRequest& req, HttpResponse& resp) {
    resp.contentType = "application/json";

    std::string err;
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty()) {
        resp.status = 400;
        resp.body = "{\"ok\":false,\"error\":\"Invalid JSON: " + JsonEscape(err) + "\"}";
        return;
    }

    if (!root.IsObject()) {
        resp.status = 400;
        resp.body = "{\"ok\":false,\"error\":\"Request must be JSON object\"}";
        return;
    }

    const JsonValue* dbVal = root.Get("database");
    if (!dbVal || !dbVal->IsString()) {
        resp.status = 400;
        resp.body = "{\"ok\":false,\"error\":\"Missing 'database' field\"}";
        return;
    }

    std::string dbName = dbVal->AsString("");
    if (dbName.empty()) {
        resp.status = 400;
        resp.body = "{\"ok\":false,\"error\":\"Database name cannot be empty\"}";
        return;
    }

    if (!engine_.CreateDatabase(dbName, err)) {
        resp.status = 400;
        resp.body = "{\"ok\":false,\"error\":\"" + JsonEscape(err) + "\"}";
        return;
    }

    resp.status = 200;
    resp.body = "{\"ok\":true,\"database\":\"" + JsonEscape(dbName) + "\"}";
}

void ApiServer::HandleUseDatabase(const HttpRequest& req, HttpResponse& resp) {
    std::string err;
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty() || !root.IsObject()) {
        resp.status = 400;
        resp.body = Error("Invalid JSON");
        return;
    }

    // ֧�����֣�
    // { "database": "data1" }
    // { "sql": "USE data1" }

    std::string db;

    if (root.Get("database")) {
        db = root.Get("database")->AsString("");
    }
    else if (root.Get("sql")) {
        std::string sql = root.Get("sql")->AsString("");
        std::string upper = ToUpper(sql);
        if (upper.find("USE ") == 0) {
            db = Trim(sql.substr(4));
        }
    }

    if (db.empty()) {
        resp.status = 400;
        resp.body = Error("Missing database name");
        return;
    }

    currentDbf_ = dbms_paths::DbfPath(db);
    currentDat_ = dbms_paths::DatPath(db);
    currentDbName_ = db; // ͬʱ�������ݿ�����

    resp.status = 200;
    resp.body = "{\"ok\":true,\"database\":\"" + JsonEscape(db) + "\"}";
}

void ApiServer::HandleListDatabases(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if (user.empty()) return;

  std::string err;
  if (!EnsureDefaultDb(err)) { resp.status = 500; resp.body = Error(err, 500); return; }

  resp.contentType = "application/json";

  std::vector<std::string> dbNames;
  try {
    const fs::path data_dir = dbms_paths::DataDirPath();
    if (fs::exists(data_dir)) {
      for (const auto& entry : fs::directory_iterator(data_dir)) {
        const auto p = entry.path();
        std::string name;
        if (entry.is_directory()) {
          const auto dbf = p / (p.filename().string() + ".dbf");
          if (!fs::exists(dbf)) continue;
          name = p.filename().string();
        } else if (entry.is_regular_file()) {
          if (p.extension() != ".dbf") continue;
          name = p.stem().string();
        } else {
          continue;
        }
        if (name == "system") continue;
        dbNames.push_back(name);
      }
    }
  } catch (const std::exception& e) {
    resp.status = 500;
    resp.body = Error(std::string("List databases failed: ") + e.what());
    return;
  }

  std::sort(dbNames.begin(), dbNames.end());

  std::ostringstream oss;
  oss << "{\"ok\":true,\"databases\":[";
  for (size_t i = 0; i < dbNames.size(); ++i) {
    if (i) oss << ",";
    oss << "\"" << JsonEscape(dbNames[i]) << "\"";
  }
  oss << "]}";

  resp.status = 200;
  resp.body = oss.str();
}

void ApiServer::HandleLoginPage(const HttpRequest& req, HttpResponse& resp) {
  (void)req;
  std::string html = ReadDocFile("login.html");
  if (html.empty()) {
    resp.status = 404;
    resp.contentType = "text/plain; charset=utf-8";
    resp.body = "Login page not found";
    return;
  }
  resp.status = 200;
  resp.contentType = "text/html; charset=utf-8";
  resp.body = html;
}

void ApiServer::HandleLogin(const HttpRequest& req, HttpResponse& resp) {
    std::string err;
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty()) { resp.status=400; resp.body=Error("Invalid JSON"); return; }
    
    std::string u = root.Get("user") ? root.Get("user")->AsString("") : "";
    std::string p = root.Get("pass") ? root.Get("pass")->AsString("") : "";
    std::string token;
    
    if (auth_.Login(u, p, token, err)) {
        resp.status = 200;
        resp.body = "{\"ok\":true, \"token\":\"" + token + "\"}";
    } else {
        resp.status = 401;
        resp.body = Error(err, 401);
    }
}

std::string ApiServer::CheckAuth(const HttpRequest& req, HttpResponse& resp) {
    std::string token;
    auto it = req.headers.find("authorization");
    if (it != req.headers.end()) {
        token = it->second;
    }
    std::string user = auth_.ValidateToken(token);
    if (user.empty()) {
        resp.status = 401; 
        resp.body = Error("Unauthorized", 401); 
        return "";
    }
    return user;
}

ApiServer::SessionContext& ApiServer::GetSession(const std::string& token) {
    auto it = sessions_.find(token);
    if (it != sessions_.end()) return it->second;
    SessionContext ctx;
    sessions_[token] = ctx;
    return sessions_[token];
}

bool ApiServer::CommitTxn(SessionContext& session, std::string& err) {
    if (!session.current_txn) { err = "No active transaction"; return false; }
    if (!txn_manager_.Commit(session.current_txn, err)) return false;
    lock_manager_.ReleaseAll(session.current_txn->id);
    for (const auto& table : session.current_txn->touched_tables) {
        ddl_.RebuildIndexes(currentDbf_, currentDat_, table, err);
    }
    delete session.current_txn;
    session.current_txn = nullptr;
    session.autocommit = true;
    return true;
}

bool ApiServer::RollbackTxn(SessionContext& session, std::string& err) {
    if (!session.current_txn) { err = "No active transaction"; return false; }
    if (!txn_manager_.Rollback(session.current_txn, err)) return false;
    lock_manager_.ReleaseAll(session.current_txn->id);
    for (const auto& table : session.current_txn->touched_tables) {
        ddl_.RebuildIndexes(currentDbf_, currentDat_, table, err);
    }
    delete session.current_txn;
    session.current_txn = nullptr;
    session.autocommit = true;
    return true;
}

void ApiServer::HandleQuery(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if(user.empty()) return;
  std::string token;
  auto tokIt = req.headers.find("authorization");
  if (tokIt != req.headers.end()) token = tokIt->second;
  SessionContext& session = GetSession(token);

  std::string err;
  JsonValue root = JsonValue::Parse(req.body, err);
  if (!err.empty() || !root.IsObject()) {
    resp.status = 400; resp.body = Error("Invalid JSON body"); return;
  }
  const JsonValue* schemaVal = root.Get("schema");
  const JsonValue* tableVal = root.Get("table");
  const JsonValue* filterVal = root.Get("filter");
  const JsonValue* limitVal = root.Get("limit");
  std::string schemaName = schemaVal ? schemaVal->AsString("default") : "default";
  std::string tableName = tableVal ? tableVal->AsString("") : "";
  if (tableName.empty()) { resp.status = 400; resp.body = Error("Missing table"); return; }

  // Check Permission
  if (!auth_.CheckPermission(user, tableName, "SELECT")) {
      resp.status = 403; resp.body = Error("Permission denied"); return;
  }

  int limit = static_cast<int>(limitVal ? limitVal->AsNumber(50) : 50);
  if (limit <= 0) limit = 50;
  if (limit > 200) limit = 200;
  std::string filter = filterVal ? filterVal->AsString("") : "";

  TableSchema schema;
  if (!LoadSchema(tableName, schema, err)) { resp.status = 400; resp.body = Error(err, 400); return; }

  QueryPlan plan;
  plan.conditions = ParseFilter(filter);
  plan.projection = {};

  std::vector<Record> out;
  std::string dataPath = DataPath(tableName);
  bool implicit = false;
  if (!session.current_txn) {
      if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
      session.current_txn = txn_manager_.Begin(currentDbName_, err);
      if (!session.current_txn) { resp.status=500; resp.body=Error(err, 500); return; }
      implicit = true;
  }
    if (!query_.Select(dataPath, currentDbf_, schema, plan, out, err, session.current_txn, &lock_manager_)) {
        if (implicit) {
            RollbackTxn(session, err);
        } else if (session.current_txn && IsLockTimeout(err)) {
            std::string rbErr;
            RollbackTxn(session, rbErr);
        }
        resp.status = 500; resp.body = Error(err, 500); return;
    }
  if (implicit) {
      if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err, 500); return; }
  }
  if (out.size() > static_cast<size_t>(limit)) out.resize(static_cast<size_t>(limit));

  std::string sql = BuildSql(schemaName, tableName, filter, limit);
  std::string rows = SerializeRows(schema, out);
  std::ostringstream oss;
  oss << "{\"ok\":true,\"rows\":" << rows << ",\"sql\":\"" << JsonEscape(sql) << "\"}";
  resp.status = 200;
  resp.body = oss.str();
}

void ApiServer::HandleInsert(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if (user.empty()) return;
  std::string token;
  auto tokIt = req.headers.find("authorization");
  if (tokIt != req.headers.end()) token = tokIt->second;
  SessionContext& session = GetSession(token);
  std::string err;
  JsonValue root = JsonValue::Parse(req.body, err);
  if (!err.empty() || !root.IsObject()) { resp.status = 400; resp.body = Error("Invalid JSON body"); return; }
  std::string tableName = root.Get("table") ? root.Get("table")->AsString("") : "";
  if (tableName.empty()) { resp.status = 400; resp.body = Error("Missing table"); return; }
  const JsonValue* rowVal = root.Get("row");
  if (!rowVal || !rowVal->IsObject()) { resp.status = 400; resp.body = Error("Missing row object"); return; }

  TableSchema schema;
  if (!LoadSchema(tableName, schema, err)) { resp.status = 400; resp.body = Error(err, 400); return; }

  std::map<std::string, JsonValue> rowObj = rowVal->AsObject();

  // build record aligned with schema
  Record rec;
  for (size_t i = 0; i < schema.fields.size(); ++i) {
    const auto& f = schema.fields[i];
    auto it = rowObj.find(f.name);
    if (it == rowObj.end()) {
      auto low = rowObj.find(ToLower(f.name));
      if (low != rowObj.end()) it = low;
    }
    if (it != rowObj.end()) {
      if (it->second.IsNumber()) {
        rec.values.push_back(std::to_string(static_cast<int>(it->second.AsNumber())));
      } else {
        rec.values.push_back(it->second.AsString(""));
      }
    } else {
      rec.values.push_back("");
    }
  }

  // auto id if first field is key and missing/empty
  if (!schema.fields.empty() && !schema.fields[0].name.empty() && rec.values.size() >= 1) {
    if (rec.values[0].empty()) {
      std::string e;
      std::string existingPath = DataPath(tableName);
      auto all = LoadAll(schema, existingPath, e);
      int maxId = 0;
      for (const auto& r : all) {
        if (r.values.empty()) continue;
        try { maxId = (std::max)(maxId, std::stoi(r.values[0])); } catch (...) {}
      }
      rec.values[0] = std::to_string(maxId + 1);
    }
  }

  std::string dataPath = DataPath(tableName);
  bool implicit = false;
  if (!session.current_txn) {
    if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
    session.current_txn = txn_manager_.Begin(currentDbName_, err);
    if (!session.current_txn) { resp.status=500; resp.body=Error(err, 500); return; }
    implicit = true;
  }
  if (!dml_.Insert(dataPath, currentDbf_, schema, {rec}, err, session.current_txn, &log_, &lock_manager_)) {
    if (implicit) {
      RollbackTxn(session, err);
    } else if (session.current_txn && IsLockTimeout(err)) {
      std::string rbErr;
      RollbackTxn(session, rbErr);
    }
    resp.status = 500; resp.body = Error(err, 500); return;
  }
  if (implicit) {
    if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err, 500); return; }
  }
  std::ostringstream oss;
  oss << "{\"ok\":true,\"id\":\"" << JsonEscape(rec.values.empty() ? "" : rec.values[0]) << "\"}";
  resp.status = 200;
  resp.body = oss.str();
}

void ApiServer::HandleUpdate(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if (user.empty()) return;
  std::string token;
  auto tokIt = req.headers.find("authorization");
  if (tokIt != req.headers.end()) token = tokIt->second;
  SessionContext& session = GetSession(token);
  std::string err;
  JsonValue root = JsonValue::Parse(req.body, err);
  if (!err.empty() || !root.IsObject()) { resp.status = 400; resp.body = Error("Invalid JSON body"); return; }
  std::string tableName = root.Get("table") ? root.Get("table")->AsString("") : "";
  std::string id = root.Get("id") ? root.Get("id")->AsString("") : "";
  const JsonValue* patchVal = root.Get("patch");
  if (tableName.empty() || id.empty() || !patchVal || !patchVal->IsObject()) {
    resp.status = 400; resp.body = Error("Missing table/id/patch"); return;
  }

  TableSchema schema;
  if (!LoadSchema(tableName, schema, err)) { resp.status = 400; resp.body = Error(err, 400); return; }

  std::vector<std::pair<std::string, std::string>> assigns;
  const auto& patchObj = patchVal->AsObject();
  for (const auto& kv : patchObj) {
    // normalize field name to schema casing
    std::string targetName;
    for (const auto& f : schema.fields) {
      if (ToLower(f.name) == ToLower(kv.first)) { targetName = f.name; break; }
    }
    if (targetName.empty()) continue;
    std::string val = kv.second.IsNumber() ? std::to_string(static_cast<int>(kv.second.AsNumber())) : kv.second.AsString("");
    assigns.push_back({targetName, val});
  }
  Condition cond;
  if (schema.fields.empty()) { resp.status = 400; resp.body = Error("Schema has no fields", 400); return; }
  cond.fieldName = schema.fields[0].name;  // key field (convention: first field)

  cond.op = "=";
  cond.value = id;
  std::string dataPath = DataPath(tableName);
  bool implicit = false;
  if (!session.current_txn) {
    if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
    session.current_txn = txn_manager_.Begin(currentDbName_, err);
    if (!session.current_txn) { resp.status=500; resp.body=Error(err, 500); return; }
    implicit = true;
  }
    if (!dml_.Update(dataPath, currentDbf_, schema, std::vector<Condition>{cond}, assigns, err, session.current_txn, &log_, &lock_manager_)) {
      if (implicit) {
        RollbackTxn(session, err);
      } else if (session.current_txn && IsLockTimeout(err)) {
        std::string rbErr;
        RollbackTxn(session, rbErr);
      }
      resp.status = 500; resp.body = Error(err, 500); return;
    }
  if (implicit) {
    if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err, 500); return; }
  }
  resp.status = 200; resp.body = "{\"ok\":true}";
}

void ApiServer::HandleDelete(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if (user.empty()) return;
  std::string token;
  auto tokIt = req.headers.find("authorization");
  if (tokIt != req.headers.end()) token = tokIt->second;
  SessionContext& session = GetSession(token);
  std::string err;
  JsonValue root = JsonValue::Parse(req.body, err);
  if (!err.empty() || !root.IsObject()) { resp.status = 400; resp.body = Error("Invalid JSON body"); return; }
  std::string tableName = root.Get("table") ? root.Get("table")->AsString("") : "";
  std::string id = root.Get("id") ? root.Get("id")->AsString("") : "";
  if (tableName.empty() || id.empty()) { resp.status = 400; resp.body = Error("Missing table/id"); return; }

  TableSchema schema;
  if (!LoadSchema(tableName, schema, err)) { resp.status = 400; resp.body = Error(err); return; }
  Condition cond;
  if (schema.fields.empty()) { resp.status = 400; resp.body = Error("Schema has no fields", 400); return; }
  cond.fieldName = schema.fields[0].name;  // key field (convention: first field)

  cond.op = "=";
  cond.value = id;
  std::string dataPath = DataPath(tableName);
  bool implicit = false;
  if (!session.current_txn) {
    if (!session.autocommit) { resp.status=400; resp.body=Error("No active transaction"); return; }
    session.current_txn = txn_manager_.Begin(currentDbName_, err);
    if (!session.current_txn) { resp.status=500; resp.body=Error(err, 500); return; }
    implicit = true;
  }
    if (!dml_.Delete(dataPath, currentDbf_, schema, std::vector<Condition>{cond}, ReferentialAction::kRestrict, false, err, session.current_txn, &log_, &lock_manager_)) {
      if (implicit) {
        RollbackTxn(session, err);
      } else if (session.current_txn && IsLockTimeout(err)) {
        std::string rbErr;
        RollbackTxn(session, rbErr);
      }
      resp.status = 500; resp.body = Error(err, 500); return;
    }
  if (implicit) {
    if (!CommitTxn(session, err)) { resp.status=500; resp.body=Error(err, 500); return; }
  }
  resp.status = 200; resp.body = "{\"ok\":true}";
}

void ApiServer::HandleCreateTable(const HttpRequest& req, HttpResponse& resp) {
    std::string err;
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty() || !root.IsObject()) { resp.status = 400; resp.body = Error("Invalid JSON body"); return; }

    // Preferred: { "sql": "CREATE TABLE t (id int primary key, name char[32] not null)" }
    std::string sql = root.Get("sql") ? root.Get("sql")->AsString("") : "";
    TableSchema schema;

    if (!sql.empty()) {
        Parser p;
        std::string perr;
        ParsedCommand cmd = p.Parse(sql, perr);
        if (!perr.empty()) {
            resp.status = 400;
            resp.body = Error(perr);
            return;
        }

        //// ===== ADD: USE DATABASE =====
        //if (cmd.type == CommandType::kUseDatabase) {
        //    HandleUseDatabase(req, resp);
        //    return;
        //}


        if (cmd.type != CommandType::kCreate) {
            resp.status = 400;
            resp.body = Error("Invalid CREATE TABLE SQL");
            return;
        }

        schema = cmd.schema;

    }
    else {
        // Optional: { "table":"t", "fields":[{"name":"id","type":"int","isKey":true,"nullable":false}, ...] }
        std::string tableName = root.Get("table") ? root.Get("table")->AsString("") : "";
        const JsonValue* fieldsVal = root.Get("fields");
        if (tableName.empty() || !fieldsVal || !fieldsVal->IsArray()) {
            resp.status = 400;
            resp.body = Error("Missing sql OR (table + fields[])");
            return;
        }
        schema.tableName = tableName;
        for (const auto& fv : fieldsVal->AsArray()) {
            if (!fv.IsObject()) continue;
            Field f;
            f.name = fv.Get("name") ? fv.Get("name")->AsString("") : "";
            f.type = fv.Get("type") ? fv.Get("type")->AsString("int") : "int";
            f.isKey = fv.Get("isKey") ? (fv.Get("isKey")->AsBool(false)) : false;
            f.nullable = fv.Get("nullable") ? (fv.Get("nullable")->AsBool(true)) : true;
            f.valid = true;
            f.size = 0;
            if (!f.name.empty()) schema.fields.push_back(f);
        }
    }

    // validate table name
    if (schema.tableName.empty()) { resp.status = 400; resp.body = Error("Missing table name"); return; }
    for (char c : schema.tableName) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
            resp.status = 400; resp.body = Error("Table name must be alnum or _"); return;
        }
    }
    if (schema.fields.empty()) { resp.status = 400; resp.body = Error("Empty field list"); return; }

    // validate fields + infer size
    std::set<std::string> seen;
    for (auto& f : schema.fields) {
        if (f.name.empty()) { resp.status = 400; resp.body = Error("Field name cannot be empty"); return; }
        for (char c : f.name) {
            if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
                resp.status = 400; resp.body = Error("Field name must be alnum or _"); return;
            }
        }
        std::string low = ToLower(f.name);
        if (seen.count(low)) { resp.status = 400; resp.body = Error("Duplicate field: " + f.name); return; }
        seen.insert(low);

        if (f.size <= 0) f.size = InferSizeFromType(f.type);
    }

    std::string dataPath = currentDat_;
    if (!ddl_.CreateTable(currentDbf_, dataPath, schema, err)) { resp.status = 400; resp.body = Error(err); return; }

    resp.status = 200;
    resp.body = "{\"ok\":true}";
}

void ApiServer::HandleListTables(const HttpRequest& req, HttpResponse& resp) {
  std::string user = CheckAuth(req, resp);
  if (user.empty()) return;

  std::string err;
  if (!EnsureDefaultDb(err)) { resp.status = 500; resp.body = Error(err, 500); return; }

  std::vector<TableSchema> schemas = ListSchemas();
  std::ostringstream oss;
  oss << "{\"ok\":true,\"tables\":[";
  for (size_t i = 0; i < schemas.size(); ++i) {
    if (i) oss << ',';
    oss << "\"" << JsonEscape(schemas[i].tableName) << "\"";
  }
  oss << "]}";
  resp.status = 200;
  resp.body = oss.str();
}

// ====== ADD: schema endpoints ======
static std::string SerializeSchemaObj(const TableSchema& s) {
    auto esc = [](const std::string& in) { return JsonEscape(in); };
    std::ostringstream oss;
    oss << "{"
        << "\"table\":\"" << esc(s.tableName) << "\","
        << "\"isView\":" << (s.isView ? "true" : "false") << ","
        << "\"fields\":[";
    for (size_t i = 0; i < s.fields.size(); ++i) {
        if (i) oss << ",";
        const auto& f = s.fields[i];
        oss << "{"
            << "\"name\":\"" << esc(f.name) << "\","
            << "\"type\":\"" << esc(f.type) << "\","
            << "\"size\":" << f.size << ","
            << "\"isKey\":" << (f.isKey ? "true" : "false") << ","
            << "\"nullable\":" << (f.nullable ? "true" : "false") << ","
            << "\"valid\":" << (f.valid ? "true" : "false")
            << "}";
    }
    oss << "]}";
    return oss.str();
}

void ApiServer::HandleSchemas(const HttpRequest& req, HttpResponse& resp) {
    std::string user = CheckAuth(req, resp);
    if (user.empty()) return;

    std::string err;
    if (!EnsureDefaultDb(err)) { resp.status = 500; resp.body = Error(err, 500); return; }

    auto schemas = ListSchemas();
    std::ostringstream oss;
    oss << "{\"ok\":true,\"schemas\":[";
    for (size_t i = 0; i < schemas.size(); ++i) {
        if (i) oss << ",";
        oss << SerializeSchemaObj(schemas[i]);
    }
    oss << "]}";
    resp.status = 200;
    resp.body = oss.str();
}

void ApiServer::HandleSchema(const HttpRequest& req, HttpResponse& resp) {
    std::string user = CheckAuth(req, resp);
    if (user.empty()) return;

    std::string err;
    if (!EnsureDefaultDb(err)) { resp.status = 500; resp.body = Error(err, 500); return; }
    JsonValue root = JsonValue::Parse(req.body, err);
    if (!err.empty() || !root.IsObject()) { resp.status = 400; resp.body = Error("Invalid JSON body"); return; }
    std::string table = root.Get("table") ? root.Get("table")->AsString("") : "";
    if (table.empty()) { resp.status = 400; resp.body = Error("Missing table"); return; }

    TableSchema s;
    if (!LoadSchema(table, s, err)) { resp.status = 404; resp.body = Error(err); return; }

    resp.status = 200;
    resp.body = std::string("{\"ok\":true,\"schema\":") + SerializeSchemaObj(s) + "}";
}

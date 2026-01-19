# DBMS

一个基于 C++17 的小型数据库系统，提供 HTTP API 与 Web 界面，支持基础 DDL/DML、SQL 解析与恢复。

## 功能
- 存储引擎：.dbf/.dat 文件与索引读写
- SQL 解析与执行：SELECT/INSERT/UPDATE/DELETE、JOIN、GROUP BY、聚合
- 事务：WAL 日志、锁管理、恢复扫描
- 内置 HTTP 服务器与 Web UI（workbench / SQL console）

## 构建
依赖：
- CMake >= 3.20
- C++17 编译器
- Threads（Windows 需要链接 ws2_32）

构建：
```bash
cmake -S . -B build
cmake --build build -j
```

## 运行
```bash
./build/dbms
```
Windows（MSVC）示例：
```bat
build\Debug\dbms.exe
```

默认入口：
- http://localhost:8080/（workbench）
- http://localhost:8080/sql（SQL console）
- http://localhost:8080/login.html（登录页）

默认账号：
- user: admin
- pass: admin

## 数据与文件
- 首次运行会创建默认数据库：MyDB.dbf / MyDB.dat
- 系统库与 WAL 默认在 data/ 目录，可用 DBMS_DATA_DIR 覆盖

## HTTP API（简要）
- /api/login
- /api/query, /api/insert, /api/update, /api/delete
- /api/create_table, /api/tables, /api/schemas
- /api/sql
- /api/create_database, /api/use_database, /api/databases

说明：受保护接口需在 Authorization 头中携带 /api/login 返回的 token。

## 项目结构
- src/：核心引擎与服务端代码
- docs/：静态 Web UI（构建后会复制到可执行文件同级）

# DB Workbench 系统使用手册 v2.0

欢迎使用 **DB Workbench**。本系统是一套轻量级的自研数据库管理系统，集成了高性能的 C++ 后端存储引擎与现代化的 Web 可视化管理界面。

本文档详细介绍了系统的功能特性、操作指南以及支持的 SQL 语法规范。

---

## 目录
1. [系统概览](#1-系统概览)
2. [环境启动](#2-环境启动)
3. [核心功能](#3-核心功能)
   - [可视化管理 (Dashboard)](#31-可视化管理-dashboard)
   - [SQL 控制台 (SQL Console)](#32-sql-控制台-sql-console)
4. [SQL 语法参考指南](#4-sql-语法参考指南)
   - [基础定义 (DDL)](#41-基础定义-ddl)
   - [数据操作 (DML)](#42-数据操作-dml)
   - [高级查询与连接 (Advanced Query)](#43-高级查询与连接-advanced-query)
5. [常见问题](#5-常见问题)

---

## 1. 系统概览

DB Workbench 采用前后端分离架构，旨在提供简洁高效的数据库学习与操作体验。
- **后端内核**: 自研 C++ 引擎，支持数据持久化 (.dbf/.dat)、SQL 解析、查询优化及 RESTful API。
- **前端界面**: 纯原生 HTML/JS/CSS 构建，无需额外依赖，提供响应式的操作体验。

---

## 2. 环境启动

1. **编译运行**:
   - 打开 `Project12.sln` 解决方案。
   - 确保使用 `Release` 或 `Debug` 模式（推荐 x64）。
   - 点击“开始执行(不调试)”，服务端将启动并监听 `8080` 端口。

2. **访问系统**:
   - 打开浏览器（推荐 Chrome/Edge）。
   - 访问地址: `http://localhost:8080/docs/workbench.html` (或根据本地服务显示的主页入口)。

---

## 3. 核心功能

### 3.1 可视化管理 (Dashboard)
主控台提供图形化的交互方式来管理数据库资源。
- **数据库导航**: 点击左侧列表可快速切换当前工作的数据库。
- **表级操作**:
  - **创建表**: 通过向导或 SQL 弹窗快速定义新表结构。
  - **数据浏览**: 以表格形式分页展示数据。
  - **CRUD**: 提供图形化的增加(Insert)、编辑(Edit)、删除(Delete)按钮，无需编写 SQL。
  - **查询过滤**: 顶部过滤器支持对单字段进行条件筛选（等于、包含、大于小于等）。

### 3.2 SQL 控制台 (SQL Console)
专为开发者设计的高级交互界面，支持脚本化操作。
- **功能特性**:
  - **多语句执行**: 支持一次性粘贴并执行包含分号 `;` 分隔的多条 SQL 语句（如批量建表、插入数据）。
  - **智能侧边栏**:
    - **数据库切换**: 下拉框实时切换上下文，自动同步 `USE` 命令状态。
    - **表结构预览**: 实时列出当前库的所有表，点击表名即可快速查询全表数据。
  - **增强结果页**:
    - 查询结果包含**元数据提示**，表头会显示字段类型（如 `id (INT)`, `name (CHAR)`)。
    - 若结果为空，也会显示表结构信息，便于确认字段定义。
  - **快捷键**: 支持 `Ctrl + Enter` 快速提交查询。

---

## 4. SQL 语法参考指南

系统引擎经过升级，现已支持较复杂的 ANSI SQL 标准语法。

### 4.1 基础定义 (DDL)

#### 数据库管理
```sql
CREATE DATABASE mydb;       -- 创建数据库
USE mydb;                   -- 切换当前数据库
DROP DATABASE mydb;         -- 删除数据库（慎用）
```

#### 表管理
```sql
-- 创建表
CREATE TABLE users (
    id int PRIMARY KEY,     -- 支持整型、主键约束
    username char(32),      -- 支持定长字符串
    balance double,         -- 支持浮点数
    status int NOT NULL     -- 支持非空约束
);

-- 删除表
DROP TABLE users;

-- 重命名表
RENAME TABLE old_name TO new_name;
```

### 4.2 数据操作 (DML)

#### 增删改
```sql
-- 插入数据
INSERT INTO users VALUES(1, 'Alice', 100.5, 1);

-- 更新数据
UPDATE users SET balance = 200.0, status = 2 WHERE id = 1;

-- 删除数据
DELETE FROM users WHERE id = 1;
```

### 4.3 高级查询与连接 (Advanced Query)

系统全新支持 **多表连接 (JOIN)** 和 **别名 (Alias)** 功能。

#### 基础查询
```sql
SELECT * FROM users;                        -- 全表查询
SELECT id, username FROM users WHERE id > 10; -- 条件投影
```

#### 别名支持 (Alias)
支持为表和列指定别名，简化查询语句。
```sql
-- 列别名
SELECT username AS 用户名, balance AS 余额 FROM users;

-- 表别名
SELECT u.username FROM users u;
```

#### 多表连接 (JOIN)
支持三种标准的连接方式，需配合 `ON` 子句使用。

1. **内连接 (INNER JOIN)**: 只返回两个表中匹配的行。
   ```sql
   SELECT p.name, c.category_name 
   FROM products p 
   INNER JOIN categories c ON p.category_id = c.id;
   ```

2. **左连接 (LEFT JOIN)**: 返回左表所有行，无匹配则右边补 NULL。
   ```sql
   SELECT p.name, c.category_name 
   FROM products p 
   LEFT JOIN categories c ON p.category_id = c.id;
   ```

3. **右连接 (RIGHT JOIN)**: 返回右表所有行，无匹配则左边补 NULL。
   ```sql
   SELECT p.name, c.category_name 
   FROM products p 
   RIGHT JOIN categories c ON p.category_id = c.id;
   ```

*注意：目前 JOIN 查询严格依赖 `ON` 条件，且建议使用别名以避免字段冲突。*

---

## 5. 常见问题

- **Q: 中文查询支持吗？**
  - A: 支持。系统底层已处理字符集问题，可以在 `WHERE` 条件或 `INSERT` 值中使用中文（如 `name = '张三'`），结果展示也能正确渲染中文。

- **Q: 为什么执行多条语句报错？**
  - A: 请确保每条语句之间使用英文分号 `;` 严格分隔。

- **Q: 为什么 LEFT JOIN 显示的不是 NULL 而是空字符串？**
  - A: 这里的 NULL 是数据库逻辑层面的不仅，在前端界面上通常会显示为可视化的 `NULL` 文本或空白，具体取决于字段类型。

---

*文档版本: 2.0 | 最后更新: 2026-01-14*

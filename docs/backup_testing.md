# 数据备份功能测试指南

本文档说明如何测试 `BACKUP DATABASE` 功能。

## 前置条件

1. 数据库服务已启动。
2. 您拥有管理员权限（用户名为 `admin`），或者已移除代码中的权限检查。
3. 确保目标备份路径具有写入权限。

## 测试步骤

### 1. 登录 SQL 控制台

打开浏览器访问 SQL 控制台（通常是 `http://localhost:8080/sql` 或 `docs/sql_console.html`）。

### 2. 准备数据

如果还没有数据库，先创建一个：
```sql
CREATE DATABASE TestBackupDB;
USE TestBackupDB;
CREATE TABLE t1 (id int, name char[32]);
INSERT INTO t1 VALUES (1, 'Alice');
```

### 3. 执行备份命令

在 SQL 控制台中执行以下命令：

```sql
-- 语法: BACKUP DATABASE <数据库名> TO '<目标文件夹路径>';
BACKUP DATABASE TestBackupDB TO 'backups/test_01';
```

**预期结果**：
控制台应返回成功消息：
`{"ok":true,"message":"Database TestBackupDB backed up to backups/test_01"}`

### 4. 验证备份文件

检查服务器运行目录下的 `backups/test_01` 文件夹。
该文件夹中应包含以下文件（取决于实际存在的数据）：
- `TestBackupDB.dbf` (表结构定义)
- `TestBackupDB.dat` (数据文件)
- `TestBackupDB.wal` (如果有未提交的事务日志)
- `TestBackupDB.*.idx` (如果有索引)

### 5. 恢复测试 (手动)

目前仅实现了 `BACKUP` 命令，恢复可以通过文件操作进行：

1. 停止数据库服务。
2. 将备份文件夹中的文件复制回数据库数据目录。
3. 重新启动服务。
4. 查询数据验证一致性。

## 常见问题

- **Permission denied**: 必须使用 `admin` 用户登录才能执行备份操作。
- **Filesystem error**: 确保备份路径合法且不是系统受保护目录。

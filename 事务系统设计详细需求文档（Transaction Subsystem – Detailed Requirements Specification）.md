# 事务系统设计详细需求文档（Transaction Subsystem – Detailed Requirements Specification）

## 0. 文档目标与适用范围

### 0.1 目标

为当前 DBMS 项目引入一个**中等规模、工程可落地、行为明确**的事务系统，实现：

- **DML 事务完整支持**
- **崩溃后数据一致**
- **并发安全（基础级别）**
- 与当前 `.dbf / .dat / .idx` 存储模型兼容
- 与现有 `ApiServer / Parser / DDL / DML / Query / StorageEngine` 架构无冲突

------

### 0.2 明确不在本阶段支持的内容（防止范围膨胀）

以下内容**明确不做或延后**：

- ❌ 分布式事务
- ❌ 两阶段提交（2PC）
- ❌ Snapshot Isolation / 完整 MVCC
- ❌ DDL 回滚（DDL 视为隐式提交）
- ❌ 高性能并发（目标是正确性优先）

------

## 1. 事务系统总体设计原则

### 1.1 事务模型

- **事务是“Session 级”概念**
- 一个用户连接（HTTP session / token）在同一时刻：
  - 要么处于 `AUTOCOMMIT`
  - 要么处于 `IN_TRANSACTION`

> ❗ **事务不能是“单条 SQL 级”**，否则 BEGIN/COMMIT 没有意义。

------

### 1.2 隔离级别策略（明确选择）

| 项目         | 决策                  |
| ------------ | --------------------- |
| 默认隔离级别 | **READ COMMITTED**    |
| 并发控制方式 | **基于锁（2PL）**     |
| 锁粒度       | **记录级（RID）为主** |
| SELECT 行为  | 语句级共享锁          |
| 写操作       | 事务级排他锁          |

> ❗ 本阶段 **不实现 MVCC**，避免对 StorageEngine 大规模侵入。

------

### 1.3 日志策略（WAL）

- **物理日志（before image / after image）**
- Write-Ahead Logging（先写日志，后写数据）
- Commit 时强制 `fsync`

------

## 2. 必须支持的 SQL 事务语句（TCL）

### 2.1 必须支持

```sql
BEGIN;
START TRANSACTION;

COMMIT;
ROLLBACK;

SAVEPOINT sp_name;
ROLLBACK TO SAVEPOINT sp_name;
RELEASE SAVEPOINT sp_name;
```

### 2.2 行为定义（无歧义）

| 语句        | 行为                     |
| ----------- | ------------------------ |
| BEGIN       | 若已有事务 → 报错        |
| COMMIT      | 提交当前事务并释放所有锁 |
| ROLLBACK    | 回滚整个事务             |
| SAVEPOINT   | 记录 undo log 位置       |
| ROLLBACK TO | 回滚到该 savepoint       |
| RELEASE     | 删除 savepoint（不回滚） |

------

### 2.3 AUTOCOMMIT 规则（非常关键）

- 默认：`AUTOCOMMIT = ON`
- 当 `AUTOCOMMIT = ON` 且无显式事务：
  - 每条 SQL 自动：
    - `BEGIN → 执行 → COMMIT`
- 当 `BEGIN` 执行后：
  - 自动关闭 autocommit
  - 直到 COMMIT / ROLLBACK

------

## 3. 模块级设计（与你当前工程强一致）

------

## 4. 新增模块与文件结构（强制）

在 `src/` 下新增：

```text
src/txn/
├── txn_types.h
├── txn_manager.h / .cpp
├── log_manager.h / .cpp
├── lock_manager.h / .cpp
├── recovery.h / .cpp
```

------

## 5. 核心数据结构定义（必须这样设计）

### 5.1 事务 ID（TxnId）

```cpp
using TxnId = uint64_t;
```

- 单调递增
- 启动 DB 时从 WAL 中恢复最大 TxnId

------

### 5.2 日志序号（LSN）

```cpp
using LSN = uint64_t;
```

- 每条 WAL 记录一个 LSN
- commit 需要 flush 到对应 LSN

------

### 5.3 记录标识（RID）

**必须稳定、可定位**

```cpp
struct RID {
    std::string table_name;
    uint64_t file_offset;   // 或 page+slot
};
```

> StorageEngine **必须保证 file_offset 在事务期间不变**

------

### 5.4 事务状态

```cpp
enum class TxnState {
    ACTIVE,
    COMMITTED,
    ABORTED
};
```

------

## 6. WAL 日志设计（最关键部分）

### 6.1 日志文件

- 文件名：`<db_name>.wal`
- 追加写（append-only）

------

### 6.2 日志类型（明确）

```cpp
enum class LogType {
    BEGIN,
    INSERT,
    UPDATE,
    DELETE,
    COMMIT,
    ABORT
};
```

------

### 6.3 日志记录格式（明确、不可随意变）

```cpp
struct LogRecord {
    LSN lsn;
    TxnId txn_id;
    LogType type;
    RID rid;                     // INSERT/UPDATE/DELETE
    std::vector<uint8_t> before; // UPDATE/DELETE
    std::vector<uint8_t> after;  // INSERT/UPDATE
};
```

> **原则**：
>
> - INSERT：只有 after
> - DELETE：只有 before
> - UPDATE：before + after

------

### 6.4 WAL 写入规则（严格）

1. **写数据前，必须先写 WAL**
2. COMMIT：
   - 写 COMMIT log
   - `fsync`
3. 回滚：
   - 写 ABORT log（可不强制 fsync）

------

## 7. Undo / Rollback 机制（精确行为）

### 7.1 Undo Log 来源

- **直接复用 WAL**

- 每个事务维护：

  ```cpp
  std::vector<LSN> undo_chain;
  ```

------

### 7.2 回滚算法（明确）

回滚事务 T：

```text
for log in undo_chain (reverse order):
    if UPDATE:
        write before image
    if INSERT:
        delete record
    if DELETE:
        re-insert before image
```

------

### 7.3 SAVEPOINT 实现方式

```cpp
struct Savepoint {
    std::string name;
    size_t undo_chain_size;
};
```

- rollback to savepoint：
  - 回滚 undo_chain 到该 size

------

## 8. 锁管理（Lock Manager）

### 8.1 锁类型

```cpp
enum class LockMode {
    SHARED,
    EXCLUSIVE
};
```

------

### 8.2 锁粒度

- **RID 级锁（记录锁）**
- 表锁可选（DDL 使用）

------

### 8.3 锁规则（READ COMMITTED）

| 操作   | 锁   | 生命周期 |
| ------ | ---- | -------- |
| SELECT | S    | 语句结束 |
| INSERT | X    | 事务结束 |
| UPDATE | X    | 事务结束 |
| DELETE | X    | 事务结束 |

------

### 8.4 死锁策略（明确）

- **第一阶段**：无死锁检测
- 获取不到锁 → 阻塞 + 超时
- 超时 → 回滚当前事务

------

## 9. StorageEngine 必须满足的事务前提

### 9.1 必须支持

- 根据 RID 定位记录
- 读出完整 record byte buffer
- 覆盖写 record
- 删除 / 插入 record（offset 稳定）

------

### 9.2 严格禁止的行为（事务期）

❌ **在事务未提交前：**

- 重建整张表索引
- 修改 schema
- 重排 record offset

------

## 10. 各现有模块的改造要求（逐个明确）

------

### 10.1 Parser

- 新增 ParsedCommand：

  ```cpp
  enum class CommandType {
      TCL, DDL, DML, QUERY
  };
  ```

- TCL 子类型：

  - BEGIN / COMMIT / ROLLBACK / SAVEPOINT / …

------

### 10.2 ApiServer（关键）

必须引入：

```cpp
struct SessionContext {
    Txn* current_txn;
    bool autocommit;
    IsolationLevel isolation;
};
```

- SessionContext 与登录 token 绑定
- 同一 token 的 SQL 共享事务状态

------

### 10.3 DML / Query / DDL

#### 接口统一改为：

```cpp
Execute(cmd, SessionContext&, TxnManager&)
```

- DML / Query：
  - 必须接受 Txn*
  - 必须通过 LockManager
  - 必须写 WAL
- DDL：
  - 自动 COMMIT
  - 若存在活跃事务 → 报错

------

## 11. 启动与崩溃恢复流程（明确）

### 11.1 启动流程（main.cpp）

```text
StorageEngine init
LogManager init
Recovery.run()
TxnManager init
ApiServer start
```

------

### 11.2 Recovery 算法（简化 ARIES）

1. 扫描 WAL
2. 记录：
   - committed txns
   - active txns
3. Redo：
   - 对 committed txn 重放 after image
4. Undo：
   - 对 active txn 应用 before image

------

## 12. 事务执行完整时序（UPDATE 示例）

```text
BEGIN
→ Lock X on RID
→ read before image
→ WAL: UPDATE(before, after)
→ write record
→ COMMIT
→ WAL: COMMIT + fsync
→ release locks
```

------

## 13. 可验证的验收标准

- BEGIN + UPDATE + ROLLBACK → 数据不变
- BEGIN + UPDATE + COMMIT → 数据持久
- 崩溃后：
  - 已 COMMIT 的数据存在
  - 未 COMMIT 的数据不存在
- 并发 UPDATE 同一行 → 串行执行

------

## 14. 实现顺序建议（严格）

1. **TxnManager + Undo（无并发）**
2. WAL + 崩溃恢复
3. LockManager
4. SAVEPOINT
5. 索引事务化（最后）
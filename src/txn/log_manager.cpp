#include "log_manager.h"
#include <cstdio>
#if defined(_WIN32)
#include <io.h>
#endif

namespace {
bool WriteUInt32(FILE* f, uint32_t v) { return std::fwrite(&v, sizeof(uint32_t), 1, f) == 1; }
bool WriteUInt64(FILE* f, uint64_t v) { return std::fwrite(&v, sizeof(uint64_t), 1, f) == 1; }
bool ReadUInt32(FILE* f, uint32_t& v) { return std::fread(&v, sizeof(uint32_t), 1, f) == 1; }
bool ReadUInt64(FILE* f, uint64_t& v) { return std::fread(&v, sizeof(uint64_t), 1, f) == 1; }

bool WriteString(FILE* f, const std::string& s) {
  if (!WriteUInt32(f, static_cast<uint32_t>(s.size()))) return false;
  return s.empty() || std::fwrite(s.data(), 1, s.size(), f) == s.size();
}

bool ReadString(FILE* f, std::string& s) {
  uint32_t len = 0;
  if (!ReadUInt32(f, len)) return false;
  s.assign(len, '\0');
  return len == 0 || std::fread(&s[0], 1, len, f) == len;
}
}

LogManager::LogManager(const std::string& db_name) {
  SetDbName(db_name);
}

void LogManager::SetDbName(const std::string& db_name) {
  if (db_name.empty()) {
    wal_path_.clear();
    return;
  }
  wal_path_ = db_name + ".wal";
}

LSN LogManager::Append(LogRecord& rec, std::string& err) {
  if (wal_path_.empty()) {
    err = "WAL path not set";
    return 0;
  }

  FILE* f = nullptr;
  if (fopen_s(&f, wal_path_.c_str(), "ab") != 0 || !f) {
    err = "Cannot open WAL file for append: " + wal_path_;
    return 0;
  }

  rec.lsn = next_lsn_++;
  uint32_t type = static_cast<uint32_t>(rec.type);

  bool ok = true;
  ok = ok && WriteUInt64(f, rec.lsn);
  ok = ok && WriteUInt64(f, rec.txn_id);
  ok = ok && WriteUInt32(f, type);
  ok = ok && WriteString(f, rec.rid.table_name);
  ok = ok && WriteUInt64(f, rec.rid.file_offset);
  ok = ok && WriteUInt32(f, static_cast<uint32_t>(rec.before.size()));
  if (ok && !rec.before.empty()) ok = std::fwrite(rec.before.data(), 1, rec.before.size(), f) == rec.before.size();
  ok = ok && WriteUInt32(f, static_cast<uint32_t>(rec.after.size()));
  if (ok && !rec.after.empty()) ok = std::fwrite(rec.after.data(), 1, rec.after.size(), f) == rec.after.size();

  std::fclose(f);

  if (!ok) {
    err = "Failed to write WAL record";
    return 0;
  }
  cache_[rec.lsn] = rec;
  return rec.lsn;
}

bool LogManager::Flush(LSN, std::string& err) {
  if (wal_path_.empty()) {
    err = "WAL path not set";
    return false;
  }

  FILE* f = nullptr;
  if (fopen_s(&f, wal_path_.c_str(), "ab") != 0 || !f) {
    err = "Cannot open WAL file for flush: " + wal_path_;
    return false;
  }
  std::fflush(f);
#if defined(_WIN32)
  _commit(_fileno(f));
#endif
  std::fclose(f);
  return true;
}

bool LogManager::GetRecord(LSN lsn, LogRecord& out) const {
  auto it = cache_.find(lsn);
  if (it == cache_.end()) return false;
  out = it->second;
  return true;
}

bool LogManager::ReadAll(std::vector<LogRecord>& out, std::string& err) const {
  out.clear();
  if (wal_path_.empty()) return true;

  FILE* f = nullptr;
  if (fopen_s(&f, wal_path_.c_str(), "rb") != 0 || !f) return true; // no wal yet

  while (true) {
    LogRecord rec;
    uint64_t lsn = 0, txn = 0, off = 0;
    uint32_t type = 0, beforeSz = 0, afterSz = 0;
    std::string table;

    if (!ReadUInt64(f, lsn)) break;
    if (!ReadUInt64(f, txn)) { err = "WAL read txn_id failed"; break; }
    if (!ReadUInt32(f, type)) { err = "WAL read type failed"; break; }
    if (!ReadString(f, table)) { err = "WAL read table_name failed"; break; }
    if (!ReadUInt64(f, off)) { err = "WAL read offset failed"; break; }
    if (!ReadUInt32(f, beforeSz)) { err = "WAL read before size failed"; break; }
    rec.before.resize(beforeSz);
    if (beforeSz > 0 && std::fread(rec.before.data(), 1, beforeSz, f) != beforeSz) { err = "WAL read before failed"; break; }
    if (!ReadUInt32(f, afterSz)) { err = "WAL read after size failed"; break; }
    rec.after.resize(afterSz);
    if (afterSz > 0 && std::fread(rec.after.data(), 1, afterSz, f) != afterSz) { err = "WAL read after failed"; break; }

    rec.lsn = lsn;
    rec.txn_id = txn;
    rec.type = static_cast<LogType>(type);
    rec.rid.table_name = table;
    rec.rid.file_offset = off;
    out.push_back(rec);
  }

  std::fclose(f);
  if (!err.empty()) return false;
  return true;
}

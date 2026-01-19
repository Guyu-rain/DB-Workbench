#include "storage_engine.h"
#include <fstream>
#include <vector>

namespace {
constexpr char kTableSep = '~';

void AppendUInt32(std::vector<uint8_t>& out, uint32_t v) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&v);
  out.insert(out.end(), p, p + sizeof(uint32_t));
}

void AppendString(std::vector<uint8_t>& out, const std::string& s) {
  AppendUInt32(out, static_cast<uint32_t>(s.size()));
  out.insert(out.end(), s.begin(), s.end());
}
}

bool StorageEngine::SerializeRecord(const TableSchema& schema, const Record& record, std::vector<uint8_t>& outBytes, std::string& err) const {
  if (record.values.size() != schema.fields.size()) {
    err = "Record field count mismatch";
    return false;
  }
  outBytes.clear();
  outBytes.push_back(record.valid ? 1 : 0);
  for (const auto& v : record.values) AppendString(outBytes, v);
  return true;
}

bool StorageEngine::ReadRecordBytesAt(const std::string& datPath, const TableSchema& schema, long offset, std::vector<uint8_t>& outBytes, std::string& err) {
  std::ifstream ifs(datPath, std::ios::binary);
  if (!ifs.is_open()) { err = "Cannot open dat file: " + datPath; return false; }
  ifs.seekg(offset);
  if (!ifs) { err = "Seek failed"; return false; }

  outBytes.clear();
  char valid = 0;
  ifs.read(&valid, 1);
  if (!ifs) { err = "Read valid flag failed"; return false; }
  outBytes.push_back(static_cast<uint8_t>(valid));

  for (const auto& f : schema.fields) {
    (void)f;
    uint32_t len = 0;
    ifs.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
    if (!ifs) { err = "Read length failed"; return false; }
    AppendUInt32(outBytes, len);
    if (len > 0) {
      std::vector<char> buf(len);
      ifs.read(buf.data(), len);
      if (!ifs) { err = "Read field bytes failed"; return false; }
      outBytes.insert(outBytes.end(), buf.begin(), buf.end());
    }
  }
  return true;
}

bool StorageEngine::WriteRecordBytesAt(const std::string& datPath, long offset, const std::vector<uint8_t>& bytes, std::string& err) {
  std::fstream fs(datPath, std::ios::binary | std::ios::in | std::ios::out);
  if (!fs.is_open()) { err = "Cannot open dat file for write: " + datPath; return false; }
  fs.seekp(offset);
  if (!fs) { err = "Seek failed"; return false; }
  if (!bytes.empty()) fs.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
  return static_cast<bool>(fs);
}

bool StorageEngine::ComputeAppendRecordOffset(const std::string& datPath, const TableSchema& schema, long& outOffset, std::string& err) {
  std::ifstream ifs(datPath, std::ios::binary | std::ios::ate);
  std::streamsize sz = 0;
  if (ifs.is_open()) {
    sz = ifs.tellg();
    ifs.close();
  }

  std::vector<uint8_t> header;
  header.push_back(kTableSep);
  AppendString(header, schema.tableName);
  AppendUInt32(header, 1);
  AppendUInt32(header, static_cast<uint32_t>(schema.fields.size()));
  outOffset = static_cast<long>(sz + header.size());
  return true;
}

bool StorageEngine::WriteInsertBlockAt(const std::string& datPath, const TableSchema& schema, long recordOffset, const std::vector<uint8_t>& recordBytes, std::string& err) {
  std::vector<uint8_t> header;
  header.push_back(kTableSep);
  AppendString(header, schema.tableName);
  AppendUInt32(header, 1);
  AppendUInt32(header, static_cast<uint32_t>(schema.fields.size()));
  long headerOffset = recordOffset - static_cast<long>(header.size());
  if (headerOffset < 0) { err = "Invalid record offset for insert"; return false; }

  std::fstream fs(datPath, std::ios::binary | std::ios::in | std::ios::out);
  if (!fs.is_open()) {
    fs.open(datPath, std::ios::binary | std::ios::out);
    fs.close();
    fs.open(datPath, std::ios::binary | std::ios::in | std::ios::out);
  }
  if (!fs.is_open()) { err = "Cannot open dat file for insert"; return false; }

  fs.seekp(0, std::ios::end);
  std::streamoff endPos = fs.tellp();
  if (endPos < headerOffset) {
    fs.seekp(0, std::ios::end);
    std::vector<char> pad(static_cast<size_t>(headerOffset - endPos), 0);
    if (!pad.empty()) fs.write(pad.data(), static_cast<std::streamsize>(pad.size()));
  }

  fs.seekp(headerOffset);
  fs.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
  fs.seekp(recordOffset);
  if (!recordBytes.empty()) fs.write(reinterpret_cast<const char*>(recordBytes.data()), static_cast<std::streamsize>(recordBytes.size()));
  return static_cast<bool>(fs);
}

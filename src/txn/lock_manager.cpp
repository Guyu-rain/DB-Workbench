#include "lock_manager.h"
#include <chrono>
#include <thread>

namespace {
constexpr int kTimeoutMs = 2000;
}

std::string LockManager::Key(const RID& rid) const {
  return rid.table_name + "#" + std::to_string(rid.file_offset);
}

bool LockManager::TryLockShared(TxnId txn_id, const std::string& key) {
  LockState& st = locks_[key];
  if (st.exclusive_owner != 0 && st.exclusive_owner != txn_id) return false;
  st.shared_owners.insert(txn_id);
  owned_[txn_id].insert(key);
  return true;
}

bool LockManager::TryLockExclusive(TxnId txn_id, const std::string& key) {
  LockState& st = locks_[key];
  if (st.exclusive_owner == txn_id) {
    owned_[txn_id].insert(key);
    return true;
  }
  if (st.exclusive_owner != 0 && st.exclusive_owner != txn_id) return false;
  if (!st.shared_owners.empty()) {
    if (st.shared_owners.size() == 1 && st.shared_owners.count(txn_id)) {
      st.shared_owners.clear();
    } else {
      return false;
    }
  }
  st.exclusive_owner = txn_id;
  owned_[txn_id].insert(key);
  return true;
}

bool LockManager::LockShared(TxnId txn_id, const RID& rid, std::string& err) {
  std::string key = Key(rid);
  std::unique_lock<std::mutex> lock(mu_);
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kTimeoutMs);
  while (true) {
    if (TryLockShared(txn_id, key)) return true;
    if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      err = "Lock timeout (shared)";
      return false;
    }
  }
}

bool LockManager::LockExclusive(TxnId txn_id, const RID& rid, std::string& err) {
  std::string key = Key(rid);
  std::unique_lock<std::mutex> lock(mu_);
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kTimeoutMs);
  while (true) {
    if (TryLockExclusive(txn_id, key)) return true;
    if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
      err = "Lock timeout (exclusive)";
      return false;
    }
  }
}

void LockManager::ReleaseShared(TxnId txn_id, const RID& rid) {
  std::string key = Key(rid);
  std::lock_guard<std::mutex> lock(mu_);
  auto lit = locks_.find(key);
  if (lit == locks_.end()) return;
  LockState& st = lit->second;
  st.shared_owners.erase(txn_id);

  auto oit = owned_.find(txn_id);
  if (oit != owned_.end()) {
    if (st.exclusive_owner != txn_id && st.shared_owners.count(txn_id) == 0) {
      oit->second.erase(key);
      if (oit->second.empty()) owned_.erase(oit);
    }
  }
  cv_.notify_all();
}

void LockManager::ReleaseAll(TxnId txn_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = owned_.find(txn_id);
  if (it == owned_.end()) return;
  for (const auto& key : it->second) {
    auto lit = locks_.find(key);
    if (lit == locks_.end()) continue;
    LockState& st = lit->second;
    if (st.exclusive_owner == txn_id) st.exclusive_owner = 0;
    st.shared_owners.erase(txn_id);
  }
  owned_.erase(it);
  cv_.notify_all();
}

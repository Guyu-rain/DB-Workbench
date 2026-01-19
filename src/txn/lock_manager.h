#pragma once
#include <string>
#include <map>
#include <set>
#include <mutex>
#include <condition_variable>
#include "txn_types.h"

class LockManager {
 public:
  bool LockShared(TxnId txn_id, const RID& rid, std::string& err);
  bool LockExclusive(TxnId txn_id, const RID& rid, std::string& err);
  void ReleaseShared(TxnId txn_id, const RID& rid);
  void ReleaseAll(TxnId txn_id);

 private:
  struct LockState {
    TxnId exclusive_owner = 0;
    std::set<TxnId> shared_owners;
  };

  std::string Key(const RID& rid) const;
  bool TryLockShared(TxnId txn_id, const std::string& key);
  bool TryLockExclusive(TxnId txn_id, const std::string& key);

  std::map<std::string, LockState> locks_;
  std::map<TxnId, std::set<std::string>> owned_;
  std::mutex mu_;
  std::condition_variable cv_;
};

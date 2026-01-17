#pragma once
#include <string>
#include "txn_types.h"

class StorageEngine;
class LogManager;

class Recovery {
 public:
  static TxnId Run(StorageEngine& engine, const std::string& db_name, std::string& err, LSN* max_lsn_out = nullptr);
};

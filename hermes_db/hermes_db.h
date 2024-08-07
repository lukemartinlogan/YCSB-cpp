//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_HERMES_H_
#define YCSB_C_HERMES_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include "hermes/hermes.h"

namespace ycsbc {

class HermesDB : public DB {
 public:
  hermes::Bucket bucket;
 public:
  HermesDB() {}
  ~HermesDB() {}

  void Init();
  void Cleanup();
  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result);
  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);
  Status Delete(const std::string &table, const std::string &key);
};

DB *NewHermesDB();

} // ycsbc

#endif // YCSB_C_HERMES_H_


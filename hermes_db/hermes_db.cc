//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "hermes_db.h"
#include "hermes/bucket.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

namespace ycsbc {

typedef DB::Status Status;

void HermesDB::Init() {
  TRANSPARENT_HERMES();
}

void HermesDB::Cleanup() {
  HERMES->Clear();
}

Status HermesDB::Read(const std::string &table, const std::string &key,
            const std::vector<std::string> *fields, std::vector<Field> &result) {

  return Status::kOK;
}

Status HermesDB::Scan(const std::string &table, const std::string &key, int len,
            const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  return Status::kOK;
}

Status HermesDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  return Status::kOK;
}

Status HermesDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  return Status::kOK;
}

Status HermesDB::Delete(const std::string &table, const std::string &key) {
  return Status::kOK;
}

DB *NewHermesDB() {
  return new HermesDB;
}

const bool registered = DBFactory::RegisterDB("hermes", NewHermesDB);

} // ycsbc

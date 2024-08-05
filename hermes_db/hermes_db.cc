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
  hermes::Bucket bucket = HERMES->GetBucket(table);
  hermes::Context ctx;
//  if (bucket.GetBlobId(key).IsNull()) {
//    return Status::kNotFound;
//  }
  bucket.Get(key, result, ctx);
  return Status::kOK;
}

Status HermesDB::Scan(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  // Create a set
  std::unordered_set<std::string> field_set;
  if (fields != nullptr) {
    for (const auto &field : *fields) {
      field_set.insert(field);
    }
  }
  // Read entire blob
  hermes::Bucket bucket = HERMES->GetBucket(table);
  hermes::Context ctx;
  bucket.Get(key, result, ctx);
  // Read field from keys
  for (int i = 0; i < len; ++i) {
    result.emplace_back();
    bucket.Get(key + std::to_string(i), result, ctx);
    if (fields != nullptr) {
      for (auto it = result.back().begin(); it != result.back().end();) {
        if (field_set.find(it->name) == field_set.end()) {
          it = result.back().erase(it);
        } else {
          ++it;
        }
      }
    }
  }
  return Status::kOK;
}

Status HermesDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  // Get whole blob
  hermes::Bucket bucket = HERMES->GetBucket(table);
  hermes::Context ctx;
  std::vector<Field> result;
  bucket.Get(key, result, ctx);
  // Update
  for (auto &field : values) {
    for (auto &record : result) {
      if (record.name == field.name) {
        record.value = field.value;
        break;
      }
    }
  }
  // Put back
  bucket.Put(key, result, ctx);
  return Status::kOK;
}

Status HermesDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  hermes::Bucket bucket = HERMES->GetBucket(table);
  hermes::Context ctx;
  bucket.Put(key, values, ctx);
  return Status::kOK;
}

Status HermesDB::Delete(const std::string &table, const std::string &key) {
  hermes::Bucket bucket = HERMES->GetBucket(table);
  hermes::Context ctx;
  hermes::BlobId blob_id = bucket.GetBlobId(key);
  bucket.DestroyBlob(blob_id, ctx);
  return Status::kOK;
}

DB *NewHermesDB() {
  return new HermesDB;
}

const bool registered = DBFactory::RegisterDB("hermes", NewHermesDB);

} // ycsbc

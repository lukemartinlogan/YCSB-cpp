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
  bucket = HERMES->GetBucket("ycsb");
}

void HermesDB::Cleanup() {
  HERMES->Clear();
}

size_t GetSrlSize(std::vector<HermesDB::Field> &values) {
  size_t srl_size = 0;
  for (HermesDB::Field &field : values) {
    srl_size += field.name.size() + field.value.size();
  }
  return srl_size;
}

//void SerializeBlob(hipc::LPointer<char> &blob,
//                   size_t blob_size,
//                   std::vector<HermesDB::Field> &values) {
//  size_t off = 0;
//  hipc::LPointer<char> blob = HRUN_CLIENT->AllocateBufferClient(srl_size);
//  for (HermesDB::Field &field : values) {
//    memcpy(blob.ptr_ + off, field.name.c_str(), field.name.size());
//    off += field.name.size();
//    memcpy(blob.ptr_ + off, field.value.c_str(), field.value.size());
//    off += field.value.size();
//  }
//}
//
//void DeserializeBlob(hipc::LPointer<char> &blob,
//                     size_t blob_size,
//                     std::vector<HermesDB::Field> &values) {
//
//}

Status HermesDB::Read(const std::string &table, const std::string &key,
                      const std::vector<std::string> *fields, std::vector<Field> &result) {
  hermes::Context ctx;
//  if (bucket.GetBlobId(key).IsNull()) {
//    return Status::kNotFound;
//  }
  bucket.Get(key, result, ctx);
  return Status::kOK;
}

Status HermesDB::Scan(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  return Status::kNotImplemented;
}

Status HermesDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  // Get whole blob
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
  // Put
  bucket.AsyncPut(key, values, ctx);
  return Status::kOK;
}

Status HermesDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  hermes::Context ctx;
  bucket.AsyncPut(key, values, ctx);
  return Status::kOK;
}

Status HermesDB::Delete(const std::string &table, const std::string &key) {
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

//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "redis_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

namespace ycsbc {

typedef DB::Status Status;

void RedisDB::Init() {
  // Connect to Redis server
  context_ = redisConnect("127.0.0.1", 6379);
  if (context_ == nullptr || context_->err) {
    if (context_) {
      std::cerr << "Connection error: " << context_->errstr << std::endl;
      redisFree(context_);
    } else {
      std::cerr << "Connection error: can't allocate redis context_" << std::endl;
    }
    throw std::runtime_error("Failed to connect to redis");
  }
}

void RedisDB::Cleanup() {
  redisFree(context_);
}

Status RedisDB::Read(const std::string &table, const std::string &key,
            const std::vector<std::string> *fields, std::vector<Field> &result) {

  std::string fullKey = table + ":" + key;

  if (fields == nullptr) {
    // Fetch all fields if no specific fields are provided
    redisReply* reply = (redisReply*)redisCommand(
        context_, "HGETALL %b", fullKey.c_str(), (size_t)fullKey.length());
    if (!reply || context_->err) {
      if (reply) freeReplyObject(reply);
      return Status::kError;
    }

    if (reply->type == REDIS_REPLY_NIL) {
      freeReplyObject(reply);
      return Status::kNotFound;
    }

    result.clear();
    for (size_t i = 0; i < reply->elements; i += 2) {
      Field field;
      field.name = reply->element[i]->str;
      field.value = reply->element[i+1]->str;
      result.push_back(field);
    }
    freeReplyObject(reply);
  } else {
    // Fetch specific fields
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.push_back("HMGET");
    argvlen.push_back(5);
    argv.push_back(fullKey.c_str());
    argvlen.push_back(fullKey.length());

    for (const std::string& field : *fields) {
      argv.push_back(field.c_str());
      argvlen.push_back(field.length());
    }

    redisReply* reply = (redisReply*)redisCommandArgv(context_, argv.size(), argv.data(), argvlen.data());
    if (!reply || context_->err) {
      if (reply) freeReplyObject(reply);
      return Status::kError;
    }

    if (reply->type == REDIS_REPLY_NIL) {
      freeReplyObject(reply);
      return Status::kNotFound;
    }

    result.clear();
    for (size_t i = 0; i < fields->size(); ++i) {
      Field field;
      field.name = (*fields)[i];
      field.value = reply->element[i]->str ? reply->element[i]->str : "";
      result.push_back(field);
    }
    freeReplyObject(reply);
  }
  return Status::kOK;
}

Status RedisDB::Scan(const std::string &table, const std::string &key, int len,
            const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
  uint64_t key_id = std::stoull(key);
  for (int i = 0; i < len; ++i) {
    std::string fullKey = table + ":" + std::to_string(key_id + i);
    if (fields == nullptr) {
      // Fetch all fields if no specific fields are provided
      redisReply *reply = (redisReply *) redisCommand(
          context_, "HGETALL %b", fullKey.c_str(), (size_t) fullKey.length());
      if (!reply || context_->err) {
        if (reply) freeReplyObject(reply);
        return Status::kError;
      }

      if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return Status::kNotFound;
      }

      std::vector<Field> record;
      for (size_t i = 0; i < reply->elements; i += 2) {
        Field field;
        field.name = reply->element[i]->str;
        field.value = reply->element[i + 1]->str;
        record.push_back(field);
      }
      result.push_back(record);
      freeReplyObject(reply);
    } else {
      // Fetch specific fields
      std::vector<const char *> argv;
      std::vector<size_t> argvlen;
      argv.push_back("HMGET");
      argvlen.push_back(5);
      argv.push_back(fullKey.c_str());
      argvlen.push_back(fullKey.length());

      for (const std::string &field : *fields) {
        argv.push_back(field.c_str());
        argvlen.push_back(field.length());
      }

      redisReply *reply = (redisReply *) redisCommandArgv(
          context_, argv.size(), argv.data(), argvlen.data());
      if (!reply || context_->err) {
        if (reply) freeReplyObject(reply);
        return Status::kError;
      }

      if (reply->type == REDIS_REPLY_NIL) {
        freeReplyObject(reply);
        return Status::kNotFound;
      }

      std::vector<Field> record;
      for (size_t i = 0; i < fields->size(); ++i) {
        Field field;
        field.name = (*fields)[i];
        field.value = reply->element[i]->str ? reply->element[i]->str : "";
        record.push_back(field);
      }
      result.push_back(record);
      freeReplyObject(reply);
    }
  }
  
  return Status::kOK;
}

Status RedisDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  std::string fullKey = table + ":" + key;

  // Prepare arguments for the HMSET command
  std::vector<const char*> argv;
  std::vector<size_t> argvlen;

  argv.push_back("HMSET");
  argvlen.push_back(5);
  argv.push_back(fullKey.c_str());
  argvlen.push_back(fullKey.length());

  for (const auto& field : values) {
    argv.push_back(field.name.c_str());
    argvlen.push_back(field.name.length());
    argv.push_back(field.value.c_str());
    argvlen.push_back(field.value.length());
  }

  redisReply *reply = (redisReply*)redisCommandArgv(
      context_, argv.size(), argv.data(), argvlen.data());
  if (!reply || context_->err) {
    if (reply) freeReplyObject(reply);
    return Status::kError;
  }
  freeReplyObject(reply);
  return Status::kOK;
}

Status RedisDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
  std::string fullKey = table + ":" + key;

  std::vector<const char*> argv;
  std::vector<size_t> argvlen;

  argv.push_back("HMSET");
  argvlen.push_back(5);
  argv.push_back(fullKey.c_str());
  argvlen.push_back(fullKey.length());

  for (const auto &field : values) {
    argv.push_back(field.name.c_str());
    argvlen.push_back(field.name.length());
    argv.push_back(field.value.c_str());
    argvlen.push_back(field.value.length());
  }

  redisReply *reply = (redisReply*)redisCommandArgv(
      context_, argv.size(), argv.data(), argvlen.data());
  if (!reply || context_->err) {
    if (reply) freeReplyObject(reply);
    return Status::kError;
  }
  freeReplyObject(reply);
  return Status::kOK;
}

Status RedisDB::Delete(const std::string &table, const std::string &key) {
  std::string fullKey = table + ":" + key;
  redisReply *reply = (redisReply*)redisCommand(context_, "DEL %b", fullKey.c_str(), (size_t)fullKey.length());
  if (!reply || context_->err) {
    if (reply) freeReplyObject(reply);
    return Status::kError;
  }

  Status status = (reply->integer > 0) ? Status::kOK : Status::kNotFound;
  freeReplyObject(reply);
  return status;
}

DB *NewRedisDB() {
  return new RedisDB;
}

const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

} // ycsbc

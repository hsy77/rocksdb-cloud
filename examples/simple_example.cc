// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// RocksDB简单例子，操作了一下写，读，和pinnableSlice（可 pinnable 的切片）。

#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism(); // 优化数据库性能，增加并行度。
  options.OptimizeLevelStyleCompaction(); //设置为层级压缩（level-style compaction），提高性能。
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  //Status保存数据库的状态，用于检查操作是否成功。
  //rocksdb 中可能会遇到错误的大多数函数都会返回这种类型的值。您可以检查此类结果是否正常，还可以打印相关的错误消息
  Status s = DB::Open(options, kDBPath, &db); // 使用指定的选项打开数据库，如果数据库不存在且启用了create_if_missing，会自动创建
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  {
    PinnableSlice pinnable_val; //用于高效读取值，避免额外的内存分配。
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);  //如果可以钉住（pin）值，pinnable_val 会直接引用数据库内部的内存，无需额外分配。
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer. 如果无法钉住值（例如值存储在外部存储介质中），RocksDB 会将值拷贝到 string_val。
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value"); // 如果值未被钉住，则 string_val 应包含值。
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse 重置 `PinnableSlice`，为下一次使用做好准备。
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  delete db;

  return 0;
}

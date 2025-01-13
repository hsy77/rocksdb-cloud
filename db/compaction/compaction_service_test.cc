//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// CompactionService 的测试套件，旨在验证自定义的 CompactionService 
// 实现（MyTestCompactionService）在不同情境下的正确性和稳定性。

#include "compaction_service.h"    //加的头文件
#include "db/db_test_util.h"       // RocksDB 测试相关的工具和基类。
#include "port/stack_trace.h"      // 堆栈跟踪，通常用于调试。
#include "table/unique_id_impl.h"  // 提供生成唯一ID的实现。

namespace ROCKSDB_NAMESPACE {

class MyTestCompactionService : public CompactionService {
 public:
 // 构造函数，接收数据库路径、配置选项、统计信息、事件监听器和表属性收集器工厂等参数，
 // 并初始化内部成员变量。
  MyTestCompactionService(
      std::string db_path, Options& options,
      std::shared_ptr<Statistics>& statistics,
      std::vector<std::shared_ptr<EventListener>>& listeners,
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
          table_properties_collector_factories)
      : db_path_(std::move(db_path)), // 数据库的路径，指向存储数据文件的目录。
        options_(options),            // RocksDB 的配置信息，包含各种操作选项。
        statistics_(statistics),      // 统计信息的共享指针，用于收集和报告数据库的各种性能和操作指标。
        start_info_("na", "na", "na", 0, Env::TOTAL),
        wait_info_("na", "na", "na", 0, Env::TOTAL),
        listeners_(listeners),        // 事件监听器的共享指针集合，用于监听和响应数据库操作事件。
        table_properties_collector_factories_(
            std::move(table_properties_collector_factories)) {} // 表属性收集器工厂的共享指针集合，用于在压缩过程中收集表的自定义属性。

  static const char* kClassName() { return "MyTestCompactionService"; }

  // 返回类名，用于标识 CompactionService 的实现。
  const char* Name() const override { return kClassName(); }

  // 调度压缩任务，生成唯一ID并将任务信息存储在内部数据结构中。
  // 返回 CompactionServiceScheduleResponse，指示任务是否成功调度。

  // Schedule 方法负责接收来自 RocksDB 的压缩任务请求，生成一个唯一的任务ID，
  // 并将任务的输入和信息存储起来。通过使用互斥锁确保线程安全，并且允许在测试过程中
  // 通过覆盖标志来模拟不同的任务调度结果。

  // info：包含压缩任务的元数据和配置信息，例如数据库名称、ID、会话ID、优先级等。
  // compaction_service_input：压缩任务的具体输入数据，可能包括需要压缩的文件列表、范围等。
  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override {

    // 在多线程环境下，Schedule 方法的执行是安全的，不会引起数据竞争。
    InstrumentedMutexLock l(&mutex_);

    // 将传入的 info 复制到成员变量 start_info_，记录压缩任务的开始信息。
    start_info_ = info;

    // 确保传入的压缩任务信息中的数据库名称与当前服务实例的数据库路径一致，以防止调度错误的任务。
    assert(info.db_name == db_path_);

    // 调用 RocksDB 的环境接口生成一个唯一的任务ID，用于标识和跟踪该压缩任务。
    std::string unique_id = Env::Default()->GenerateUniqueId();

    // 映射任务ID到具体的压缩输入数据。
    jobs_.emplace(unique_id, compaction_service_input);

    // 映射任务ID到任务的元信息。
    infos_.emplace(unique_id, info);

    // unique_id：生成的唯一任务ID。
    // 如果 is_override_start_status_ 为 true，则使用 override_start_status_ 作为任务的初始状态。
    // 否则，默认为 CompactionServiceJobStatus::kSuccess，表示任务成功调度。
    CompactionServiceScheduleResponse response(
        unique_id, is_override_start_status_
                       ? override_start_status_
                       : CompactionServiceJobStatus::kSuccess);
    return response;
  }

  // 等待压缩任务完成，根据任务ID检索任务输入，执行压缩，并返回任务状态。
  // scheduled_job_id：任务的唯一ID，由 Schedule 方法生成并分配。
  // result：指向字符串的指针，用于存储压缩操作的结果。
  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    // 用于存储从 jobs_ 中检索到的压缩输入数据。
    std::string compaction_input;
    {
      // 使用 InstrumentedMutexLock 对 mutex_ 加锁，
      // 确保访问和修改 jobs_ 和 infos_ 的操作是线程安全的。
      InstrumentedMutexLock l(&mutex_);

      // 查找任务ID在 jobs_ 中的对应压缩输入。
      // 如果任务ID不存在，返回失败状态。
      // 如果存在，移动压缩输入到 compaction_input，并从 jobs_ 中删除该任务记录，避免重复处理。
      auto job_index = jobs_.find(scheduled_job_id);
      if (job_index == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = std::move(job_index->second);
      jobs_.erase(job_index);

      // 查找任务ID在 infos_ 中的对应任务信息。
      // 如果任务ID不存在，返回失败状态。
      // 如果存在，移动任务信息到 wait_info_，并从 infos_ 中删除该任务记录。
      auto info_index = infos_.find(scheduled_job_id);
      if (info_index == infos_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      wait_info_ = std::move(info_index->second);
      infos_.erase(info_index);
    }

    // 在测试中，允许通过标志 is_override_wait_status_ 覆盖默认的等待状态，
    // 返回预设的状态而不执行实际的压缩。这对于模拟不同的任务结果非常有用。
    if (is_override_wait_status_) {
      return override_wait_status_;
    }

    // 创建并配置一个 CompactionServiceOptionsOverride 对象，
    // 确保压缩操作使用与当前服务实例一致的配置选项。
    // 包括环境 (env)、校验和生成工厂、比较器、合并操作符、压缩过滤器、
    // 前缀提取器、表工厂、SST 分区工厂和统计信息。
    // 如果存在事件监听器和表属性收集器工厂，则将它们也包含在压缩选项中。
    CompactionServiceOptionsOverride options_override;
    options_override.env = options_.env;
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = statistics_;
    if (!listeners_.empty()) {
      options_override.listeners = listeners_;
    }

    if (!table_properties_collector_factories_.empty()) {
      options_override.table_properties_collector_factories =
          table_properties_collector_factories_;
    }

    // 准备一个 OpenAndCompactOptions 对象，用于控制实际的压缩操作。
    // 特别是，通过指针 options.canceled 允许外部控制压缩任务的取消。
    OpenAndCompactOptions options;
    options.canceled = &canceled_;

    // 调用 RocksDB 的 DB::OpenAndCompact 方法执行实际的压缩操作。
    // options：OpenAndCompactOptions，控制压缩行为（如取消）。
    // db_path_：数据库路径。
    // db_path_ + "/" + scheduled_job_id：目标路径，通常指向一个临时或特定于任务的目录。
    // compaction_input：具体的压缩输入数据。
    // result：用于存储压缩结果的输出参数。
    // options_override：覆盖的压缩选项，确保使用正确的配置执行压缩。
    Status s =
        DB::OpenAndCompact(options, db_path_, db_path_ + "/" + scheduled_job_id,
                           compaction_input, result, options_override);
    
    // 在测试中，允许通过标志 is_override_wait_result_ 覆盖实际的压缩结果，
    // 为特定的测试场景提供控制。
    if (is_override_wait_result_) {
      *result = override_wait_result_;
    }

    // 原子地增加已完成的压缩任务计数器，供测试验证使用。
    compaction_num_.fetch_add(1);

    // 根据 OpenAndCompact 的执行结果返回相应的任务状态。
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  // 获取已完成的压缩任务数量。
  int GetCompactionNum() { return compaction_num_.load(); }

  // 获取任务开始和等待时的相关信息。
  CompactionServiceJobInfo GetCompactionInfoForStart() { return start_info_; }
  CompactionServiceJobInfo GetCompactionInfoForWait() { return wait_info_; }

  // 用于测试中覆盖默认的任务状态和结果。
  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status_ = true;
    override_start_status_ = s;
  }

  void OverrideWaitStatus(CompactionServiceJobStatus s) {
    is_override_wait_status_ = true;
    override_wait_status_ = s;
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result_ = true;
    override_wait_result_ = std::move(str);
  }

  // 重置所有覆盖状态。
  void ResetOverride() {
    is_override_wait_result_ = false;
    is_override_start_status_ = false;
    is_override_wait_status_ = false;
  }

  // 设置是否取消正在进行的压缩任务。
  void SetCanceled(bool canceled) { canceled_ = canceled; }

 private:
  InstrumentedMutex mutex_; // 用于线程安全的互斥锁。
  std::atomic_int compaction_num_{0}; // 记录压缩任务数量。
  std::map<std::string, std::string> jobs_; // 存储任务输入的映射。
  std::map<std::string, CompactionServiceJobInfo> infos_; // 存储任务信息的映射。
  const std::string db_path_;   // 存储初始化时传入的参数。
  Options options_;
  std::shared_ptr<Statistics> statistics_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string override_wait_result_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories_;
  std::atomic_bool canceled_{false};
};

// 类定义
class CompactionServiceTest : public DBTestBase {
 public:
 // 通过调用基类 DBTestBase 的构造函数，初始化测试环境。
  explicit CompactionServiceTest()
      : DBTestBase("compaction_service_test", true) {}

 protected:
 // 配置并重新打开数据库以使用自定义的 CompactionService
 // 配置数据库选项以使用 MyTestCompactionService，然后销毁并重新打开数据库，
 // 创建多个列族（Column Families）。
  void ReopenWithCompactionService(Options* options) {
    options->env = env_;
    primary_statistics_ = CreateDBStatistics();
    options->statistics = primary_statistics_;
    compactor_statistics_ = CreateDBStatistics();

    compaction_service_ = std::make_shared<MyTestCompactionService>(
        dbname_, *options, compactor_statistics_, remote_listeners,
        remote_table_properties_collector_factories);
    options->compaction_service = compaction_service_;
    DestroyAndReopen(*options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, *options);
  }

  // 获取压缩器和主数据库的统计信息。
  Statistics* GetCompactorStatistics() { return compactor_statistics_.get(); }

  Statistics* GetPrimaryStatistics() { return primary_statistics_.get(); }

  // 获取当前使用的 MyTestCompactionService 实例。
  MyTestCompactionService* GetCompactionService() {
    CompactionService* cs = compaction_service_.get();
    return static_cast_with_check<MyTestCompactionService>(cs);
  }

  // 生成测试数据，插入多个键值对并刷新到磁盘。可选地手动移动文件到特定层级。
  void GenerateTestData(bool move_files_manually = false) {
    // Generate 20 files @ L2 Per CF
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 20; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 10 + j;
          ASSERT_OK(Put(cf_id, Key(key_id), "value" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(2, cf_id);
      }

      // Generate 10 files @ L1 overlap with all 20 files @ L2
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 20 + j * 2;
          ASSERT_OK(
              Put(cf_id, Key(key_id), "value_new" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(1, cf_id);
        ASSERT_EQ(FilesPerLevel(cf_id), "0,10,20");
      }
    }
  }

  // 验证数据的正确性，确保压缩后的数据符合预期。
  void VerifyTestData() {
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 200; i++) {
        auto result = Get(cf_id, Key(i));
        if (i % 2) {
          ASSERT_EQ(result, "value" + std::to_string(i));
        } else {
          ASSERT_EQ(result, "value_new" + std::to_string(i));
        }
      }
    }
  }

  std::vector<std::shared_ptr<EventListener>> remote_listeners;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      remote_table_properties_collector_factories;

 private:
  std::shared_ptr<Statistics> compactor_statistics_;
  std::shared_ptr<Statistics> primary_statistics_;
  std::shared_ptr<CompactionService> compaction_service_;
};

// 测试基本压缩功能
// 测试自动压缩是否正常工作，验证统计信息和压缩次数。
// 模拟压缩失败的情况，通过 SyncPoint 覆盖压缩状态，并确保系统能够正确处理压缩失败。
TEST_F(CompactionServiceTest, BasicCompactions) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  Statistics* primary_statistics = GetPrimaryStatistics();
  Statistics* compactor_statistics = GetCompactorStatistics();

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);

  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES), 1);
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
  // even with remote compaction, primary host still needs to read SST files to
  // `verify_table()`.
  ASSERT_GE(primary_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  // all the compaction write happens on the remote side
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_GE(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 1);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_READ_BYTES),
            primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES));
  // compactor is already the remote side, which doesn't have remote
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            0);

  // Test failed compaction
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        auto s = static_cast<Status*>(status);
        *s = Status::Aborted("MyTestCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = Put(Key(key_id), "value_new" + std::to_string(key_id));
      if (s.IsAborted()) {
        break;
      }
    }
    if (s.IsAborted()) {
      break;
    }
    s = Flush();
    if (s.IsAborted()) {
      break;
    }
    s = dbfull()->TEST_WaitForCompact();
    if (s.IsAborted()) {
      break;
    }
  }
  ASSERT_TRUE(s.IsAborted());

  // Test re-open and successful unique id verification
  std::atomic_int verify_passed{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
        // override job status
        auto id = static_cast<UniqueId64x2*>(arg);
        assert(*id != kNullUniqueId64x2);
        verify_passed++;
      });
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1", "cf_2", "cf_3"},
                           options);
  ASSERT_GT(verify_passed, 0);
  Close();
}

// 测试手动触发压缩
// 禁用自动压缩，手动触发压缩范围，验证压缩次数和数据正确性。
TEST_F(CompactionServiceTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  end_str = Key(92);
  end = end_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();
}

// 测试在远程端取消压缩任务
// 测试在压缩任务开始前和进行中取消压缩任务，确保系统能够正确处理中止操作。
TEST_F(CompactionServiceTest, CancelCompactionOnRemoteSide) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Test cancel compaction at the beginning
  my_cs->SetCanceled(true);
  auto s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();

  // Test cancel compaction in progress
  ReopenWithCompactionService(&options);
  GenerateTestData();
  my_cs = GetCompactionService();
  my_cs->SetCanceled(false);

  std::atomic_bool cancel_issued{false};
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Inprogress",
                                        [&](void* /*arg*/) {
                                          cancel_issued = true;
                                          my_cs->SetCanceled(true);
                                        });

  SyncPoint::GetInstance()->EnableProcessing();

  s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(cancel_issued);
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();
}

// 测试压缩任务启动失败的情况
// 覆盖 Schedule 方法的响应为失败，确保系统能够正确处理启动失败的压缩任务。
TEST_F(CompactionServiceTest, FailedToStart) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kFailure);

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete());
}

// 测试压缩任务返回无效结果
// 覆盖 Wait 方法返回无效结果，验证系统能够检测并处理无效结果。
TEST_F(CompactionServiceTest, InvalidResult) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideWaitResult("Invalid Str");

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_FALSE(s.ok());
}

// 测试子压缩的功能
// 配置多个子压缩任务，确保系统能够正确处理并发的子压缩任务。
TEST_F(CompactionServiceTest, SubCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  int compaction_num_before = my_cs->GetCompactionNum();

  auto cro = CompactRangeOptions();
  cro.max_subcompactions = 10;
  Status s = db_->CompactRange(cro, nullptr, nullptr);
  ASSERT_OK(s);
  VerifyTestData();
  int compaction_num = my_cs->GetCompactionNum() - compaction_num_before;
  // make sure there's sub-compaction by checking the compaction number
  ASSERT_GE(compaction_num, 2);
}

class PartialDeleteCompactionFilter : public CompactionFilter {
 public:
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& key, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    int i = std::stoi(key.ToString().substr(3));
    if (i > 5 && i <= 105) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

  const char* Name() const override { return "PartialDeleteCompactionFilter"; }
};

 // 测试压缩过滤器的功能
 // 使用自定义的压缩过滤器（PartialDeleteCompactionFilter），
 // 在压缩过程中部分删除键值对，验证过滤器的效果。
TEST_F(CompactionServiceTest, CompactionFilter) {
  Options options = CurrentOptions();
  std::unique_ptr<CompactionFilter> delete_comp_filter(
      new PartialDeleteCompactionFilter());
  options.compaction_filter = delete_comp_filter.get();
  ReopenWithCompactionService(&options);
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i > 5 && i <= 105) {
      ASSERT_EQ(result, "NOT_FOUND");
    } else if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

// 测试快照与压缩的交互
// 创建快照，进行压缩，验证快照期间的数据一致性。
TEST_F(CompactionServiceTest, Snapshot) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put(Key(1), "value1"));
  ASSERT_OK(Put(Key(2), "value1"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), "value2"));
  ASSERT_OK(Put(Key(3), "value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ("value1", Get(Key(1), s1));
  ASSERT_EQ("value2", Get(Key(1)));
  db_->ReleaseSnapshot(s1);
}

// 测试并发压缩任务
// 配置多个后台线程，触发并发压缩任务，验证系统在高并发下的表现和数据正确性。
TEST_F(CompactionServiceTest, ConcurrentCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.max_background_jobs = 20;
  ReopenWithCompactionService(&options);
  GenerateTestData(true);

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  std::vector<std::thread> threads;
  for (const auto& file : meta.levels[1].files) {
    threads.emplace_back([&]() {
      std::string fname = file.db_path + "/" + file.name;
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  VerifyTestData();
  auto my_cs = GetCompactionService();
  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
  ASSERT_EQ(FilesPerLevel(), "0,0,10");
}

// 测试压缩任务信息的获取
// 触发压缩任务，获取并验证压缩任务的信息，如数据库名称、ID、会话ID和优先级。
TEST_F(CompactionServiceTest, CompactionInfo) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  auto my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_GE(comp_num, 1);

  CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(dbname_, info.db_name);
  std::string db_id, db_session_id;
  ASSERT_OK(db_->GetDbIdentity(db_id));
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_OK(db_->GetDbSessionId(db_session_id));
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(dbname_, info.db_name);
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);

  // Test priority USER
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  SstFileMetaData file = meta.levels[1].files[0];
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {file.db_path + "/" + file.name}, 2));
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::USER, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::USER, info.priority);

  // Test priority BOTTOM
  env_->SetBackgroundThreads(1, Env::BOTTOM);
  options.num_levels = 2;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::BOTTOM, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::BOTTOM, info.priority);
}

// 测试自动压缩时的本地回退机制
TEST_F(CompactionServiceTest, FallbackLocalAuto) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kUseLocal);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  ASSERT_EQ(my_cs->GetCompactionNum(), 0);

  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES), 0);
}

// 测试手动压缩时的本地回退机制
TEST_F(CompactionServiceTest, FallbackLocalManual) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  // re-enable remote compaction
  my_cs->ResetOverride();
  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GT(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);

  // return run local again with API WaitForComplete
  my_cs->OverrideWaitStatus(CompactionServiceJobStatus::kUseLocal);
  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  primary_write_bytes = primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_EQ(my_cs->GetCompactionNum(),
            comp_num);  // no remote compaction is run
  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_write_bytes);

  // verify result after 2 manual compactions
  VerifyTestData();
}

// 测试远程事件监听器的功能
// 使用自定义的事件监听器，监听压缩过程中的事件，如子压缩开始/完成、文件创建等，确保事件被正确触发和处理。
TEST_F(CompactionServiceTest, RemoteEventListener) {
  class RemoteEventListenerTest : public EventListener {
   public:
    const char* Name() const override { return "RemoteEventListenerTest"; }

    void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
      auto result = on_going_compactions.emplace(info.job_id);
      ASSERT_TRUE(result.second);  // make sure there's no duplication
      compaction_num++;
      EventListener::OnSubcompactionBegin(info);
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      auto num = on_going_compactions.erase(info.job_id);
      ASSERT_TRUE(num == 1);  // make sure the compaction id exists
      EventListener::OnSubcompactionCompleted(info);
    }
    void OnTableFileCreated(const TableFileCreationInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_created++;
      EventListener::OnTableFileCreated(info);
    }
    void OnTableFileCreationStarted(
        const TableFileCreationBriefInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_creation_started++;
      EventListener::OnTableFileCreationStarted(info);
    }

    bool ShouldBeNotifiedOnFileIO() override {
      file_io_notified++;
      return EventListener::ShouldBeNotifiedOnFileIO();
    }

    std::atomic_uint64_t file_io_notified{0};
    std::atomic_uint64_t file_creation_started{0};
    std::atomic_uint64_t file_created{0};

    std::set<int> on_going_compactions;  // store the job_id
    std::atomic_uint64_t compaction_num{0};
  };

  auto listener = new RemoteEventListenerTest();
  remote_listeners.emplace_back(listener);

  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // check the events are triggered
  ASSERT_TRUE(listener->file_io_notified > 0);
  ASSERT_TRUE(listener->file_creation_started > 0);
  ASSERT_TRUE(listener->file_created > 0);
  ASSERT_TRUE(listener->compaction_num > 0);
  ASSERT_TRUE(listener->on_going_compactions.empty());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
}

// 测试表属性收集器的功能
// 使用自定义的表属性收集器，在生成的 SST 文件中添加用户属性，验证属性收集器的效果和正确性。
TEST_F(CompactionServiceTest, TablePropertiesCollector) {
  const static std::string kUserPropertyName = "TestCount";

  class TablePropertiesCollectorTest : public TablePropertiesCollector {
   public:
    Status Finish(UserCollectedProperties* properties) override {
      *properties = UserCollectedProperties{
          {kUserPropertyName, std::to_string(count_)},
      };
      return Status::OK();
    }

    UserCollectedProperties GetReadableProperties() const override {
      return UserCollectedProperties();
    }

    const char* Name() const override { return "TablePropertiesCollectorTest"; }

    Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                      EntryType /*type*/, SequenceNumber /*seq*/,
                      uint64_t /*file_size*/) override {
      count_++;
      return Status::OK();
    }

   private:
    uint32_t count_ = 0;
  };

  class TablePropertiesCollectorFactoryTest
      : public TablePropertiesCollectorFactory {
   public:
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /*context*/) override {
      return new TablePropertiesCollectorTest();
    }

    const char* Name() const override {
      return "TablePropertiesCollectorFactoryTest";
    }
  };

  auto factory = new TablePropertiesCollectorFactoryTest();
  remote_table_properties_collector_factories.emplace_back(factory);

  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  ReopenWithCompactionService(&options);

  // generate a few SSTs locally which should not have user property
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    ASSERT_EQ(it, properties.end());
  }

  // trigger compaction
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));

  bool has_user_property = false;
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    if (it != properties.end()) {
      has_user_property = true;
      ASSERT_GT(std::stoi(it->second), 0);
    }
  }
  ASSERT_TRUE(has_user_property);
}

}  // namespace ROCKSDB_NAMESPACE

// 安装堆栈跟踪处理器，初始化 Google Test 框架，注册自定义对象，然后运行所有测试用例。
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}

#include "compaction_service.h"

#include <aws/core/Aws.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <nlohmann/json.hpp>

#include <iostream>
#include <string>
#include <thread>

// 使用 nlohmann::json
using json = nlohmann::json;

// 定义返回结果结构体
struct CompactionResult {
    std::string status;
    std::string message;
    uint64_t process_latency;
    uint64_t open_db_latency;
};


// 序列化 Compaction 任务参数为 JSON 字符串
std::string SerializeCompactionTask(const std::string& db_path, 
                                    const std::string& scheduled_job_id,
                                    const std::string& compaction_input,
                                    const std::string& options_override_json) {
    json task;
    task["db_path"] = db_path;
    task["scheduled_job_id"] = scheduled_job_id;
    task["compaction_input"] = compaction_input;
    task["options_override"] = options_override_json;
    return task.dump();
}

// 调用 Lambda 函数
CompactionResult TriggerCompactionLambda(const std::string& function_name, const std::string& payload) {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::Region::US_EAST_1; // 替换为您的区域

    Aws::Lambda::LambdaClient lambda_client(clientConfig);

    Aws::Lambda::Model::InvokeRequest invoke_request;
    invoke_request.SetFunctionName(function_name);
    invoke_request.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse); // 同步调用
    invoke_request.SetPayload(Aws::Utils::ByteBuffer((unsigned char*)payload.c_str(), payload.length()));

    auto outcome = lambda_client.Invoke(invoke_request);

    CompactionResult result;

    if (outcome.IsSuccess()) {
        auto& result_payload = outcome.GetResult().GetPayload();
        std::stringstream ss;
        ss << std::istreambuf_iterator<char>(result_payload);
        std::string response_json = ss.str();

        // 解析响应
        try {
            json response = json::parse(response_json);
            result.status = response["status"].get<std::string>();
            result.message = response["message"].get<std::string>();
            result.process_latency = response["process_latency"].get<uint64_t>();
            result.open_db_latency = response["open_db_latency"].get<uint64_t>();
        } catch (const std::exception& e) {
            result.status = "FAILED";
            result.message = std::string("Failed to parse Lambda response: ") + e.what();
            result.process_latency = 0;
            result.open_db_latency = 0;
        }
    } else {
        result.status = "FAILED";
        result.message = outcome.GetError().GetMessage();
        result.process_latency = 0;
        result.open_db_latency = 0;
    }

    return result;
}

namespace ROCKSDB_NAMESPACE {

// 启动压缩任务
CompactionServiceScheduleResponse MyTestCompactionService::Schedule(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_service_input) {

  // 获取互斥锁保护
  InstrumentedMutexLock l(&mutex_);
  // 保存任务信息
  start_info_ = info;
  // 验证数据库路径
  assert(info.db_name == db_path_);
  // 将任务加入任务队列
  jobs_.emplace(info.job_id, compaction_service_input);
  // 返回状态（可被覆盖）
  CompactionServiceJobStatus s = CompactionServiceJobStatus::kSuccess;
  if (is_override_start_status_) {
    return override_start_status_;
  }
  CompactionServiceScheduleResponse response(s);
  return response;
}

// 等待任务完成并处理结果
CompactionServiceJobStatus MyTestCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info,
    CompactionAdditionInfo* compaction_addition_info,
    std::string* compaction_service_result) {

  std::string compaction_input;
  assert(info.db_name == db_path_);
  {
    InstrumentedMutexLock l(&mutex_);
    wait_info_ = info;

    // 查找并获取任务信息
    auto i = jobs_.find(info.job_id);
    if (i == jobs_.end()) {
      return CompactionServiceJobStatus::kFailure;
    }
    compaction_input = std::move(i->second);
    jobs_.erase(i);
  }

  if (is_override_wait_status_) {
    return override_wait_status_;
  }

  // 设置压缩选项
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

  OpenAndCompactOptions options;
  options.canceled = &canceled_;

  // 填充 CompactionAdditionInfo
  CompactionAdditionInfo addition_info;

  addition_info.set_score(compaction_addition_info->score);
  addition_info.set_num_entries(compaction_addition_info->num_entries);
  addition_info.set_num_deletions(compaction_addition_info->num_deletions);
  addition_info.set_compensated_file_size(
      compaction_addition_info->compensated_file_size);
  addition_info.set_output_level(compaction_addition_info->output_level());
  addition_info.set_start_level(compaction_addition_info->start_level());

  // 获取 trigger_ms
  uint64_t trigger_ms = compaction_addition_info->trigger_ms();

  // 定义 Lambda 任务参数
  std::string scheduled_job_id = std::to_string(info.job_id);
  std::string payload = SerializeCompactionTask(
      db_path_,
      scheduled_job_id,
      compaction_input,
      options_override.dump()
  );

  // 初始化 AWS SDK
  Aws::SDKOptions aws_options;
  Aws::InitAPI(aws_options);
  CompactionResult lambda_result;
  {
      // 调用 Lambda 函数
      lambda_result = TriggerCompactionLambda("rocksdb-openandcompact", payload);
  }
  Aws::ShutdownAPI(aws_options);

  if (lambda_result.status == "SUCCESS") {
      // 解析返回的消息（如果包含性能指标等信息）
      try {
          json response = json::parse(lambda_result.message);
          *compaction_service_result = response["message"].get<std::string>();
          *compact_process_latency = response["process_latency"].get<uint64_t>();
          *open_db_latency = response["open_db_latency"].get<uint64_t>();

          // 更新 compaction_addition_info
          compaction_addition_info->trigger_ms = lambda_result.process_latency;
          compaction_addition_info->num_entries = lambda_result.open_db_latency;

          compaction_num_.fetch_add(1);
          return CompactionServiceJobStatus::kSuccess;
      } catch (const std::exception& e) {
          std::cerr << "Failed to parse Lambda response: " << e.what() << std::endl;
          return CompactionServiceJobStatus::kUseLocal;
      }
  } else {
      std::cerr << "Lambda compaction task failed: " << lambda_result.message << std::endl;
      return CompactionServiceJobStatus::kUseLocal;
  }
}

};  // namespace ROCKSDB_NAMESPACE
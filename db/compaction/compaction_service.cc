#include "compaction_service.h"

#include <aws/core/Aws.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <nlohmann/json.hpp>

#include <iostream>
#include <string>
#include <thread>
#include <memory>

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
                                    const std::string& compaction_input) {
    json task;
    task["db_path"] = db_path;
    task["scheduled_job_id"] = scheduled_job_id;
    task["compaction_input"] = compaction_input;
    return task.dump();
}

// 调用 Lambda 函数
CompactionResult TriggerCompactionLambda(const std::string& function_name, const std::string& payload) {
    Aws::Lambda::LambdaClient lambda_client;

    Aws::Lambda::Model::InvokeRequest invoke_request;
    invoke_request.SetFunctionName(function_name);
    
    // 使用 SetBody 设置请求负载
    auto payload_stream = Aws::MakeShared<Aws::StringStream>("InvokeRequestPayload");
    (*payload_stream) << payload;
    invoke_request.SetBody(payload_stream);

    auto outcome = lambda_client.Invoke(invoke_request);

    CompactionResult result;

    if (outcome.IsSuccess()) {
        auto& result_payload = outcome.GetResult().GetPayload();
        std::stringstream ss;
        
        // 使用 rdbuf() 将整个缓冲区内容插入到 stringstream
        ss << result_payload.rdbuf();
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
CompactionServiceJobStatus MyTestCompactionService::StartV2(
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
  return s;
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

  OpenAndCompactOptions options;
  options.canceled = &canceled_;

  // 获取 trigger_ms
  uint64_t trigger_ms = compaction_addition_info->trigger_ms;

  // 定义 Lambda 任务参数
  std::string scheduled_job_id = std::to_string(info.job_id);
  std::string payload = SerializeCompactionTask(
      db_path_,
      scheduled_job_id,
      compaction_input
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

          // 更新 compaction_addition_info
          compaction_addition_info->trigger_ms = lambda_result.process_latency;

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
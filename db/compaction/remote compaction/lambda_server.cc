// your_lambda_executable.cpp

#include <aws/lambda-runtime/runtime.h>
#include <nlohmann/json.hpp>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <chrono>
#include <iostream>
#include <string>

using json = nlohmann::json;
using namespace aws::lambda_runtime;

// 定义返回结果结构
struct CompactionResult {
    std::string status;
    std::string message;
    uint64_t process_latency;
    uint64_t open_db_latency;
};

// 获取当前时间戳（毫秒）
uint64_t GetCurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::high_resolution_clock::now().time_since_epoch())
        .count();
}

// 处理 Lambda 事件的函数
json HandleRequest(json input) {
    std::string db_path;
    std::string scheduled_job_id;
    std::string compaction_input;
    std::string options_override_str;

    // 解析输入参数
    try {
        db_path = input.at("db_path").get<std::string>();
        scheduled_job_id = input.at("scheduled_job_id").get<std::string>();
        compaction_input = input.at("compaction_input").get<std::string>();
        options_override_str = input.at("options_override").get<std::string>();
    } catch (const std::exception& e) {
        CompactionResult result = {"FAILED", std::string("Invalid input: ") + e.what(), 0, 0};
        return json(result);
    }

    // 解析 options_override JSON
    json options_override;
    try {
        options_override = json::parse(options_override_str);
    } catch (const std::exception& e) {
        CompactionResult result = {"FAILED", std::string("Invalid options_override: ") + e.what(), 0, 0};
        return json(result);
    }

    // 配置 RocksDB 的 Options
    rocksdb::Options options;
    if (options_override.contains("create_if_missing")) {
        options.create_if_missing = options_override["create_if_missing"].get<bool>();
    }
    // 根据需要添加更多选项配置...
    rocksdb::CompactionServiceOptionsOverride options_override;
    ROCKSDB_NAMESPACE::Options options_;
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory =
        options_.sst_partitioner_factory;
    options_override.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

    rocksdb::Status status;
    std::string compact_result_message;
    uint64_t start_time = GetCurrentTimeMs();
    uint64_t open_db_latency = 0;

    CompactionResult result;

    // 执行 OpenAndCompact
    status = ROCKSDB_NAMESPACE::DB::OpenAndCompact(
        db_path,db_path + "/" + scheduled_job_id,
        compaction_input,&compact_result_message,&start_time,
        &open_db_latency,options_override);

    uint64_t end_time = GetCurrentTimeMs();
    result.process_latency = end_time - start_time;

    if (status.ok()) {
        result.status = "SUCCESS";
        result.message = compact_result_message;
        result.open_db_latency = open_db_latency;
    } else {
        result.status = "FAILED";
        result.message = status.ToString();
        result.open_db_latency = 0;
    }

    return json(result);
}

int main() {
    run_handler(handler<decltype(HandleRequest), json>(HandleRequest));
}
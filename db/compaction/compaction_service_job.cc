//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Compaction Service 的实现，用于管理和执行数据库的压缩任务。

#include "db/compaction/compaction_job.h"   // 包含与压缩任务相关的类和功能。
#include "db/compaction/compaction_state.h"
#include "logging/logging.h"                // 提供日志记录功能。
#include "monitoring/iostats_context_imp.h" // 用于监控 I/O 统计信息和线程状态。
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"         // 辅助处理数据库配置选项。
#include "rocksdb/utilities/options_type.h"

namespace ROCKSDB_NAMESPACE {

// 前向声明 SubcompactionState 类，表示该类将在后续代码中使用，但具体定义在别处。
// 这有助于减少编译依赖，提高编译速度。
class SubcompactionState;

// 使用 CompactionService 处理键值压缩任务。
// CompactionService 可以是一个远程服务或自定义实现，负责执行实际的压缩操作。

// 通过 CompactionService 调度和执行一个压缩任务，处理所有必要的数据准备、任务调度、
// 结果处理和错误处理。它确保压缩任务的各个方面都被正确管理，并与 CompactionService 进行有效的通信。
CompactionServiceJobStatus
CompactionJob::ProcessKeyValueCompactionWithCompactionService(
    SubcompactionState* sub_compact) {

  // 确保传入的 sub_compact 和相关指针不为空，并且存在 compaction_service。
  // 这是为了在调试阶段捕捉潜在的逻辑错误。
  assert(sub_compact);
  assert(sub_compact->compaction);
  assert(db_options_.compaction_service);

  // 从 sub_compact 中获取当前的压缩任务，并初始化 compaction_input，包含目标压缩层级和数据库 ID。
  const Compaction* compaction = sub_compact->compaction;
  CompactionServiceInput compaction_input;
  compaction_input.output_level = compaction->output_level();
  compaction_input.db_id = db_id_;

// 遍历所有输入文件，将它们的文件名添加到 compaction_input.input_files 中。
// 这些文件将被传递给 CompactionService 进行压缩。
  const std::vector<CompactionInputFiles>& inputs =
      *(compact_->compaction->inputs());

  // 添加的变量
  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;
  uint64_t compensated_file_size = 0;
  
  for (const auto& files_per_level : inputs) {
    for (const auto& file : files_per_level.files) {
      compaction_input.input_files.emplace_back(
          MakeTableFileName(file->fd.GetNumber()));
      num_entries += file->num_entries;
      num_deletions += file->num_deletions;
      compensated_file_size += file->compensated_file_size;
    }
  }
  CompactionAdditionInfo compaction_addition_info{
      sub_compact->compaction->score(),
      num_entries,
      num_deletions,
      compensated_file_size,
      compaction_input.output_level,
      compaction->start_level(),
      db_options_.clock->NowMicros()};

  // 记录压缩任务所属的列族名称、最新的列族选项、数据库选项和现存的快照。
  // 这些信息对于正确执行压缩任务至关重要。
  compaction_input.column_family.name =
      compaction->column_family_data()->GetName();
  compaction_input.column_family.options =
      compaction->column_family_data()->GetLatestCFOptions();
  compaction_input.db_options =
      BuildDBOptions(db_options_, mutable_db_options_copy_);
  compaction_input.snapshots = existing_snapshots_;

  // 指定压缩任务的键范围，即开始键和结束键。如果存在，则将其转换为字符串形式，否则为空。
  compaction_input.has_begin = sub_compact->start.has_value();
  compaction_input.begin =
      compaction_input.has_begin ? sub_compact->start->ToString() : "";
  compaction_input.has_end = sub_compact->end.has_value();
  compaction_input.end =
      compaction_input.has_end ? sub_compact->end->ToString() : "";

  // 将 CompactionServiceInput 序列化为二进制字符串，以便传输给 CompactionService。
  // 如果序列化失败，记录错误状态并返回失败。
  std::string compaction_input_binary;
  Status s = compaction_input.Write(&compaction_input_binary);
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  // 将输入文件名拼接成一个字符串
  std::ostringstream input_files_oss;
  bool is_first_one = true;
  for (const auto& file : compaction_input.input_files) {
    input_files_oss << (is_first_one ? "" : ", ") << file;
    is_first_one = false;
  }

  // 并记录日志，指示远程压缩任务的开始，包括列族名称、作业 ID、输出层级和输入文件列表。
  ROCKS_LOG_INFO(
      db_options_.info_log,
      "[%s] [JOB %d] Starting remote compaction (output level: %d): %s",
      compaction_input.column_family.name.c_str(), job_id_,
      compaction_input.output_level, input_files_oss.str().c_str());
  
  // 创建 CompactionServiceJobInfo，包含数据库名称、ID、会话 ID、压缩作业 ID 和线程优先级。
  CompactionServiceJobInfo info(dbname_, db_id_, db_session_id_,
                                GetCompactionId(sub_compact), thread_pri_);

  // 调用 compaction_service->Schedule 方法，传递作业信息和序列化的压缩输入，得到调度响应。
  // 用的schedule，返回的response
  CompactionServiceScheduleResponse response =
      db_options_.compaction_service->Schedule(info, compaction_input_binary);

  // 根据调度响应的状态进行不同的处理：
  // - kSuccess：任务成功调度，继续等待压缩完成。
  // - kFailure：记录错误状态和警告日志，并返回失败状态。
  // - kUseLocal：表示压缩服务决定回退到本地压缩，记录信息日志并返回该状态。
  // - default：对于未知状态，触发断言失败，便于调试。
  switch (response.status) {
    case CompactionServiceJobStatus::kSuccess:
      break;
    case CompactionServiceJobStatus::kFailure:
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to schedule a remote compaction job.");
      ROCKS_LOG_WARN(db_options_.info_log,
                     "[%s] [JOB %d] Remote compaction failed to start.",
                     compaction_input.column_family.name.c_str(), job_id_);
      return response.status;
    case CompactionServiceJobStatus::kUseLocal:
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[%s] [JOB %d] Remote compaction fallback to local by API (Schedule)",
          compaction_input.column_family.name.c_str(), job_id_);
      return response.status;
    default:
      assert(false);  // unknown status
      break;
  }

  // 记录等待远程压缩完成的日志。
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Waiting for remote compaction...",
                 compaction_input.column_family.name.c_str(), job_id_);

  // 调用 compaction_service->Wait 方法，传递调度得到的任务 ID，
  // 等待压缩完成，并接收压缩结果的序列化数据。
  std::string compaction_result_binary;

  // 没有用wait
  // CompactionServiceJobStatus compaction_status =
  //     db_options_.compaction_service->Wait(response.scheduled_job_id,
  //                                          &compaction_result_binary);
  
  CompactionServiceJobStatus compaction_status =
    db_options_.compaction_service->WaitForCompleteV2(
      info, &compaction_addition_info, &compaction_result_binary);

  // 如果 Wait 方法返回 kUseLocal，记录回退到本地压缩的日志，并返回该状态。
  if (compaction_status == CompactionServiceJobStatus::kUseLocal) {
    ROCKS_LOG_INFO(
        db_options_.info_log,
        // "[%s] [JOB %d] Remote compaction fallback to local by API (Wait)",
        "[%s] [JOB %d] Remote compaction fallback to local by API (WaitForCompleteV2)",
        compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  // 否则，尝试反序列化压缩结果。如果反序列化失败，记录错误并返回失败状态。
  CompactionServiceResult compaction_result;
  s = CompactionServiceResult::Read(compaction_result_binary,
                                    &compaction_result);

  // 如果 compaction_status 为失败：
  // - 检查反序列化是否成功。
  //   - 如果成功且压缩结果的状态为成功，记录不完整状态（本质上是失败，因为 compaction_status 是失败）。
  //   - 如果压缩结果的状态为失败，使用远程返回的状态。
  // - 如果反序列化失败，记录不完整状态。
  // - 记录警告日志并返回失败状态。
  if (compaction_status == CompactionServiceJobStatus::kFailure) {
    if (s.ok()) {
      if (compaction_result.status.ok()) {
        sub_compact->status = Status::Incomplete(
            "CompactionService failed to run the compaction job (even though "
            "the internal status is okay).");
      } else {
        // set the current sub compaction status with the status returned from
        // remote
        sub_compact->status = compaction_result.status;
      }
    } else {
      sub_compact->status = Status::Incomplete(
          "CompactionService failed to run the compaction job (and no valid "
          "result is returned).");
      compaction_result.status.PermitUncheckedError();
    }
    ROCKS_LOG_WARN(db_options_.info_log,
                   "[%s] [JOB %d] Remote compaction failed.",
                   compaction_input.column_family.name.c_str(), job_id_);
    return compaction_status;
  }

  // 如果反序列化过程中出现错误，记录错误状态并返回失败。
  // 否则，设置 sub_compact 的状态为压缩结果的状态。
  if (!s.ok()) {
    sub_compact->status = s;
    compaction_result.status.PermitUncheckedError();
    return CompactionServiceJobStatus::kFailure;
  }
  sub_compact->status = compaction_result.status;

  // 将压缩结果中的输出文件名称拼接成字符串
  std::ostringstream output_files_oss;
  is_first_one = true;
  for (const auto& file : compaction_result.output_files) {
    output_files_oss << (is_first_one ? "" : ", ") << file.file_name;
    is_first_one = false;
  }

  // 并记录日志，指示接收到远程压缩结果，包括输出路径和文件列表。
  ROCKS_LOG_INFO(db_options_.info_log,
                 "[%s] [JOB %d] Receive remote compaction result, output path: "
                 "%s, files: %s",
                 compaction_input.column_family.name.c_str(), job_id_,
                 compaction_result.output_path.c_str(),
                 output_files_oss.str().c_str());

  // 如果序列化过程中存在错误，记录错误状态并返回失败。
  if (!s.ok()) {
    sub_compact->status = s;
    return CompactionServiceJobStatus::kFailure;
  }

  // 遍历压缩结果中的输出文件：
  // - 生成新文件号，并构建目标文件路径。
  // - 重命名源文件到目标文件路径。
  // - 获取文件大小，并填充 FileMetaData 结构体。
  // - 将文件元数据添加到当前的压缩任务输出中。
  for (const auto& file : compaction_result.output_files) {
    uint64_t file_num = versions_->NewFileNumber();
    auto src_file = compaction_result.output_path + "/" + file.file_name;
    auto tgt_file = TableFileName(compaction->immutable_options()->cf_paths,
                                  file_num, compaction->output_path_id());
    s = fs_->RenameFile(src_file, tgt_file, IOOptions(), nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }

    FileMetaData meta;
    uint64_t file_size;
    s = fs_->GetFileSize(tgt_file, IOOptions(), &file_size, nullptr);
    if (!s.ok()) {
      sub_compact->status = s;
      return CompactionServiceJobStatus::kFailure;
    }
    meta.fd = FileDescriptor(file_num, compaction->output_path_id(), file_size,
                             file.smallest_seqno, file.largest_seqno);
    meta.smallest.DecodeFrom(file.smallest_internal_key);
    meta.largest.DecodeFrom(file.largest_internal_key);
    meta.oldest_ancester_time = file.oldest_ancester_time;
    meta.file_creation_time = file.file_creation_time;
    meta.epoch_number = file.epoch_number;
    meta.marked_for_compaction = file.marked_for_compaction;
    meta.unique_id = file.unique_id;

    auto cfd = compaction->column_family_data();
    sub_compact->Current().AddOutput(std::move(meta),
                                     cfd->internal_comparator(), false, true,
                                     file.paranoid_hash);
  }

  // 更新压缩任务的统计信息，包括读取和写入的字节数，以及输出记录数和总字节数。
  sub_compact->compaction_job_stats = compaction_result.stats;
  sub_compact->Current().SetNumOutputRecords(
      compaction_result.num_output_records);
  sub_compact->Current().SetTotalBytes(compaction_result.total_bytes);
  RecordTick(stats_, REMOTE_COMPACT_READ_BYTES, compaction_result.bytes_read);
  RecordTick(stats_, REMOTE_COMPACT_WRITE_BYTES,
             compaction_result.bytes_written);
  
  // 加的代码
  RecordTimeToHistogram(stats_, REMOTE_COMPACT_PROCESS,
                        compaction_addition_info.trigger_ms);
  RecordTimeToHistogram(stats_, REMOTE_COMPACT_OPEN_DB_DELAY,
                        compaction_addition_info.num_entries);
  
  // 表示压缩任务成功完成。
  return CompactionServiceJobStatus::kSuccess;
}

// 生成完整的表文件名，结合输出路径和文件编号。
std::string CompactionServiceCompactionJob::GetTableFileName(
    uint64_t file_number) {
  return MakeTableFileName(output_path_, file_number);
}

// 记录压缩过程中读取和写入的字节数。
void CompactionServiceCompactionJob::RecordCompactionIOStats() {
  compaction_result_->bytes_read += IOSTATS(bytes_read);
  compaction_result_->bytes_written += IOSTATS(bytes_written);
  CompactionJob::RecordCompactionIOStats();
}

// 初始化 CompactionServiceCompactionJob 对象，设置所有必要的参数以执行压缩任务。
CompactionServiceCompactionJob::CompactionServiceCompactionJob(
    int job_id, Compaction* compaction, const ImmutableDBOptions& db_options,
    const MutableDBOptions& mutable_db_options, const FileOptions& file_options,
    VersionSet* versions, const std::atomic<bool>* shutting_down,
    LogBuffer* log_buffer, FSDirectory* output_directory, Statistics* stats,
    InstrumentedMutex* db_mutex, ErrorHandler* db_error_handler,
    std::vector<SequenceNumber> existing_snapshots,
    std::shared_ptr<Cache> table_cache, EventLogger* event_logger,
    const std::string& dbname, const std::shared_ptr<IOTracer>& io_tracer,
    const std::atomic<bool>& manual_compaction_canceled,
    const std::string& db_id, const std::string& db_session_id,
    std::string output_path,
    const CompactionServiceInput& compaction_service_input,
    CompactionServiceResult* compaction_service_result)
    : CompactionJob(
          job_id, compaction, db_options, mutable_db_options, file_options,
          versions, shutting_down, log_buffer, nullptr, output_directory,
          nullptr, stats, db_mutex, db_error_handler,
          std::move(existing_snapshots), kMaxSequenceNumber, nullptr, nullptr,
          std::move(table_cache), event_logger,
          compaction->mutable_cf_options()->paranoid_file_checks,
          compaction->mutable_cf_options()->report_bg_io_stats, dbname,
          &(compaction_service_result->stats), Env::Priority::USER, io_tracer,
          manual_compaction_canceled, db_id, db_session_id,
          compaction->column_family_data()->GetFullHistoryTsLow()),
      output_path_(std::move(output_path)),
      compaction_input_(compaction_service_input),
      compaction_result_(compaction_service_result) {}

// 这是 CompactionJob 的重载方法，负责执行具体的压缩任务。
// 执行一个压缩任务，主要步骤包括准备输入数据、调度任务、等待结果、处理输出文件、
// 记录统计信息以及最终返回状态。
// 通过与 CompactionService 的集成，它允许压缩任务在外部服务中执行，
// 从而实现更灵活和可扩展的压缩策略。
Status CompactionServiceCompactionJob::Run() {

  // 更新线程的操作阶段，便于监控和调试。
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_COMPACTION_RUN);

  // 确保列族数据和当前压缩层级中存在文件，避免进行无效的压缩。
  auto* c = compact_->compaction;
  assert(c->column_family_data() != nullptr);
  assert(c->column_family_data()->current()->storage_info()->NumLevelFiles(
             compact_->compaction->level()) > 0);

  // 根据输出层级计算 SST 文件的写入提示，确定压缩目标的最底层级。
  write_hint_ =
      c->column_family_data()->CalculateSSTWriteHint(c->output_level());
  bottommost_level_ = c->bottommost_level();

  // 创建并初始化 SubcompactionState，包含压缩上下文和范围信息。
  Slice begin = compaction_input_.begin;
  Slice end = compaction_input_.end;
  compact_->sub_compact_states.emplace_back(
      c,
      compaction_input_.has_begin ? std::optional<Slice>(begin)
                                  : std::optional<Slice>(),
      compaction_input_.has_end ? std::optional<Slice>(end)
                                : std::optional<Slice>(),
      /*sub_job_id*/ 0);

  // 刷新日志缓冲区，记录压缩信息，并开始计时。
  log_buffer_->FlushBufferToLog();
  LogCompaction();
  const uint64_t start_micros = db_options_.clock->NowMicros();
  // Pick the only sub-compaction we should have
  assert(compact_->sub_compact_states.size() == 1);

  // 取得唯一的子压缩状态指针
  SubcompactionState* sub_compact = compact_->sub_compact_states.data();

  // 调用自定义的压缩处理方法
  // 调用前面详细解析的 ProcessKeyValueCompactionWithCompactionService 方法，执行压缩任务。
  ProcessKeyValueCompaction(sub_compact);

  // 计算压缩任务的总时间和 CPU 使用时间，并记录到统计信息中。
  compaction_stats_.stats.micros =
      db_options_.clock->NowMicros() - start_micros;
  compaction_stats_.stats.cpu_micros =
      sub_compact->compaction_job_stats.cpu_micros;

  RecordTimeToHistogram(stats_, COMPACTION_TIME,
                        compaction_stats_.stats.micros);
  RecordTimeToHistogram(stats_, COMPACTION_CPU_TIME,
                        compaction_stats_.stats.cpu_micros);

  // 检查并处理压缩任务的状态和 I/O 操作的错误。
  // 如果没有错误，则执行目录同步操作（如 fsync），确保数据持久化。
  Status status = sub_compact->status;
  IOStatus io_s = sub_compact->io_status;

  if (io_status_.ok()) {
    io_status_ = io_s;
  }

  if (status.ok()) {
    constexpr IODebugContext* dbg = nullptr;

    if (output_directory_) {
      io_s = output_directory_->FsyncWithDirOptions(IOOptions(), dbg,
                                                    DirFsyncOptions());
    }
  }
  if (io_status_.ok()) {
    io_status_ = io_s;
  }
  if (status.ok()) {
    status = io_s;
  }
  if (status.ok()) {
    // TODO: Add verify_table()
  }

  // Finish up all book-keeping to unify the subcompaction results
  // 聚合压缩统计信息，更新全局压缩统计，并记录 I/O 统计信息。
  compact_->AggregateCompactionStats(compaction_stats_, *compaction_job_stats_);
  UpdateCompactionStats();
  RecordCompactionIOStats();

  // 刷新日志，设置压缩任务的最终状态.
  LogFlush(db_options_.info_log);
  compact_->status = status;
  compact_->status.PermitUncheckedError();

  // Build compaction result
  // 填充 CompactionServiceResult，包括输出层级、输出路径、输出文件列表以及记录数和总字节数。
  compaction_result_->output_level = compact_->compaction->output_level();
  compaction_result_->output_path = output_path_;
  for (const auto& output_file : sub_compact->GetOutputs()) {
    auto& meta = output_file.meta;
    compaction_result_->output_files.emplace_back(
        MakeTableFileName(meta.fd.GetNumber()), meta.fd.smallest_seqno,
        meta.fd.largest_seqno, meta.smallest.Encode().ToString(),
        meta.largest.Encode().ToString(), meta.oldest_ancester_time,
        meta.file_creation_time, meta.epoch_number,
        output_file.validator.GetHash(), meta.marked_for_compaction,
        meta.unique_id);
  }
  InternalStats::CompactionStatsFull compaction_stats;
  sub_compact->AggregateCompactionStats(compaction_stats);
  compaction_result_->num_output_records =
      compaction_stats.stats.num_output_records;
  compaction_result_->total_bytes = compaction_stats.TotalBytesWritten();
  
  // 返回最终的压缩任务状态。
  return status;
}

void CompactionServiceCompactionJob::CleanupCompaction() {
  CompactionJob::CleanupCompaction();
}

// Internal binary format for the input and result data
enum BinaryFormatVersion : uint32_t {
  kOptionsString = 1,  // Use string format similar to Option string format
};

// 这些映射定义了如何处理结构体中的每个字段，包括字段名称、偏移量、数据类型、验证类型和标志。
static std::unordered_map<std::string, OptionTypeInfo> cfd_type_info = {
    {"name",
     {offsetof(struct ColumnFamilyDescriptor, name), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"options",
     {offsetof(struct ColumnFamilyDescriptor, options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto cf_options = static_cast<ColumnFamilyOptions*>(addr);
        return GetColumnFamilyOptionsFromString(opts, ColumnFamilyOptions(),
                                                value, cf_options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto cf_options = static_cast<const ColumnFamilyOptions*>(addr);
        std::string result;
        auto status =
            GetStringFromColumnFamilyOptions(opts, *cf_options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const ColumnFamilyOptions*>(addr1);
        const auto that_one = static_cast<const ColumnFamilyOptions*>(addr2);
        auto this_conf = CFOptionsAsConfigurable(*this_one);
        auto that_conf = CFOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_input_type_info = {
    {"column_family",
     OptionTypeInfo::Struct(
         "column_family", &cfd_type_info,
         offsetof(struct CompactionServiceInput, column_family),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
    {"db_options",
     {offsetof(struct CompactionServiceInput, db_options),
      OptionType::kConfigurable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto options = static_cast<DBOptions*>(addr);
        return GetDBOptionsFromString(opts, DBOptions(), value, options);
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto options = static_cast<const DBOptions*>(addr);
        std::string result;
        auto status = GetStringFromDBOptions(opts, *options, &result);
        *value = "{" + result + "}";
        return status;
      },
      [](const ConfigOptions& opts, const std::string& name, const void* addr1,
         const void* addr2, std::string* mismatch) {
        const auto this_one = static_cast<const DBOptions*>(addr1);
        const auto that_one = static_cast<const DBOptions*>(addr2);
        auto this_conf = DBOptionsAsConfigurable(*this_one);
        auto that_conf = DBOptionsAsConfigurable(*that_one);
        std::string mismatch_opt;
        bool result =
            this_conf->AreEquivalent(opts, that_conf.get(), &mismatch_opt);
        if (!result) {
          *mismatch = name + "." + mismatch_opt;
        }
        return result;
      }}},
    {"snapshots", OptionTypeInfo::Vector<uint64_t>(
                      offsetof(struct CompactionServiceInput, snapshots),
                      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                      {0, OptionType::kUInt64T})},
    {"input_files", OptionTypeInfo::Vector<std::string>(
                        offsetof(struct CompactionServiceInput, input_files),
                        OptionVerificationType::kNormal, OptionTypeFlags::kNone,
                        {0, OptionType::kEncodedString})},
    {"output_level",
     {offsetof(struct CompactionServiceInput, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"db_id",
     {offsetof(struct CompactionServiceInput, db_id),
      OptionType::kEncodedString}},
    {"has_begin",
     {offsetof(struct CompactionServiceInput, has_begin), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"begin",
     {offsetof(struct CompactionServiceInput, begin),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"has_end",
     {offsetof(struct CompactionServiceInput, has_end), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"end",
     {offsetof(struct CompactionServiceInput, end), OptionType::kEncodedString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    cs_output_file_type_info = {
        {"file_name",
         {offsetof(struct CompactionServiceOutputFile, file_name),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_seqno",
         {offsetof(struct CompactionServiceOutputFile, smallest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_seqno",
         {offsetof(struct CompactionServiceOutputFile, largest_seqno),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, smallest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_internal_key",
         {offsetof(struct CompactionServiceOutputFile, largest_internal_key),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"oldest_ancester_time",
         {offsetof(struct CompactionServiceOutputFile, oldest_ancester_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_creation_time",
         {offsetof(struct CompactionServiceOutputFile, file_creation_time),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"epoch_number",
         {offsetof(struct CompactionServiceOutputFile, epoch_number),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"paranoid_hash",
         {offsetof(struct CompactionServiceOutputFile, paranoid_hash),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"marked_for_compaction",
         {offsetof(struct CompactionServiceOutputFile, marked_for_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"unique_id",
         OptionTypeInfo::Array<uint64_t, 2>(
             offsetof(struct CompactionServiceOutputFile, unique_id),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kUInt64T})},
};

static std::unordered_map<std::string, OptionTypeInfo>
    compaction_job_stats_type_info = {
        {"elapsed_micros",
         {offsetof(struct CompactionJobStats, elapsed_micros),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cpu_micros",
         {offsetof(struct CompactionJobStats, cpu_micros), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"num_input_records",
         {offsetof(struct CompactionJobStats, num_input_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_blobs_read",
         {offsetof(struct CompactionJobStats, num_blobs_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files",
         {offsetof(struct CompactionJobStats, num_input_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_files_at_output_level",
         {offsetof(struct CompactionJobStats, num_input_files_at_output_level),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_records",
         {offsetof(struct CompactionJobStats, num_output_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files",
         {offsetof(struct CompactionJobStats, num_output_files),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_output_files_blob",
         {offsetof(struct CompactionJobStats, num_output_files_blob),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_full_compaction",
         {offsetof(struct CompactionJobStats, is_full_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"is_manual_compaction",
         {offsetof(struct CompactionJobStats, is_manual_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_bytes",
         {offsetof(struct CompactionJobStats, total_input_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_blob_bytes_read",
         {offsetof(struct CompactionJobStats, total_blob_bytes_read),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes",
         {offsetof(struct CompactionJobStats, total_output_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_output_bytes_blob",
         {offsetof(struct CompactionJobStats, total_output_bytes_blob),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_records_replaced",
         {offsetof(struct CompactionJobStats, num_records_replaced),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_key_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_key_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"total_input_raw_value_bytes",
         {offsetof(struct CompactionJobStats, total_input_raw_value_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_input_deletion_records",
         {offsetof(struct CompactionJobStats, num_input_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_expired_deletion_records",
         {offsetof(struct CompactionJobStats, num_expired_deletion_records),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_corrupt_keys",
         {offsetof(struct CompactionJobStats, num_corrupt_keys),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_write_nanos",
         {offsetof(struct CompactionJobStats, file_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_range_sync_nanos",
         {offsetof(struct CompactionJobStats, file_range_sync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_fsync_nanos",
         {offsetof(struct CompactionJobStats, file_fsync_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"file_prepare_write_nanos",
         {offsetof(struct CompactionJobStats, file_prepare_write_nanos),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"smallest_output_key_prefix",
         {offsetof(struct CompactionJobStats, smallest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"largest_output_key_prefix",
         {offsetof(struct CompactionJobStats, largest_output_key_prefix),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_fallthru",
         {offsetof(struct CompactionJobStats, num_single_del_fallthru),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"num_single_del_mismatch",
         {offsetof(struct CompactionJobStats, num_single_del_mismatch),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

namespace {
// this is a helper struct to serialize and deserialize class Status, because
// Status's members are not public.
// 由于 Status 类的内部成员不可见，这个适配器结构体用于将 Status 对象序列化和反序列化。
struct StatusSerializationAdapter {
  uint8_t code;
  uint8_t subcode;
  uint8_t severity;
  std::string message;

  StatusSerializationAdapter() = default;
  explicit StatusSerializationAdapter(const Status& s) {
    code = s.code();
    subcode = s.subcode();
    severity = s.severity();
    auto msg = s.getState();
    message = msg ? msg : "";
  }

  Status GetStatus() const {
    return Status{static_cast<Status::Code>(code),
                  static_cast<Status::SubCode>(subcode),
                  static_cast<Status::Severity>(severity), message};
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo>
    status_adapter_type_info = {
        {"code",
         {offsetof(struct StatusSerializationAdapter, code),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"subcode",
         {offsetof(struct StatusSerializationAdapter, subcode),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"severity",
         {offsetof(struct StatusSerializationAdapter, severity),
          OptionType::kUInt8T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"message",
         {offsetof(struct StatusSerializationAdapter, message),
          OptionType::kEncodedString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

static std::unordered_map<std::string, OptionTypeInfo> cs_result_type_info = {
    {"status",
     {offsetof(struct CompactionServiceResult, status),
      OptionType::kCustomizable, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const std::string& value, void* addr) {
        auto status_obj = static_cast<Status*>(addr);
        StatusSerializationAdapter adapter;
        Status s = OptionTypeInfo::ParseType(
            opts, value, status_adapter_type_info, &adapter);
        *status_obj = adapter.GetStatus();
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr, std::string* value) {
        const auto status_obj = static_cast<const Status*>(addr);
        StatusSerializationAdapter adapter(*status_obj);
        std::string result;
        Status s = OptionTypeInfo::SerializeType(opts, status_adapter_type_info,
                                                 &adapter, &result);
        *value = "{" + result + "}";
        return s;
      },
      [](const ConfigOptions& opts, const std::string& /*name*/,
         const void* addr1, const void* addr2, std::string* mismatch) {
        const auto status1 = static_cast<const Status*>(addr1);
        const auto status2 = static_cast<const Status*>(addr2);

        StatusSerializationAdapter adatper1(*status1);
        StatusSerializationAdapter adapter2(*status2);
        return OptionTypeInfo::TypesAreEqual(opts, status_adapter_type_info,
                                             &adatper1, &adapter2, mismatch);
      }}},
    {"output_files",
     OptionTypeInfo::Vector<CompactionServiceOutputFile>(
         offsetof(struct CompactionServiceResult, output_files),
         OptionVerificationType::kNormal, OptionTypeFlags::kNone,
         OptionTypeInfo::Struct("output_files", &cs_output_file_type_info, 0,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone))},
    {"output_level",
     {offsetof(struct CompactionServiceResult, output_level), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"output_path",
     {offsetof(struct CompactionServiceResult, output_path),
      OptionType::kEncodedString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"num_output_records",
     {offsetof(struct CompactionServiceResult, num_output_records),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"total_bytes",
     {offsetof(struct CompactionServiceResult, total_bytes),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_read",
     {offsetof(struct CompactionServiceResult, bytes_read),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"bytes_written",
     {offsetof(struct CompactionServiceResult, bytes_written),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"stats", OptionTypeInfo::Struct(
                  "stats", &compaction_job_stats_type_info,
                  offsetof(struct CompactionServiceResult, stats),
                  OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
};

// CompactionServiceInput 和 CompactionServiceResult 的序列化与反序列化
// 这两个方法用于将 CompactionServiceInput 和 CompactionServiceResult 
// 结构体序列化为二进制字符串，并反序列化回结构体对象。
// 这对于在 CompactionService 和 CompactionJob 之间传输数据非常重要。
Status CompactionServiceInput::Read(const std::string& data_str,
                                    CompactionServiceInput* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceInput string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_input_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Input data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceInput::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_input_type_info, this, output);
}

Status CompactionServiceResult::Read(const std::string& data_str,
                                     CompactionServiceResult* obj) {
  if (data_str.size() <= sizeof(BinaryFormatVersion)) {
    return Status::InvalidArgument("Invalid CompactionServiceResult string");
  }
  auto format_version = DecodeFixed32(data_str.data());
  if (format_version == kOptionsString) {
    ConfigOptions cf;
    cf.invoke_prepare_options = false;
    cf.ignore_unknown_options = true;
    return OptionTypeInfo::ParseType(
        cf, data_str.substr(sizeof(BinaryFormatVersion)), cs_result_type_info,
        obj);
  } else {
    return Status::NotSupported(
        "Compaction Service Result data version not supported: " +
        std::to_string(format_version));
  }
}

Status CompactionServiceResult::Write(std::string* output) {
  char buf[sizeof(BinaryFormatVersion)];
  EncodeFixed32(buf, kOptionsString);
  output->append(buf, sizeof(BinaryFormatVersion));
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::SerializeType(cf, cs_result_type_info, this, output);
}

// 仅在调试模式下（非 NDEBUG 构建）定义的测试辅助方法，
// 用于比较 CompactionServiceInput 和 CompactionServiceResult 对象是否相等。
#ifndef NDEBUG
bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceResult::TEST_Equals(CompactionServiceResult* other,
                                          std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_result_type_info, this, other,
                                       mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other) {
  std::string mismatch;
  return TEST_Equals(other, &mismatch);
}

bool CompactionServiceInput::TEST_Equals(CompactionServiceInput* other,
                                         std::string* mismatch) {
  ConfigOptions cf;
  cf.invoke_prepare_options = false;
  return OptionTypeInfo::TypesAreEqual(cf, cs_input_type_info, this, other,
                                       mismatch);
}
#endif  // NDEBUG
}  // namespace ROCKSDB_NAMESPACE

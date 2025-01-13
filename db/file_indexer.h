//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <cstdint>
#include <functional>
#include <limits>
#include <vector>

#include "memory/arena.h"
#include "port/port.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Comparator;
struct FileMetaData;
struct FdWithKeyRange;
struct FileLevel;

// The file tree structure in Version is prebuilt and the range of each file
// is known. On Version::Get(), it uses binary search to find a potential file
// and then check if a target key can be found in the file by comparing the key
// to each file's smallest and largest key. The results of these comparisons
// can be reused beyond checking if a key falls into a file's range.
// With some pre-calculated knowledge, each key comparison that has been done
// can serve as a hint to narrow down further searches: if a key compared to
// be smaller than a file's smallest or largest, that comparison can be used
// to find out the right bound of next binary search. Similarly, if a key
// compared to be larger than a file's smallest or largest, it can be utilized
// to find out the left bound of next binary search.
// With these hints: it can greatly reduce the range of binary search,
// especially for bottom levels, given that one file most likely overlaps with
// only N files from level below (where N is max_bytes_for_level_multiplier).
// So on level L, we will only look at ~N files instead of N^L files on the
// naive approach.
class FileIndexer {
 public:
  explicit FileIndexer(const Comparator* ucmp);

  size_t NumLevelIndex() const;

  size_t LevelIndexSize(size_t level) const;

  // Return a file index range in the next level to search for a key based on
  // smallest and largest key comparison for the current file specified by
  // level and file_index. When *left_index < *right_index, both index should
  // be valid and fit in the vector size.
  void GetNextLevelIndex(const size_t level, const size_t file_index,
                         const int cmp_smallest, const int cmp_largest,
                         int32_t* left_bound, int32_t* right_bound) const;

  void UpdateIndex(Arena* arena, const size_t num_levels,
                   std::vector<FileMetaData*>* const files);

  enum { kLevelMaxIndex = std::numeric_limits<int32_t>::max() };

 private:
  size_t num_levels_;
  const Comparator* ucmp_;

  struct IndexUnit {
    IndexUnit()
        : smallest_lb(0), largest_lb(0), smallest_rb(-1), largest_rb(-1) {}
    // During file search, a key is compared against smallest and largest
    // from a FileMetaData. It can have 3 possible outcomes:
    // (1) key is smaller than smallest, implying it is also smaller than
    //     larger. Precalculated index based on "smallest < smallest" can
    //     be used to provide right bound.
    // (2) key is in between smallest and largest.
    //     Precalculated index based on "smallest > greatest" can be used to
    //     provide left bound.
    //     Precalculated index based on "largest < smallest" can be used to
    //     provide right bound.
    // (3) key is larger than largest, implying it is also larger than smallest.
    //     Precalculated index based on "largest > largest" can be used to
    //     provide left bound.
    //
    // As a result, we will need to do:
    // Compare smallest (<=) and largest keys from upper level file with
    // smallest key from lower level to get a right bound.
    // Compare smallest (>=) and largest keys from upper level file with
    // largest key from lower level to get a left bound.
    //
    // Example:
    //    level 1:              [50 - 60]
    //    level 2:        [1 - 40], [45 - 55], [58 - 80]
    // A key 35, compared to be less than 50, 3rd file on level 2 can be
    // skipped according to rule (1). LB = 0, RB = 1.
    // A key 53, sits in the middle 50 and 60. 1st file on level 2 can be
    // skipped according to rule (2)-a, but the 3rd file cannot be skipped
    // because 60 is greater than 58. LB = 1, RB = 2.
    // A key 70, compared to be larger than 60. 1st and 2nd file can be skipped
    // according to rule (3). LB = 2, RB = 2.
    //


    // 在文件搜索期间，将键与最小和最大进行比较
    // 来自文件元数据。它可能有 3 种可能的结果：
    // (1) key 小于smallest，意味着它也小于更大。根据“最小<最小”预先计算的索引可以用于提供右界。
    // (2) key 介于最小和最大之间。
    //     基于“最小 > 最大”的预先计算的索引可用于提供左边界。
    //     基于“最大 < 最小”的预先计算的索引可用于提供右界。
    // (3) 键大于最大，意味着它也大于最小。基于“最大>最大”的预先计算的索引可用于提供左边界。
    //
    // 因此，我们需要做：
    // 将上层文件中的最小 (<=) 和最大键与较低级别的最小键以获得右边界。
    // 将上层文件中的最小 (>=) 和最大键与下层最大的键以获得左边界。
    //
    // 例子：
    //    level 1：[50 -60]
    //    level 2：[1 -40]、[45 -55]、[58 -80]

    // level 1 的 smallest 为 50，largest 为 60
    // smallest_lb 指下一级中恰好包含比 smallest 大的 sstable，故为 1，即 [45, 55]。
    // smallest_rb 指下一级中恰好包含比 smallest 小的 sstable，故为 1，即 [45, 55]。
    // largest_lb 指下一级中恰好包含比 largest 大的 sstable，故为 2，即 [58, 80]。
    // largest_rb 指下一级中恰好包含比 largest 小的 sstable，故为 2，即 [58, 80]。

    // 在 level1 中查找 key 35，没有找到，且在 sstable 左边，因此需要跳到 level2 中的 0 ~ smallest_lb 中去找，即sstable0 和 sstable 1。
    // key 53 没有找到，且在 sstable 内部，因此需要跳到 level2 的重合区间去找，故范围为 smallest_lb ~ largest_rb，即 sstable1 和 sstable 2。 
    // key70 没找到，且在 sstable 右边，因此需要跳到 level2 中的 largest_lb ~ 右边界 中去找，即 sstable2 本身。

    // key 35，相比之下小于50，根据规则（1）可以跳过level 2的第三个sst。 LB = 0，RB = 1。

    // key 53，位于 50 和 60 中间。根据规则(2)-a可以跳过level 2的第一个文件。
    // 因为 60 大于 58导致level 2 的第3个文件不能跳过。LB = 1，RB = 2。

    // key 70，相比大于60。根据规则（3）level 2的第一个和第二个文件可以跳过。LB = 2，RB = 2。

    // Point to a left most file in a lower level that may contain a key,
    // which compares greater than smallest of a FileMetaData (upper level)
    int32_t smallest_lb;
    // Point to a left most file in a lower level that may contain a key,
    // which compares greater than largest of a FileMetaData (upper level)
    int32_t largest_lb;
    // Point to a right most file in a lower level that may contain a key,
    // which compares smaller than smallest of a FileMetaData (upper level)
    int32_t smallest_rb;
    // Point to a right most file in a lower level that may contain a key,
    // which compares smaller than largest of a FileMetaData (upper level)
    int32_t largest_rb;
  };

  // Data structure to store IndexUnits in a whole level
  struct IndexLevel {
    size_t num_index;
    IndexUnit* index_units;

    IndexLevel() : num_index(0), index_units(nullptr) {}
  };

  void CalculateLB(
      const std::vector<FileMetaData*>& upper_files,
      const std::vector<FileMetaData*>& lower_files, IndexLevel* index_level,
      std::function<int(const FileMetaData*, const FileMetaData*)> cmp_op,
      std::function<void(IndexUnit*, int32_t)> set_index);

  void CalculateRB(
      const std::vector<FileMetaData*>& upper_files,
      const std::vector<FileMetaData*>& lower_files, IndexLevel* index_level,
      std::function<int(const FileMetaData*, const FileMetaData*)> cmp_op,
      std::function<void(IndexUnit*, int32_t)> set_index);

  autovector<IndexLevel> next_level_index_;
  int32_t* level_rb_;
};

}  // namespace ROCKSDB_NAMESPACE

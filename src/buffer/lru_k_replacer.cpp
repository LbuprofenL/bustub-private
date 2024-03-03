//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : max_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);

  auto current_time = std::chrono::system_clock::now();
  auto duration = current_time.time_since_epoch();
  auto current_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  std::vector<std::pair<frame_id_t, size_t>> vector;
  vector.reserve(curr_size_);
  for (auto &pair : node_store_) {
    auto k_dist = current_timestamp - pair.second.history_.back();
    vector.emplace_back(pair.first, k_dist);
  }

  std::sort(vector.begin(), vector.end(), [](const auto &lhs, const auto &rhs) { return lhs.second > rhs.second; });
  for (auto &pair : vector) {
    if (node_store_[pair.first].history_.size() != k_) {
      pair.second = SIZE_MAX;
    }
  }
  std::stable_sort(vector.begin(), vector.end(),
                   [](const auto &lhs, const auto &rhs) { return lhs.second > rhs.second; });

  frame_id_t result = INVALID_PAGE_ID;
  for (const auto &pair : vector) {
    auto is_evictable = node_store_[pair.first].is_evictable_;
    if (is_evictable) {
      result = pair.first;
      break;
    }
  }

  if (result == INVALID_PAGE_ID) {
    return false;
  }

  // Remove(result);
  node_store_.erase(result);
  evictable_node_.erase(result);
  // only evictable frame can be removed
  replacer_size_--;
  curr_size_--;
  *frame_id = result;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::scoped_lock<std::mutex> lck(latch_);

  auto current_time = std::chrono::system_clock::now();
  auto duration = current_time.time_since_epoch();
  current_timestamp_ = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  if (curr_size_ == 0 || node_store_.count(frame_id) == 0) {
    // first insert
    //    curr_size_++;
    //
    //    auto new_node = LRUKNode();
    //    new_node.fid_ = frame_id;
    //    new_node.k_ = k_;
    //
    //    new_node.history_.push_front(current_timestamp_);
    //
    //    node_store_[frame_id] = new_node;
    AddFrames(frame_id);
  } else {
    // not first access
    node_store_[frame_id].history_.push_front(current_timestamp_);
    if (node_store_[frame_id].history_.size() > k_) {
      node_store_[frame_id].history_.pop_back();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lck(latch_);
  if (node_store_.count(frame_id) == 0) {
    throw Exception("frame id is invalid");
  }

  auto is_evictable = node_store_[frame_id].is_evictable_;

  if (set_evictable == is_evictable) {
    return;
  }
  if (set_evictable && !is_evictable) {
    //    replacer_size_++;
    //    if (replacer_size_ > max_size_) {
    //      throw Exception("too many evictable frames");
    //    }
    //    evictable_node_[frame_id] = node_store_[frame_id];
    EnableEvictable(frame_id);
  }

  if (!set_evictable && is_evictable) {
    //    replacer_size_--;
    //
    //    evictable_node_.erase(frame_id);
    DisableEvictable(frame_id);
  }
  //  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lck(latch_);
  if (node_store_.count(frame_id) == 0) {
    return;
  }

  if (!node_store_[frame_id].is_evictable_) {
    throw Exception("non-evictable frame");
  }

  node_store_.erase(frame_id);
  evictable_node_.erase(frame_id);
  // only evictable frame can be removed
  replacer_size_--;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lck(latch_);
  auto size = replacer_size_;
  return size;
}

void LRUKReplacer::AddFrames(frame_id_t frame_id) {
  curr_size_++;

  auto new_node = LRUKNode();
  new_node.fid_ = frame_id;
  new_node.k_ = k_;

  new_node.history_.push_front(current_timestamp_);

  node_store_[frame_id] = new_node;
}

void LRUKReplacer::EnableEvictable(frame_id_t frame_id) {
  replacer_size_++;
  if (replacer_size_ > max_size_) {
    throw Exception("too many evictable frames");
  }
  node_store_[frame_id].is_evictable_ = true;
  evictable_node_[frame_id] = node_store_[frame_id];
}

void LRUKReplacer::DisableEvictable(frame_id_t frame_id) {
  replacer_size_--;
  node_store_[frame_id].is_evictable_ = false;

  evictable_node_.erase(frame_id);
}
}  // namespace bustub

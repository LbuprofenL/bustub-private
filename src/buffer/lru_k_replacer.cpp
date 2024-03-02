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
  this->latch_.lock();

  auto current_time = std::chrono::system_clock::now();
  auto duration = current_time.time_since_epoch();
  auto current_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  std::vector<std::pair<frame_id_t, size_t>> vector;
  vector.reserve(this->curr_size_);
  for (auto &pair : this->node_store_) {
    auto k_dist = current_timestamp - pair.second.history_.back();
    vector.emplace_back(pair.first, k_dist);
  }

  std::sort(vector.begin(), vector.end(), [](const auto &lhs, const auto &rhs) { return lhs.second > rhs.second; });
  for (auto &pair : vector) {
    if (this->node_store_[pair.first].history_.size()!=this->k_){
      pair.second = SIZE_MAX;
    }
  }
  std::stable_sort(vector.begin(), vector.end(), [](const auto &lhs, const auto &rhs) { return lhs.second > rhs.second; });

  frame_id_t result = INVALID_PAGE_ID;
  for (const auto &pair : vector) {
    auto is_evictable = this->node_store_[pair.first].is_evictable_;
    if (is_evictable) {
      result = pair.first;
      break;
    }
  }

  if (result == INVALID_PAGE_ID) {
    this->latch_.unlock();
    return false;
  }

  *frame_id = result;
  this->node_store_.erase(*frame_id);
  this->replacer_size_--;
  this->curr_size_--;
  this->latch_.unlock();

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  this->latch_.lock();

  auto current_time = std::chrono::system_clock::now();
  auto duration = current_time.time_since_epoch();
  this->current_timestamp_ = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

  if (this->curr_size_ == 0 || this->node_store_.count(frame_id) == 0) {
    // first insert
    this->curr_size_++;

    auto new_node = LRUKNode();
    new_node.fid_ = frame_id;
    new_node.k_ = this->k_;

    new_node.history_.push_front(this->current_timestamp_);

    this->node_store_[frame_id] = new_node;
  } else {
    // not first access
    this->node_store_[frame_id].history_.push_front(this->current_timestamp_);
    if(this->node_store_[frame_id].history_.size()> this->k_){
      this->node_store_[frame_id].history_.pop_back();
    }
  }
  this->latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  this->latch_.lock();
  if (this->node_store_.count(frame_id) == 0) {
    throw Exception("frame id is invalid");
  }

  auto is_evictable = this->node_store_[frame_id].is_evictable_;
  if (set_evictable && !is_evictable) {
    this->replacer_size_++;
    if (this->replacer_size_ > this->max_size_) {
      throw Exception("too many evictable frames");
    }
  } else if (!set_evictable && is_evictable) {
    this->replacer_size_--;
  }
  this->node_store_[frame_id].is_evictable_ = set_evictable;

  this->latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  this->latch_.lock();
  if (this->node_store_.count(frame_id) != 0) {
    if (!this->node_store_[frame_id].is_evictable_) {
      throw Exception("non-evictable frame");
    }
    this->node_store_[frame_id].history_.clear();
  }
  this->latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  this->latch_.lock();
  auto size = this->replacer_size_;
  this->latch_.unlock();
  return size;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lck(latch_);
  frame_id_t new_frame_id;
  auto new_page_id = AllocatePage();

  auto get = NewFrame(new_page_id, &new_frame_id);
  if (!get) {
    return nullptr;
  }

  // pin the new page
  pages_[new_frame_id].pin_count_++;

  replacer_->RecordAccess(new_frame_id);
  replacer_->SetEvictable(new_frame_id, false);

  page_table_[new_page_id] = new_frame_id;

  *page_id = new_page_id;
  return &pages_[new_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lck(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
  } else {
    auto get = NewFrame(page_id, &frame_id);
    if (!get) {
      return nullptr;
    }
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].RLatch();
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
    future.get();
    pages_[frame_id].RUnlatch();
    page_table_[page_id] = frame_id;
  }
  // pin the new page
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id, access_type);

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  pages_[frame_id].WLatch();
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  future.get();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &p : page_table_) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lck(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  // delete in page_table
  page_table_.erase(page_id);
  // delete in replacer
  replacer_->Remove(frame_id);

  //append this frame in free_list
  free_list_.push_back(frame_id);

  // reset page
  ResetFrame(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::NewFrame(page_id_t page_id, frame_id_t *new_frame_id) -> bool {
  frame_id_t frame_id;
  //TODO change to a while loop in order to enhance concurrency
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto get = replacer_->Evict(&frame_id);
    if (!get) {
      return false;
    }
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
      future.get();
    }
    //remove this frame_id in page_table
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  pages_[frame_id].WLatch();
  ResetFrame(frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].WUnlatch();
  replacer_->RecordAccess(frame_id);

  *new_frame_id = frame_id;
  return true;
};

auto BufferPoolManager::ResetFrame(frame_id_t frame_id) -> bool {
  // TODO do some check
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  return true;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub

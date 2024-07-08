//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <optional>

#include "common/exception.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {
size_t num_threads = 32;  // IO型任务，线程数可以大于CPU两倍

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  for (size_t i = 0; i <= num_threads; i++) {
    // Initiate a threads pool
    background_thread_.emplace_back(
        [&] { StartWorkerThread(); });  //这里是一个匿名函数，&表示以引用形式捕获外部作用域中的变量
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (size_t i = 0; i <= num_threads; i++) {
    request_queue_.Put(std::nullopt);
  }
  cv_.notify_all();
  for (auto &t : background_thread_) {
    if (t.has_value()) {
      t->join();
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
  cv_.notify_one();
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::unique_lock<std::mutex> lck(m_);
    cv_.wait(lck);

    auto r = request_queue_.Get();
    if (r == std::nullopt) {
      return;
    }

    lck.unlock();
    if (r->is_write_) {
      disk_manager_->WritePage(r->page_id_, r->data_);
    } else {
      disk_manager_->ReadPage(r->page_id_, r->data_);
    }
    r->callback_.set_value(true);
  }
}

}  // namespace bustub

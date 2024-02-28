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

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  for (auto &t:background_thread_){
    if (t.has_value()) {
      t->join();
    }
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));

  std::scoped_lock<std::mutex> lck(m_);
  // Spawn the background thread
  background_thread_.emplace_back([&] { StartWorkerThread(); });
}

void DiskScheduler::StartWorkerThread() {
  auto r = request_queue_.Get();
  if (!r.has_value()){
    throw ExecutionException("Empty DiskRequest");
  }

  if (r->is_write_){
    disk_manager_->WritePage(r->page_id_,r->data_);
  } else{
    disk_manager_->ReadPage(r->page_id_,r->data_);
  }

  r->callback_.set_value(true);
}

}  // namespace bustub

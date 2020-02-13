//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

// TODO: NewPage
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it != page_table_.end()) {
    replacer_->Pin(page_it->second);
    // page_it->second == frame_id
    return &pages_[page_it->second];
  }
  // search freelist
  if (!free_list_.empty()) {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = free_frame_id;
    frame_table_[free_frame_id] = page_id;
    pages_[free_frame_id].WLatch();
    disk_manager_->ReadPage(page_id, pages_[free_frame_id].GetData());
    pages_[free_frame_id].WUnlatch();
    // NOTE: unpin to replacer
    replacer_->Unpin(free_frame_id);
    return &pages_[free_frame_id];
  }
  // using replacer
  frame_id_t replaced;
  auto success = replacer_->Victim(&replaced);
  if (!success) {
    // no frame to replace;
    return nullptr;
  }
  pages_[replaced].RLatch();
  // TODO: just for test
//  auto replace_page_id = pages_[replaced].GetPageId();
  auto replace_page_id = frame_table_[replaced];
  if (pages_[replaced].IsDirty()) {
    disk_manager_->WritePage(replace_page_id, pages_[replaced].GetData());
  }
  pages_[replaced].RUnlatch();
  // update page_table_
  auto replace_it = page_table_.find(replace_page_id);
  if (replace_it != page_table_.end()) {
    page_table_.erase(replace_it);
  }
  // update page_table_
  page_table_[page_id] = replaced;
  frame_table_[replaced] = page_id;

  pages_[replaced].WLatch();
//  pages_[replaced]
  disk_manager_->ReadPage(page_id, pages_[replaced].GetData());
  pages_[replaced].WUnlatch();
  return &pages_[replaced];

//  return nullptr;
}

// TODO: dirty needs write to WritePage ?
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return false;
  }
  if (is_dirty) {
    disk_manager_->WritePage(page_id, pages_[page_it->second].GetData());
  }
  replacer_->Unpin(page_it->second);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return false;
  }
  pages_[page_it->second].RLatch();
  disk_manager_->WritePage(page_id, pages_[page_it->second].GetData());
  pages_[page_it->second].RUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard lock(latch_);
  if (!free_list_.empty()) {
    auto free_frame_id = free_list_.front();
    free_list_.pop_front();
    auto new_page_id = disk_manager_->AllocatePage();
    *page_id = new_page_id;
    page_table_[new_page_id]= free_frame_id;
    frame_table_[free_frame_id] = new_page_id;
    return &pages_[free_frame_id];
  }
  if (replacer_->Size() > 0) {
    frame_id_t replaced_frame_id;
    auto ret = replacer_->Victim(&replaced_frame_id);
    if (ret) {
      pages_[replaced_frame_id].RLatch();
      // TODO: just for pass the test
      auto replaced_page_id = frame_table_[replaced_frame_id];
//      auto replaced_page_id = pages_[replaced_frame_id].GetPageId();
      // dirty page write to disk
      if (pages_[replaced_frame_id].IsDirty()) {
        disk_manager_->WritePage(replaced_page_id, pages_[replaced_frame_id].GetData());
      }
      // TODO: no need to deallocate page
//      disk_manager_->DeallocatePage(pages_[replaced_frame_id].GetPageId());
      pages_[replaced_frame_id].RUnlatch();
      // NOTE: important rm victim page from page_table
      page_table_.erase(replaced_page_id);
      auto new_page_id = disk_manager_->AllocatePage();
      *page_id = new_page_id;
      page_table_[new_page_id]= replaced_frame_id;
      frame_table_[replaced_frame_id] = new_page_id;
      return &pages_[replaced_frame_id];
    }
  }
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::lock_guard lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    // not found in page table
    return true;
  }
  auto frame_id = page_it->second;
  pages_[frame_id].WLatch();
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  disk_manager_->DeallocatePage(page_id);
  // TODO: necessary ResetMemory?
  pages_[frame_id].ResetMemory();
  pages_[frame_id].WUnlatch();
  page_table_.erase(page_it);
  frame_table_.erase(frame_id);
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard lock(latch_);
  for (auto it: page_table_) {
    pages_[it.second].RLatch();
    disk_manager_->WritePage(it.first, pages_[it.second].GetData());
    pages_[it.second].RUnlatch();
  }
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include "common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  max_num_pages_ = num_pages;
  header_ = list_.end();
}

ClockReplacer::~ClockReplacer() = default;

// TODO: should victim remove frame?
bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard lock(mu_);
  if (list_.empty()) {
    return false;
  }
  for (auto it = header_; it != list_.end(); it++) {
    auto ref_it = ref_.find(*it);
    if (ref_it != ref_.end()) {
      // ref flag is false
      if (!ref_it->second) {
        *frame_id = *it;
        // update header
        auto tmp = it;
        header_ = ++it;
        list_.erase(tmp);
        return true;
      } else {
        // flip true to false
        ref_[*it] = false;
      }
    }
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard lock(mu_);
  if (*header_ == frame_id) {
    auto it = header_;
    header_++;
    list_.erase(it);
    auto ref_it = ref_.find(frame_id);
    if (ref_it != ref_.end()) {
      ref_.erase(ref_it);
    }
    return;
  }
  auto it = ref_.find(frame_id);
  if (it != ref_.end()) {
    list_.remove_if([frame_id](frame_id_t id) { return frame_id == id;});
    ref_.erase(it);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard lock(mu_);
  auto ref_it = ref_.find(frame_id);
  if (ref_it != ref_.end()) {
    // already in the replacer
    return;
  }
  list_.push_back(frame_id);
  ref_[frame_id] = false;
  if (header_ == list_.end()) {
    header_ = list_.begin();
  }
}

size_t ClockReplacer::Size() {
  std::lock_guard lock(mu_);
  return list_.size();
}

}  // namespace bustub

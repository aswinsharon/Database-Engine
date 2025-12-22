#include "buffer/lru_replacer.h"

namespace minidb {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

bool LRUReplacer::Victim(frame_id_t* frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    if (lru_list_.empty()) {
        return false;
    }
    
    // Remove the least recently used frame (back of the list)
    frame_id_t victim_frame = lru_list_.back();
    lru_list_.pop_back();
    lru_hash_.erase(victim_frame);
    
    *frame_id = victim_frame;
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        // Remove from LRU list since it's now pinned
        lru_list_.erase(it->second);
        lru_hash_.erase(it);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    
    auto it = lru_hash_.find(frame_id);
    if (it != lru_hash_.end()) {
        // Already in the list, move to front (most recently used)
        lru_list_.erase(it->second);
        lru_list_.push_front(frame_id);
        lru_hash_[frame_id] = lru_list_.begin();
    } else {
        // Not in the list, add to front
        lru_list_.push_front(frame_id);
        lru_hash_[frame_id] = lru_list_.begin();
    }
}

size_t LRUReplacer::Size() {
    std::lock_guard<std::mutex> lock(latch_);
    return lru_list_.size();
}

}  // namespace minidb
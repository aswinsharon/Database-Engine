#pragma once

#include "common/config.h"
#include "common/types.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace minidb {

/**
 * LRU (Least Recently Used) replacement policy for buffer pool.
 * Maintains a doubly-linked list of frame IDs ordered by access time.
 * Most recently used frames are at the front, least recently used at the back.
 */
class LRUReplacer {
public:
    /**
     * Constructor
     * @param num_pages maximum number of pages the replacer can hold
     */
    explicit LRUReplacer(size_t num_pages);

    /**
     * Destructor
     */
    ~LRUReplacer() = default;

    // Non-copyable
    LRUReplacer(const LRUReplacer&) = delete;
    LRUReplacer& operator=(const LRUReplacer&) = delete;

    /**
     * Remove the victim frame as defined by the replacement policy
     * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
     * @return true if a victim frame was found, false otherwise
     */
    bool Victim(frame_id_t* frame_id);

    /**
     * Pin a frame, indicating that it should not be victimized until it is unpinned
     * @param frame_id the id of the frame to pin
     */
    void Pin(frame_id_t frame_id);

    /**
     * Unpin a frame, indicating that it can now be victimized
     * @param frame_id the id of the frame to unpin
     */
    void Unpin(frame_id_t frame_id);

    /**
     * Return the number of elements in the replacer that can be victimized
     */
    size_t Size();

private:
    std::mutex latch_;
    
    // Doubly-linked list to maintain LRU order
    std::list<frame_id_t> lru_list_;
    
    // Hash map for O(1) access to list iterators
    std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_hash_;
    
    size_t num_pages_;
};

}  // namespace minidb
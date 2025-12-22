#pragma once

#include "common/config.h"
#include "common/types.h"
#include "storage/disk_manager.h"
#include "storage/page.h"
#include "buffer/lru_replacer.h"
#include <unordered_map>
#include <memory>
#include <mutex>

namespace minidb {

/**
 * BufferPoolManager manages the in-memory page cache.
 * It provides the interface between the storage layer and higher-level components.
 * 
 * Key responsibilities:
 * - Fetch pages from disk into memory
 * - Manage page replacement using LRU policy
 * - Handle dirty page flushing
 * - Provide pin/unpin semantics for page access
 */
class BufferPoolManager {
public:
    /**
     * Constructor
     * @param pool_size number of pages in the buffer pool
     * @param disk_manager pointer to the disk manager
     */
    BufferPoolManager(size_t pool_size, DiskManager* disk_manager);

    /**
     * Destructor - flushes all dirty pages
     */
    ~BufferPoolManager();

    // Non-copyable
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;

    /**
     * Fetch a page from the buffer pool
     * @param page_id id of page to be fetched
     * @return pointer to the requested page, nullptr if not found
     */
    Page* FetchPage(page_id_t page_id);

    /**
     * Unpin a page from the buffer pool
     * @param page_id id of page to be unpinned
     * @param is_dirty true if the page was modified
     * @return true if successful, false otherwise
     */
    bool UnpinPage(page_id_t page_id, bool is_dirty);

    /**
     * Flush a page to disk
     * @param page_id id of page to be flushed
     * @return true if successful, false otherwise
     */
    bool FlushPage(page_id_t page_id);

    /**
     * Create a new page in the buffer pool
     * @param[out] page_id id of created page
     * @return pointer to new page, nullptr if all frames are pinned
     */
    Page* NewPage(page_id_t* page_id);

    /**
     * Delete a page from the buffer pool
     * @param page_id id of page to be deleted
     * @return true if successful, false otherwise
     */
    bool DeletePage(page_id_t page_id);

    /**
     * Flush all dirty pages to disk
     */
    void FlushAllPages();

    /**
     * Get buffer pool statistics
     */
    size_t GetPoolSize() const { return pool_size_; }
    size_t GetFreeFrameCount() const;

private:
    /**
     * Find a free frame in the buffer pool
     * @param[out] frame_id id of the free frame
     * @return true if a free frame was found, false otherwise
     */
    bool FindFreeFrame(frame_id_t* frame_id);

    /**
     * Update page metadata and add to page table
     */
    void UpdatePage(Page* page, page_id_t page_id, frame_id_t frame_id);

private:
    size_t pool_size_;
    DiskManager* disk_manager_;
    
    // Buffer pool frames
    std::unique_ptr<Page[]> pages_;
    
    // Page table: page_id -> frame_id
    std::unordered_map<page_id_t, frame_id_t> page_table_;
    
    // Free frame list
    std::list<frame_id_t> free_list_;
    
    // LRU replacer for page replacement
    std::unique_ptr<LRUReplacer> replacer_;
    
    // Latch for thread safety
    mutable std::mutex latch_;
};

}  // namespace minidb
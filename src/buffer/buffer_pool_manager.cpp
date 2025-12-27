#include "buffer/buffer_pool_manager.h"

#include <cassert>
#include <iostream>

namespace minidb {

/**
 * Constructor - initializes the buffer pool with the specified size
 * @param pool_size Number of pages that can be cached in memory
 * @param disk_manager Pointer to disk manager for page I/O operations
 */
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    // Initialize buffer pool frames
    pages_ = std::make_unique<Page[]>(pool_size_);

    // Initialize free frame list with all available frames
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.push_back(static_cast<frame_id_t>(i));
    }

    // Initialize LRU replacer for page replacement policy
    replacer_ = std::make_unique<LRUReplacer>(pool_size_);
}

/**
 * Destructor - flushes all dirty pages to disk before cleanup
 */
BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
}

/**
 * Fetch a page from the buffer pool or disk
 * If the page is already in memory, increment its pin count and return it.
 * If not in memory, find a free frame, load the page from disk, and return it.
 * @param page_id ID of the page to fetch
 * @return Pointer to the requested page, or nullptr if unable to fetch
 */
Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        // Pin the page and update LRU (remove from replacer since it's now pinned)
        page->IncPinCount();
        replacer_->Pin(frame_id);

        return page;
    }

    // Page not in buffer pool, need to fetch from disk
    frame_id_t frame_id;
    if (!FindFreeFrame(&frame_id)) {
        // No free frame available (all frames are pinned)
        return nullptr;
    }

    Page* page = &pages_[frame_id];

    // Read page from disk
    try {
        disk_manager_->ReadPage(page_id, page->GetData());
    } catch (const std::exception& e) {
        // Return frame to free list on error
        free_list_.push_back(frame_id);
        return nullptr;
    }

    // Update page metadata and page table
    UpdatePage(page, page_id, frame_id);

    return page;
}

/**
 * Unpin a page from the buffer pool
 * Decreases the pin count and marks the page as dirty if specified.
 * When pin count reaches 0, the page becomes eligible for replacement.
 * @param page_id ID of the page to unpin
 * @param is_dirty true if the page was modified since last fetch
 * @return true if successful, false if page not found or not pinned
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool
        return false;
    }

    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    if (page->GetPinCount() <= 0) {
        // Page is not pinned
        return false;
    }

    // Update dirty flag if page was modified
    if (is_dirty) {
        page->SetDirty(true);
    }

    // Decrease pin count
    page->DecPinCount();

    // If pin count reaches 0, add to replacer (eligible for eviction)
    if (page->GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
    }

    return true;
}

/**
 * Flush a specific page to disk
 * Writes the page data to disk and clears the dirty flag
 * @param page_id ID of the page to flush
 * @return true if successful, false if page not found or I/O error
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        // Page not in buffer pool
        return false;
    }

    frame_id_t frame_id = it->second;
    Page* page = &pages_[frame_id];

    // Write page to disk
    try {
        disk_manager_->WritePage(page_id, page->GetData());
        page->SetDirty(false);
    } catch (const std::exception& e) {
        return false;
    }

    return true;
}

/**
 * Create a new page in the buffer pool
 * Allocates a new page ID from disk manager and finds a free frame for it
 * @param[out] page_id Pointer to store the ID of the newly created page
 * @return Pointer to the new page, or nullptr if no free frames available
 */
Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // Find a free frame
    frame_id_t frame_id;
    if (!FindFreeFrame(&frame_id)) {
        return nullptr;
    }

    // Allocate new page ID from disk manager
    *page_id = disk_manager_->AllocatePage();

    Page* page = &pages_[frame_id];

    // Initialize page with clean state
    page->ResetMemory();
    page->SetPageId(*page_id);
    page->SetDirty(true);  // New page is dirty (needs to be written)

    // Update page metadata and page table
    UpdatePage(page, *page_id, frame_id);

    return page;
}

/**
 * Delete a page from the buffer pool and deallocate it
 * Removes the page from memory and marks it as free in the disk manager
 * @param page_id ID of the page to delete
 * @return true if successful, false if page is pinned or other error
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        if (page->GetPinCount() > 0) {
            // Cannot delete pinned page (still in use)
            return false;
        }

        // Remove from replacer and page table
        replacer_->Pin(frame_id);  // Remove from LRU tracking
        page_table_.erase(it);

        // Reset page and add frame to free list
        page->ResetMemory();
        free_list_.push_back(frame_id);
    }

    // Deallocate page from disk manager (add to free list)
    disk_manager_->DeallocatePage(page_id);

    return true;
}

/**
 * Flush all dirty pages in the buffer pool to disk
 * Used during shutdown to ensure data persistence
 */
void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> lock(latch_);

    for (const auto& entry : page_table_) {
        page_id_t page_id = entry.first;
        frame_id_t frame_id = entry.second;
        Page* page = &pages_[frame_id];

        if (page->IsDirty()) {
            try {
                disk_manager_->WritePage(page_id, page->GetData());
                page->SetDirty(false);
            } catch (const std::exception& e) {
                // Log error but continue flushing other pages
                std::cerr << "Error flushing page " << page_id << ": " << e.what() << std::endl;
            }
        }
    }
}

/**
 * Get the number of free frames available
 * @return Number of frames that can be used for new pages
 */
size_t BufferPoolManager::GetFreeFrameCount() const {
    std::lock_guard<std::mutex> lock(latch_);
    return free_list_.size() + replacer_->Size();
}

/**
 * Find a free frame in the buffer pool
 * First tries to use a frame from the free list, then attempts LRU eviction
 * @param[out] frame_id Pointer to store the ID of the found free frame
 * @return true if a free frame was found, false if all frames are pinned
 */
bool BufferPoolManager::FindFreeFrame(frame_id_t* frame_id) {
    // First try to get a frame from free list (never used frames)
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }

    // No free frames, try to evict a page using LRU policy
    if (replacer_->Victim(frame_id)) {
        Page* victim_page = &pages_[*frame_id];
        page_id_t victim_page_id = victim_page->GetPageId();

        // Flush dirty page before eviction to ensure data persistence
        if (victim_page->IsDirty()) {
            try {
                disk_manager_->WritePage(victim_page_id, victim_page->GetData());
            } catch (const std::exception& e) {
                // Put frame back in replacer on error
                replacer_->Unpin(*frame_id);
                return false;
            }
        }

        // Remove from page table (no longer in memory)
        page_table_.erase(victim_page_id);

        // Reset the page for reuse
        victim_page->ResetMemory();

        return true;
    }

    // All frames are pinned (in active use)
    return false;
}

/**
 * Update page metadata and add to page table
 * Helper function to set up a page after loading or creation
 * @param page Pointer to the page to update
 * @param page_id ID to assign to the page
 * @param frame_id Frame ID where the page is stored
 */
void BufferPoolManager::UpdatePage(Page* page, page_id_t page_id, frame_id_t frame_id) {
    // Set page metadata
    page->SetPageId(page_id);
    page->IncPinCount();  // Page is initially pinned

    // Add to page table for future lookups
    page_table_[page_id] = frame_id;

    // Pin in replacer (since it's now in use and shouldn't be evicted)
    replacer_->Pin(frame_id);
}

}  // namespace minidb
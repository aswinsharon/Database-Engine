#include "buffer/buffer_pool_manager.h"

#include <cassert>
#include <iostream>

namespace minidb {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
    // Initialize buffer pool frames
    pages_ = std::make_unique<Page[]>(pool_size_);

    // Initialize free frame list
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.push_back(static_cast<frame_id_t>(i));
    }

    // Initialize LRU replacer
    replacer_ = std::make_unique<LRUReplacer>(pool_size_);
}

BufferPoolManager::~BufferPoolManager() {
    FlushAllPages();
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // Check if page is already in buffer pool
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        // Pin the page and update LRU
        page->IncPinCount();
        replacer_->Pin(frame_id);

        return page;
    }

    // Page not in buffer pool, need to fetch from disk
    frame_id_t frame_id;
    if (!FindFreeFrame(&frame_id)) {
        // No free frame available
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

    // Update dirty flag
    if (is_dirty) {
        page->SetDirty(true);
    }

    // Decrease pin count
    page->DecPinCount();

    // If pin count reaches 0, add to replacer
    if (page->GetPinCount() == 0) {
        replacer_->Unpin(frame_id);
    }

    return true;
}

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

    // Initialize page
    page->ResetMemory();
    page->SetPageId(*page_id);
    page->SetDirty(true);  // New page is dirty

    // Update page metadata and page table
    UpdatePage(page, *page_id, frame_id);

    return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        if (page->GetPinCount() > 0) {
            // Cannot delete pinned page
            return false;
        }

        // Remove from replacer and page table
        replacer_->Pin(frame_id);
        page_table_.erase(it);

        // Reset page and add frame to free list
        page->ResetMemory();
        free_list_.push_back(frame_id);
    }

    // Deallocate page from disk manager
    disk_manager_->DeallocatePage(page_id);

    return true;
}

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

size_t BufferPoolManager::GetFreeFrameCount() const {
    std::lock_guard<std::mutex> lock(latch_);
    return free_list_.size() + replacer_->Size();
}

bool BufferPoolManager::FindFreeFrame(frame_id_t* frame_id) {
    // First try to get a frame from free list
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }

    // No free frames, try to evict a page using LRU
    if (replacer_->Victim(frame_id)) {
        Page* victim_page = &pages_[*frame_id];
        page_id_t victim_page_id = victim_page->GetPageId();

        // Flush dirty page before eviction
        if (victim_page->IsDirty()) {
            try {
                disk_manager_->WritePage(victim_page_id, victim_page->GetData());
            } catch (const std::exception& e) {
                // Put frame back in replacer on error
                replacer_->Unpin(*frame_id);
                return false;
            }
        }

        // Remove from page table
        page_table_.erase(victim_page_id);

        // Reset the page
        victim_page->ResetMemory();

        return true;
    }

    // All frames are pinned
    return false;
}

void BufferPoolManager::UpdatePage(Page* page, page_id_t page_id, frame_id_t frame_id) {
    // Set page metadata
    page->SetPageId(page_id);
    page->IncPinCount();

    // Add to page table
    page_table_[page_id] = frame_id;

    // Pin in replacer (since it's now in use)
    replacer_->Pin(frame_id);
}

}  // namespace minidb
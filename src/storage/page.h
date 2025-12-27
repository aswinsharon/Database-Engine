#pragma once

#include "common/config.h"
#include "common/types.h"
#include <cstring>
#include <atomic>
#include <cassert>

namespace minidb {

/**
 * Page is the basic unit of storage in the database.
 * All pages are PAGE_SIZE bytes and contain a header followed by data.
 * The page header contains:
 * - Page ID (4 bytes)
 * - Page Type (4 bytes) 
 * - LSN (8 bytes)
 * 
 * Pages are managed by the BufferPoolManager and provide thread-safe
 * pin/unpin operations for concurrent access control.
 */
class Page {
public:
    /**
     * Constructor - initializes page to clean state
     */
    Page() { ResetMemory(); }
    
    /**
     * Destructor
     */
    ~Page() = default;

    // Non-copyable to prevent accidental copying of large page data
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    /**
     * Get the raw data content of the page (including header)
     * @return Pointer to the page data buffer
     */
    inline char* GetData() { return data_; }
    
    /**
     * Get the raw data content of the page (including header) - const version
     * @return Const pointer to the page data buffer
     */
    inline const char* GetData() const { return data_; }

    /**
     * Get page ID from the page header
     * @return The page ID stored in the header
     */
    inline page_id_t GetPageId() const {
        return *reinterpret_cast<const page_id_t*>(data_);
    }

    /**
     * Set page ID in the page header
     * @param page_id The page ID to store in the header
     */
    inline void SetPageId(page_id_t page_id) {
        *reinterpret_cast<page_id_t*>(data_) = page_id;
    }

    /**
     * Get page type from the page header
     * @return The page type (e.g., LEAF_PAGE, INTERNAL_PAGE)
     */
    inline PageType GetPageType() const {
        return *reinterpret_cast<const PageType*>(data_ + sizeof(page_id_t));
    }

    /**
     * Set page type in the page header
     * @param page_type The page type to store in the header
     */
    inline void SetPageType(PageType page_type) {
        *reinterpret_cast<PageType*>(data_ + sizeof(page_id_t)) = page_type;
    }

    /**
     * Get Log Sequence Number (LSN) from the page header
     * Used for write-ahead logging and recovery
     * @return The LSN stored in the header
     */
    inline lsn_t GetLSN() const {
        return *reinterpret_cast<const lsn_t*>(data_ + sizeof(page_id_t) + sizeof(PageType));
    }

    /**
     * Set Log Sequence Number (LSN) in the page header
     * @param lsn The LSN to store in the header
     */
    inline void SetLSN(lsn_t lsn) {
        *reinterpret_cast<lsn_t*>(data_ + sizeof(page_id_t) + sizeof(PageType)) = lsn;
    }

    /**
     * Get the current pin count for this page
     * Pin count tracks how many threads are currently using this page
     * @return Current pin count (thread-safe)
     */
    inline int GetPinCount() const { return pin_count_.load(); }
    
    /**
     * Increment the pin count (thread-safe)
     * Called when a thread starts using this page
     */
    inline void IncPinCount() { pin_count_++; }
    
    /**
     * Decrement the pin count (thread-safe)
     * Called when a thread finishes using this page
     * @pre Pin count must be > 0
     */
    inline void DecPinCount() { 
        int count = pin_count_.fetch_sub(1);
        assert(count > 0);
    }

    /**
     * Check if the page has been modified since last write to disk
     * @return true if page is dirty (needs to be written to disk)
     */
    inline bool IsDirty() const { return is_dirty_.load(); }
    
    /**
     * Set the dirty flag for this page
     * @param dirty true if page has been modified, false if clean
     */
    inline void SetDirty(bool dirty) { is_dirty_.store(dirty); }

    /**
     * Reset page to initial clean state
     * Clears all data, resets pin count and dirty flag
     */
    void ResetMemory() {
        memset(data_, 0, PAGE_SIZE);
        pin_count_.store(0);
        is_dirty_.store(false);
    }

    /**
     * Get pointer to the usable data area (after the page header)
     * This is where actual page content (B+ tree nodes, table data, etc.) is stored
     * @return Pointer to data area of size (PAGE_SIZE - PAGE_HEADER_SIZE)
     */
    inline char* GetDataArea() {
        return data_ + PAGE_HEADER_SIZE;
    }

    /**
     * Get const pointer to the usable data area (after the page header)
     * @return Const pointer to data area of size (PAGE_SIZE - PAGE_HEADER_SIZE)
     */
    inline const char* GetDataArea() const {
        return data_ + PAGE_HEADER_SIZE;
    }

private:
    char data_[PAGE_SIZE];              ///< Page data buffer (header + content)
    std::atomic<int> pin_count_{0};     ///< Number of threads currently using this page
    std::atomic<bool> is_dirty_{false}; ///< Whether page has been modified since last disk write
};

}  // namespace minidb
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
 */
class Page {
public:
    Page() { ResetMemory(); }
    ~Page() = default;

    // Non-copyable
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    /**
     * Get the actual data content of the page
     */
    inline char* GetData() { return data_; }
    inline const char* GetData() const { return data_; }

    /**
     * Get page ID from the page header
     */
    inline page_id_t GetPageId() const {
        return *reinterpret_cast<const page_id_t*>(data_);
    }

    /**
     * Set page ID in the page header
     */
    inline void SetPageId(page_id_t page_id) {
        *reinterpret_cast<page_id_t*>(data_) = page_id;
    }

    /**
     * Get page type from the page header
     */
    inline PageType GetPageType() const {
        return *reinterpret_cast<const PageType*>(data_ + sizeof(page_id_t));
    }

    /**
     * Set page type in the page header
     */
    inline void SetPageType(PageType page_type) {
        *reinterpret_cast<PageType*>(data_ + sizeof(page_id_t)) = page_type;
    }

    /**
     * Get LSN from the page header
     */
    inline lsn_t GetLSN() const {
        return *reinterpret_cast<const lsn_t*>(data_ + sizeof(page_id_t) + sizeof(PageType));
    }

    /**
     * Set LSN in the page header
     */
    inline void SetLSN(lsn_t lsn) {
        *reinterpret_cast<lsn_t*>(data_ + sizeof(page_id_t) + sizeof(PageType)) = lsn;
    }

    /**
     * Pin/Unpin operations for buffer pool management
     */
    inline int GetPinCount() const { return pin_count_.load(); }
    inline void IncPinCount() { pin_count_++; }
    inline void DecPinCount() { 
        int count = pin_count_.fetch_sub(1);
        assert(count > 0);
    }

    /**
     * Dirty flag operations
     */
    inline bool IsDirty() const { return is_dirty_.load(); }
    inline void SetDirty(bool dirty) { is_dirty_.store(dirty); }

    /**
     * Reset page to initial state
     */
    void ResetMemory() {
        memset(data_, 0, PAGE_SIZE);
        pin_count_.store(0);
        is_dirty_.store(false);
    }

    /**
     * Get pointer to the data area (after header)
     */
    inline char* GetDataArea() {
        return data_ + PAGE_HEADER_SIZE;
    }

    inline const char* GetDataArea() const {
        return data_ + PAGE_HEADER_SIZE;
    }

private:
    // Page data (header + content)
    char data_[PAGE_SIZE];
    
    // Buffer pool management
    std::atomic<int> pin_count_{0};
    std::atomic<bool> is_dirty_{false};
};

}  // namespace minidb
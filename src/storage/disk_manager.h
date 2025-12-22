#pragma once

#include "common/config.h"
#include "common/types.h"
#include "storage/page.h"
#include <fstream>
#include <string>
#include <mutex>
#include <vector>

namespace minidb {

/**
 * DiskManager manages the database file and provides page-level I/O operations.
 * It handles:
 * - Reading/writing pages to/from disk
 * - Allocating new pages
 * - Managing free page list
 * - File operations (create, open, close)
 */
class DiskManager {
public:
    /**
     * Constructor - creates or opens the database file
     */
    explicit DiskManager(const std::string& db_file);
    
    /**
     * Destructor - ensures all data is flushed and file is closed
     */
    ~DiskManager();

    // Non-copyable
    DiskManager(const DiskManager&) = delete;
    DiskManager& operator=(const DiskManager&) = delete;

    /**
     * Read a page from disk
     * @param page_id the page to read
     * @param page_data buffer to store the page data
     */
    void ReadPage(page_id_t page_id, char* page_data);

    /**
     * Write a page to disk
     * @param page_id the page to write
     * @param page_data the page data to write
     */
    void WritePage(page_id_t page_id, const char* page_data);

    /**
     * Allocate a new page
     * @return the page ID of the newly allocated page
     */
    page_id_t AllocatePage();

    /**
     * Deallocate a page (add to free list)
     * @param page_id the page to deallocate
     */
    void DeallocatePage(page_id_t page_id);

    /**
     * Get the number of pages in the database file
     */
    uint32_t GetNumPages() const { return num_pages_; }

    /**
     * Flush all pending writes to disk
     */
    void FlushLog();

    /**
     * Check if the database file exists and is valid
     */
    bool IsDBOpen() const { return db_file_.is_open(); }

private:
    /**
     * Initialize a new database file with header page
     */
    void InitializeHeaderPage();

    /**
     * Read the header page to get metadata
     */
    void ReadHeaderPage();

    /**
     * Write the header page with current metadata
     */
    void WriteHeaderPage();

    /**
     * Get file offset for a given page ID
     */
    size_t GetFileOffset(page_id_t page_id) const {
        return static_cast<size_t>(page_id) * PAGE_SIZE;
    }

private:
    std::string file_name_;
    std::fstream db_file_;
    std::mutex db_io_latch_;
    
    // Database metadata
    uint32_t num_pages_;
    std::vector<page_id_t> free_list_;
    
    // Header page data
    static constexpr page_id_t HEADER_PAGE_ID = 0;
    static constexpr uint32_t MAGIC_NUMBER = 0xDEADBEEF;
};

}  // namespace minidb
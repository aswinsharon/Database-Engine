#pragma once

#include "table/table_page.h"
#include "buffer/buffer_pool_manager.h"
#include "table/tuple.h"
#include "common/config.h"

namespace minidb {

/**
 * TableHeap manages a collection of table pages for storing tuples
 * Provides high-level operations for tuple insertion, deletion, and iteration
 */
class TableHeap {
public:
    explicit TableHeap(BufferPoolManager* buffer_pool_manager, page_id_t first_page_id = INVALID_PAGE_ID);
    ~TableHeap() = default;
    
    // Tuple operations
    bool InsertTuple(const Tuple& tuple, RID& rid);
    bool MarkDelete(const RID& rid);
    bool UpdateTuple(const Tuple& new_tuple, const RID& rid);
    bool GetTuple(const RID& rid, Tuple& tuple);
    
    // Iterator support
    class Iterator {
    public:
        Iterator(TableHeap* table_heap, RID rid);
        Iterator(const Iterator& other) = default;
        Iterator& operator=(const Iterator& other) = default;
        
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;
        
        const RID& operator*() const;
        Iterator& operator++();
        
        bool IsEnd() const;
        
    private:
        TableHeap* table_heap_;
        RID rid_;
        bool is_end_;
    };
    
    Iterator Begin();
    Iterator End();
    
    // Page management
    page_id_t GetFirstPageId() const { return first_page_id_; }
    
private:
    // Get table page with proper casting
    TablePage* GetTablePage(page_id_t page_id);
    
    // Create new page for table
    page_id_t CreateNewPage();
    
    // Find page with enough space or create new one
    page_id_t FindPageWithSpace(uint32_t tuple_size);
    
private:
    BufferPoolManager* buffer_pool_manager_;
    page_id_t first_page_id_;
    page_id_t last_page_id_;
};

} // namespace minidb
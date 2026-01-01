#pragma once

#include "storage/page.h"
#include "table/tuple.h"
#include "common/config.h"
#include <vector>

namespace minidb {

/**
 * Slotted page format for storing variable-length tuples
 * 
 * Page Layout:
 * +----------------+
 * | Page Header    | (24 bytes)
 * +----------------+
 * | TablePage      | (16 bytes)
 * | Header         |
 * +----------------+
 * | Slot Array     | (grows down)
 * | [slot_0]       |
 * | [slot_1]       |
 * | ...            |
 * +----------------+
 * | Free Space     |
 * +----------------+
 * | Tuple Data     | (grows up)
 * | [tuple_n]      |
 * | [tuple_n-1]    |
 * | ...            |
 * +----------------+
 */

struct Slot {
    uint32_t offset;  // Offset to tuple data (0 if deleted)
    uint32_t size;    // Size of tuple data
};

class TablePage {
public:
    static constexpr uint32_t TABLE_PAGE_HEADER_SIZE = 16;
    static constexpr uint32_t SLOT_SIZE = sizeof(Slot);
    
    // Initialize a new table page
    void Init(page_id_t page_id, page_id_t prev_page_id = INVALID_PAGE_ID);
    
    // Tuple operations
    bool InsertTuple(const Tuple& tuple, RID& rid);
    bool MarkDelete(const RID& rid);
    bool UpdateTuple(const Tuple& new_tuple, const RID& rid);
    bool GetTuple(const RID& rid, Tuple& tuple);
    
    // Iterator support
    bool GetFirstTupleRid(RID& first_rid);
    bool GetNextTupleRid(const RID& cur_rid, RID& next_rid);
    
    // Page info
    uint32_t GetTupleCount() const { return tuple_count_; }
    uint32_t GetFreeSpaceSize() const;
    bool IsEmpty() const { return tuple_count_ == 0; }
    
    // Page linking for table heap
    page_id_t GetNextPageId() const { return next_page_id_; }
    void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
    
private:
    // Get slot by index
    Slot* GetSlot(uint32_t slot_idx);
    const Slot* GetSlot(uint32_t slot_idx) const;
    
    // Get tuple data pointer
    char* GetTupleData(uint32_t offset);
    const char* GetTupleData(uint32_t offset) const;
    
    // Find free slot or create new one
    uint32_t GetFreeSlot();
    
    // Compact page to reclaim deleted space
    void CompactPage();
    
    // Check if we have enough space for tuple
    bool HasEnoughSpace(uint32_t tuple_size) const;
    
private:
    // Page header (16 bytes)
    page_id_t next_page_id_;     // Next page in table heap
    uint32_t tuple_count_;       // Number of tuples (including deleted)
    uint32_t free_space_pointer_; // Points to start of free space
    uint32_t deleted_tuple_count_; // Number of deleted tuples
    
    // Slot array starts here, grows downward
    // Tuple data starts at end of page, grows upward
};

} // namespace minidb
#include "table/table_page.h"
#include <cstring>
#include <algorithm>

namespace minidb {

void TablePage::Init(page_id_t page_id, page_id_t prev_page_id) {
    next_page_id_ = INVALID_PAGE_ID;
    tuple_count_ = 0;
    free_space_pointer_ = PAGE_SIZE;
    deleted_tuple_count_ = 0;
}

bool TablePage::InsertTuple(const Tuple& tuple, RID& rid) {
    uint32_t tuple_size = tuple.GetSerializedSize();
    
    if (!HasEnoughSpace(tuple_size)) {
        // Try compacting first
        CompactPage();
        if (!HasEnoughSpace(tuple_size)) {
            return false;
        }
    }
    
    // Find free slot
    uint32_t slot_idx = GetFreeSlot();
    
    // Allocate space for tuple data
    free_space_pointer_ -= tuple_size;
    
    // Serialize tuple to page
    char* tuple_data = GetTupleData(free_space_pointer_);
    tuple.SerializeTo(tuple_data);
    
    // Update slot
    Slot* slot = GetSlot(slot_idx);
    slot->offset = free_space_pointer_;
    slot->size = tuple_size;
    
    // Set RID
    rid.page_id = reinterpret_cast<Page*>(this)->GetPageId();
    rid.slot_num = slot_idx;
    
    return true;
}

bool TablePage::MarkDelete(const RID& rid) {
    if (rid.slot_num >= tuple_count_) {
        return false;
    }
    
    Slot* slot = GetSlot(rid.slot_num);
    if (slot->offset == 0) {
        return false; // Already deleted
    }
    
    slot->offset = 0;
    slot->size = 0;
    deleted_tuple_count_++;
    
    return true;
}

bool TablePage::UpdateTuple(const Tuple& new_tuple, const RID& rid) {
    if (rid.slot_num >= tuple_count_) {
        return false;
    }
    
    Slot* slot = GetSlot(rid.slot_num);
    if (slot->offset == 0) {
        return false; // Tuple is deleted
    }
    
    uint32_t new_size = new_tuple.GetSerializedSize();
    uint32_t old_size = slot->size;
    
    if (new_size <= old_size) {
        // In-place update
        char* tuple_data = GetTupleData(slot->offset);
        new_tuple.SerializeTo(tuple_data);
        slot->size = new_size;
        return true;
    } else {
        // Need more space - mark old as deleted and insert new
        slot->offset = 0;
        slot->size = 0;
        deleted_tuple_count_++;
        
        RID new_rid;
        if (InsertTuple(new_tuple, new_rid)) {
            // Move new tuple to old slot position
            Slot* new_slot = GetSlot(new_rid.slot_num);
            Slot* old_slot = GetSlot(rid.slot_num);
            
            old_slot->offset = new_slot->offset;
            old_slot->size = new_slot->size;
            
            new_slot->offset = 0;
            new_slot->size = 0;
            
            deleted_tuple_count_--;
            return true;
        }
        return false;
    }
}

bool TablePage::GetTuple(const RID& rid, Tuple& tuple) {
    if (rid.slot_num >= tuple_count_) {
        return false;
    }
    
    const Slot* slot = GetSlot(rid.slot_num);
    if (slot->offset == 0) {
        return false; // Tuple is deleted
    }
    
    const char* tuple_data = GetTupleData(slot->offset);
    tuple.DeserializeFrom(tuple_data);
    
    return true;
}

bool TablePage::GetFirstTupleRid(RID& first_rid) {
    for (uint32_t i = 0; i < tuple_count_; i++) {
        const Slot* slot = GetSlot(i);
        if (slot->offset != 0) {
            first_rid.page_id = reinterpret_cast<const Page*>(this)->GetPageId();
            first_rid.slot_num = i;
            return true;
        }
    }
    return false;
}

bool TablePage::GetNextTupleRid(const RID& cur_rid, RID& next_rid) {
    for (uint32_t i = cur_rid.slot_num + 1; i < tuple_count_; i++) {
        const Slot* slot = GetSlot(i);
        if (slot->offset != 0) {
            next_rid.page_id = reinterpret_cast<const Page*>(this)->GetPageId();
            next_rid.slot_num = i;
            return true;
        }
    }
    return false;
}

uint32_t TablePage::GetFreeSpaceSize() const {
    uint32_t slot_array_size = tuple_count_ * SLOT_SIZE;
    uint32_t used_header_size = PAGE_HEADER_SIZE + TABLE_PAGE_HEADER_SIZE;
    
    if (free_space_pointer_ <= used_header_size + slot_array_size) {
        return 0;
    }
    
    return free_space_pointer_ - used_header_size - slot_array_size;
}

Slot* TablePage::GetSlot(uint32_t slot_idx) {
    char* page_data = reinterpret_cast<char*>(this);
    char* slot_array_start = page_data + PAGE_HEADER_SIZE + TABLE_PAGE_HEADER_SIZE;
    return reinterpret_cast<Slot*>(slot_array_start + slot_idx * SLOT_SIZE);
}

const Slot* TablePage::GetSlot(uint32_t slot_idx) const {
    const char* page_data = reinterpret_cast<const char*>(this);
    const char* slot_array_start = page_data + PAGE_HEADER_SIZE + TABLE_PAGE_HEADER_SIZE;
    return reinterpret_cast<const Slot*>(slot_array_start + slot_idx * SLOT_SIZE);
}

char* TablePage::GetTupleData(uint32_t offset) {
    char* page_data = reinterpret_cast<char*>(this);
    return page_data + offset;
}

const char* TablePage::GetTupleData(uint32_t offset) const {
    const char* page_data = reinterpret_cast<const char*>(this);
    return page_data + offset;
}

uint32_t TablePage::GetFreeSlot() {
    // Look for deleted slot first
    for (uint32_t i = 0; i < tuple_count_; i++) {
        Slot* slot = GetSlot(i);
        if (slot->offset == 0) {
            deleted_tuple_count_--;
            return i;
        }
    }
    
    // Create new slot
    return tuple_count_++;
}

void TablePage::CompactPage() {
    if (deleted_tuple_count_ == 0) {
        return;
    }
    
    std::vector<std::pair<uint32_t, uint32_t>> live_tuples; // (slot_idx, size)
    
    // Collect live tuples
    for (uint32_t i = 0; i < tuple_count_; i++) {
        Slot* slot = GetSlot(i);
        if (slot->offset != 0) {
            live_tuples.push_back({i, slot->size});
        }
    }
    
    // Reset free space pointer
    free_space_pointer_ = PAGE_SIZE;
    
    // Relocate tuples
    for (auto& [slot_idx, size] : live_tuples) {
        Slot* old_slot = GetSlot(slot_idx);
        char* old_data = GetTupleData(old_slot->offset);
        
        free_space_pointer_ -= size;
        char* new_data = GetTupleData(free_space_pointer_);
        
        std::memcpy(new_data, old_data, size);
        old_slot->offset = free_space_pointer_;
    }
    
    deleted_tuple_count_ = 0;
}

bool TablePage::HasEnoughSpace(uint32_t tuple_size) const {
    uint32_t required_space = tuple_size;
    
    // Need space for new slot if no deleted slots available
    if (deleted_tuple_count_ == 0) {
        required_space += SLOT_SIZE;
    }
    
    return GetFreeSpaceSize() >= required_space;
}

} // namespace minidb
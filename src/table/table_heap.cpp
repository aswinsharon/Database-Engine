#include "table/table_heap.h"
#include <cassert>

namespace minidb {

TableHeap::TableHeap(BufferPoolManager* buffer_pool_manager, page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager), first_page_id_(first_page_id), last_page_id_(first_page_id) {
    
    if (first_page_id_ == INVALID_PAGE_ID) {
        // Create first page
        first_page_id_ = CreateNewPage();
        last_page_id_ = first_page_id_;
    }
}

bool TableHeap::InsertTuple(const Tuple& tuple, RID& rid) {
    uint32_t tuple_size = tuple.GetSerializedSize();
    page_id_t page_id = FindPageWithSpace(tuple_size);
    
    if (page_id == INVALID_PAGE_ID) {
        return false;
    }
    
    TablePage* page = GetTablePage(page_id);
    if (page == nullptr) {
        return false;
    }
    
    bool success = page->InsertTuple(tuple, rid);
    
    // Unpin page
    buffer_pool_manager_->UnpinPage(page_id, success);
    
    return success;
}

bool TableHeap::MarkDelete(const RID& rid) {
    TablePage* page = GetTablePage(rid.page_id);
    if (page == nullptr) {
        return false;
    }
    
    bool success = page->MarkDelete(rid);
    
    // Unpin page
    buffer_pool_manager_->UnpinPage(rid.page_id, success);
    
    return success;
}

bool TableHeap::UpdateTuple(const Tuple& new_tuple, const RID& rid) {
    TablePage* page = GetTablePage(rid.page_id);
    if (page == nullptr) {
        return false;
    }
    
    bool success = page->UpdateTuple(new_tuple, rid);
    
    // Unpin page
    buffer_pool_manager_->UnpinPage(rid.page_id, success);
    
    return success;
}

bool TableHeap::GetTuple(const RID& rid, Tuple& tuple) {
    TablePage* page = GetTablePage(rid.page_id);
    if (page == nullptr) {
        return false;
    }
    
    bool success = page->GetTuple(rid, tuple);
    
    // Unpin page
    buffer_pool_manager_->UnpinPage(rid.page_id, false);
    
    return success;
}

TableHeap::Iterator TableHeap::Begin() {
    RID first_rid;
    first_rid.page_id = first_page_id_;
    first_rid.slot_num = 0;
    
    // Find first valid tuple
    page_id_t current_page = first_page_id_;
    while (current_page != INVALID_PAGE_ID) {
        TablePage* page = GetTablePage(current_page);
        if (page != nullptr) {
            if (page->GetFirstTupleRid(first_rid)) {
                buffer_pool_manager_->UnpinPage(current_page, false);
                return Iterator(this, first_rid);
            }
            current_page = page->GetNextPageId();
            buffer_pool_manager_->UnpinPage(current_page, false);
        } else {
            break;
        }
    }
    
    return End();
}

TableHeap::Iterator TableHeap::End() {
    RID end_rid;
    end_rid.page_id = INVALID_PAGE_ID;
    end_rid.slot_num = 0;
    return Iterator(this, end_rid);
}

TablePage* TableHeap::GetTablePage(page_id_t page_id) {
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return nullptr;
    }
    
    return reinterpret_cast<TablePage*>(page->GetData());
}

page_id_t TableHeap::CreateNewPage() {
    page_id_t new_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
    
    if (new_page == nullptr) {
        return INVALID_PAGE_ID;
    }
    
    // Initialize table page
    TablePage* table_page = reinterpret_cast<TablePage*>(new_page->GetData());
    table_page->Init(new_page_id);
    
    // Link to previous page if exists
    if (last_page_id_ != INVALID_PAGE_ID && last_page_id_ != new_page_id) {
        TablePage* last_page = GetTablePage(last_page_id_);
        if (last_page != nullptr) {
            last_page->SetNextPageId(new_page_id);
            buffer_pool_manager_->UnpinPage(last_page_id_, true);
        }
    }
    
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    last_page_id_ = new_page_id;
    
    return new_page_id;
}

page_id_t TableHeap::FindPageWithSpace(uint32_t tuple_size) {
    page_id_t current_page = first_page_id_;
    
    while (current_page != INVALID_PAGE_ID) {
        TablePage* page = GetTablePage(current_page);
        if (page == nullptr) {
            break;
        }
        
        if (page->GetFreeSpaceSize() >= tuple_size + sizeof(Slot)) {
            buffer_pool_manager_->UnpinPage(current_page, false);
            return current_page;
        }
        
        page_id_t next_page = page->GetNextPageId();
        buffer_pool_manager_->UnpinPage(current_page, false);
        current_page = next_page;
    }
    
    // No existing page has space, create new one
    return CreateNewPage();
}

// Iterator implementation
TableHeap::Iterator::Iterator(TableHeap* table_heap, RID rid)
    : table_heap_(table_heap), rid_(rid), is_end_(rid.page_id == INVALID_PAGE_ID) {
}

bool TableHeap::Iterator::operator==(const Iterator& other) const {
    return rid_.page_id == other.rid_.page_id && rid_.slot_num == other.rid_.slot_num;
}

bool TableHeap::Iterator::operator!=(const Iterator& other) const {
    return !(*this == other);
}

const RID& TableHeap::Iterator::operator*() const {
    return rid_;
}

TableHeap::Iterator& TableHeap::Iterator::operator++() {
    if (is_end_) {
        return *this;
    }
    
    // Try to get next tuple on current page
    TablePage* page = table_heap_->GetTablePage(rid_.page_id);
    if (page != nullptr) {
        RID next_rid;
        if (page->GetNextTupleRid(rid_, next_rid)) {
            rid_ = next_rid;
            table_heap_->buffer_pool_manager_->UnpinPage(rid_.page_id, false);
            return *this;
        }
        
        // Move to next page
        page_id_t next_page_id = page->GetNextPageId();
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.page_id, false);
        
        while (next_page_id != INVALID_PAGE_ID) {
            TablePage* next_page = table_heap_->GetTablePage(next_page_id);
            if (next_page != nullptr) {
                if (next_page->GetFirstTupleRid(rid_)) {
                    table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
                    return *this;
                }
                next_page_id = next_page->GetNextPageId();
                table_heap_->buffer_pool_manager_->UnpinPage(next_page_id, false);
            } else {
                break;
            }
        }
    }
    
    // End of iteration
    is_end_ = true;
    rid_.page_id = INVALID_PAGE_ID;
    rid_.slot_num = 0;
    
    return *this;
}

bool TableHeap::Iterator::IsEnd() const {
    return is_end_;
}

} // namespace minidb
#pragma once

#include "table/table_heap.h"
#include "table/schema.h"
#include "table/tuple.h"
#include "buffer/buffer_pool_manager.h"
#include <string>
#include <memory>

namespace minidb {

/**
 * Table represents a database table with schema and storage
 * Provides high-level operations for table management
 */
class Table {
public:
    Table(const std::string& name, std::unique_ptr<Schema> schema, 
          BufferPoolManager* buffer_pool_manager, page_id_t first_page_id = INVALID_PAGE_ID);
    
    ~Table() = default;
    
    // Table info
    const std::string& GetName() const { return name_; }
    const Schema& GetSchema() const { return *schema_; }
    Schema* GetSchema() { return schema_.get(); }
    
    // Tuple operations
    bool InsertTuple(const Tuple& tuple, RID& rid);
    bool MarkDelete(const RID& rid);
    bool UpdateTuple(const Tuple& new_tuple, const RID& rid);
    bool GetTuple(const RID& rid, Tuple& tuple);
    
    // Validation
    bool ValidateTuple(const Tuple& tuple) const;
    
    // Iterator support
    using Iterator = TableHeap::Iterator;
    Iterator Begin() { return table_heap_->Begin(); }
    Iterator End() { return table_heap_->End(); }
    
    // Table heap access
    TableHeap* GetTableHeap() { return table_heap_.get(); }
    
private:
    std::string name_;
    std::unique_ptr<Schema> schema_;
    std::unique_ptr<TableHeap> table_heap_;
};

} // namespace minidb
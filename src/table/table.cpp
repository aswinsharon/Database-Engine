#include "table/table.h"

namespace minidb {

Table::Table(const std::string& name, std::unique_ptr<Schema> schema, 
             BufferPoolManager* buffer_pool_manager, page_id_t first_page_id)
    : name_(name), schema_(std::move(schema)) {
    table_heap_ = std::make_unique<TableHeap>(buffer_pool_manager, first_page_id);
}

bool Table::InsertTuple(const Tuple& tuple, RID& rid) {
    if (!ValidateTuple(tuple)) {
        return false;
    }
    
    return table_heap_->InsertTuple(tuple, rid);
}

bool Table::MarkDelete(const RID& rid) {
    return table_heap_->MarkDelete(rid);
}

bool Table::UpdateTuple(const Tuple& new_tuple, const RID& rid) {
    if (!ValidateTuple(new_tuple)) {
        return false;
    }
    
    return table_heap_->UpdateTuple(new_tuple, rid);
}

bool Table::GetTuple(const RID& rid, Tuple& tuple) {
    return table_heap_->GetTuple(rid, tuple);
}

bool Table::ValidateTuple(const Tuple& tuple) const {
    if (tuple.GetSize() != schema_->GetColumnCount()) {
        return false;
    }
    
    for (size_t i = 0; i < schema_->GetColumnCount(); i++) {
        const Column& column = schema_->GetColumn(i);
        const Value& value = tuple.GetValue(i);
        
        // Check data type compatibility
        if (value.GetType() != column.GetType() && value.GetType() != DataType::NULL_TYPE) {
            return false;
        }
        
        // Check VARCHAR length constraints
        if (column.GetType() == DataType::VARCHAR && value.GetType() == DataType::VARCHAR) {
            if (value.GetData().varchar_value.length() > column.GetSize()) {
                return false;
            }
        }
    }
    
    return true;
}

} // namespace minidb
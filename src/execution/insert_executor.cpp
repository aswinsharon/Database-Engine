#include "execution/insert_executor.h"

namespace minidb {

InsertExecutor::InsertExecutor(ExecutionContext* context,
                               const std::string& table_name,
                               std::vector<std::vector<Value>> values)
    : Executor(context), table_name_(table_name), values_(std::move(values)),
      current_index_(0), inserted_count_(0), executed_(false) {
    
    // Create result schema (just a count column)
    std::vector<Column> columns;
    columns.emplace_back("inserted_count", DataType::INTEGER, sizeof(int));
    result_schema_ = std::make_unique<Schema>(columns);
}

void InsertExecutor::Init() {
    table_ = context_->GetTable(table_name_);
    current_index_ = 0;
    inserted_count_ = 0;
    executed_ = false;
}

bool InsertExecutor::Next(Tuple& tuple, RID& rid) {
    if (executed_ || table_ == nullptr) {
        return false;
    }
    
    // Execute all insertions
    for (const auto& row_values : values_) {
        if (row_values.size() != table_->GetSchema().GetColumnCount()) {
            continue; // Skip invalid rows
        }
        
        Tuple new_tuple(row_values);
        RID new_rid;
        
        if (table_->InsertTuple(new_tuple, new_rid)) {
            inserted_count_++;
        }
    }
    
    // Return result tuple with count
    std::vector<Value> result_values;
    result_values.emplace_back(DataType::INTEGER, inserted_count_);
    tuple = Tuple(result_values);
    
    executed_ = true;
    return true;
}

const Schema& InsertExecutor::GetOutputSchema() const {
    return *result_schema_;
}

} // namespace minidb
#include "execution/seq_scan_executor.h"

namespace minidb {

SeqScanExecutor::SeqScanExecutor(ExecutionContext* context, 
                                 const std::string& table_name,
                                 std::unique_ptr<Expression> predicate)
    : Executor(context), table_name_(table_name), predicate_(std::move(predicate)), is_end_(false) {
}

void SeqScanExecutor::Init() {
    table_ = context_->GetTable(table_name_);
    if (table_ == nullptr) {
        is_end_ = true;
        return;
    }
    
    iterator_ = std::make_unique<TableHeap::Iterator>(table_->Begin());
    is_end_ = iterator_->IsEnd();
}

bool SeqScanExecutor::Next(Tuple& tuple, RID& rid) {
    if (is_end_ || iterator_->IsEnd()) {
        return false;
    }
    
    while (!iterator_->IsEnd()) {
        rid = **iterator_;
        
        // Get the tuple
        if (!table_->GetTuple(rid, tuple)) {
            ++(*iterator_);
            continue;
        }
        
        // Apply predicate if exists
        if (predicate_ != nullptr) {
            Value result = predicate_->Evaluate(tuple, GetOutputSchema());
            if (result.GetType() != DataType::BOOLEAN || !result.GetData().boolean_value) {
                ++(*iterator_);
                continue;
            }
        }
        
        // Move to next tuple for next call
        ++(*iterator_);
        return true;
    }
    
    is_end_ = true;
    return false;
}

const Schema& SeqScanExecutor::GetOutputSchema() const {
    return table_->GetSchema();
}

} // namespace minidb
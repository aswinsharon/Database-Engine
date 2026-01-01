#pragma once

#include "execution/executor.h"
#include "table/table.h"
#include <vector>

namespace minidb {

/**
 * Insert executor - inserts tuples into a table
 */
class InsertExecutor : public Executor {
public:
    InsertExecutor(ExecutionContext* context,
                   const std::string& table_name,
                   std::vector<std::vector<Value>> values);
    
    void Init() override;
    bool Next(Tuple& tuple, RID& rid) override;
    const Schema& GetOutputSchema() const override;
    
    // Get number of inserted rows
    int GetInsertedCount() const { return inserted_count_; }
    
private:
    std::string table_name_;
    std::shared_ptr<Table> table_;
    std::vector<std::vector<Value>> values_;
    size_t current_index_;
    int inserted_count_;
    bool executed_;
    
    // Result schema for INSERT (just a count)
    std::unique_ptr<Schema> result_schema_;
};

} // namespace minidb
#pragma once

#include "execution/executor.h"
#include "table/table.h"
#include "execution/expressions/expression.h"
#include <memory>
#include <unordered_map>

namespace minidb {

/**
 * Sequential scan executor - scans all tuples in a table
 */
class SeqScanExecutor : public Executor {
public:
    SeqScanExecutor(ExecutionContext* context, 
                    const std::string& table_name,
                    std::unique_ptr<Expression> predicate = nullptr);
    
    void Init() override;
    bool Next(Tuple& tuple, RID& rid) override;
    const Schema& GetOutputSchema() const override;
    
private:
    std::string table_name_;
    std::shared_ptr<Table> table_;
    std::unique_ptr<Expression> predicate_;
    std::unique_ptr<TableHeap::Iterator> iterator_;
    bool is_end_;
};

} // namespace minidb
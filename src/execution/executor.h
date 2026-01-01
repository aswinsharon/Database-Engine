#pragma once

#include "table/tuple.h"
#include "common/types.h"
#include <vector>
#include <memory>

namespace minidb {

// Forward declarations
class ExecutionContext;

/**
 * Base class for all query executors
 */
class Executor {
public:
    explicit Executor(ExecutionContext* context) : context_(context) {}
    virtual ~Executor() = default;
    
    // Initialize the executor
    virtual void Init() = 0;
    
    // Get the next tuple, returns false when no more tuples
    virtual bool Next(Tuple& tuple, RID& rid) = 0;
    
    // Get the schema of output tuples
    virtual const Schema& GetOutputSchema() const = 0;
    
protected:
    ExecutionContext* context_;
};

/**
 * Execution context containing shared resources
 */
class ExecutionContext {
public:
    ExecutionContext(BufferPoolManager* buffer_pool_manager)
        : buffer_pool_manager_(buffer_pool_manager) {}
    
    BufferPoolManager* GetBufferPoolManager() { return buffer_pool_manager_; }
    
    // Table registry
    void RegisterTable(const std::string& name, std::shared_ptr<Table> table) {
        tables_[name] = table;
    }
    
    std::shared_ptr<Table> GetTable(const std::string& name) {
        auto it = tables_.find(name);
        return (it != tables_.end()) ? it->second : nullptr;
    }
    
private:
    BufferPoolManager* buffer_pool_manager_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

} // namespace minidb
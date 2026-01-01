#pragma once

#include "execution/executor.h"
#include "table/table.h"
#include "buffer/buffer_pool_manager.h"
#include <string>
#include <memory>
#include <vector>
#include <unordered_map>

namespace minidb {

/**
 * Simple SQL statement types
 */
enum class StatementType {
    SELECT,
    INSERT,
    DELETE,
    CREATE_TABLE,
    INVALID
};

/**
 * Parsed SQL statement
 */
struct ParsedStatement {
    StatementType type = StatementType::INVALID;
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::vector<Value>> values;
    std::string where_column;
    Value where_value;
    std::string where_operator;
    
    // For CREATE TABLE
    std::vector<Column> table_columns;
};

/**
 * Query result containing tuples and metadata
 */
struct QueryResult {
    std::vector<Tuple> tuples;
    std::vector<RID> rids;
    std::unique_ptr<Schema> schema;
    int affected_rows = 0;
    bool success = false;
    std::string error_message;
};

/**
 * Simple query engine with basic SQL parsing and execution
 */
class QueryEngine {
public:
    explicit QueryEngine(BufferPoolManager* buffer_pool_manager);
    ~QueryEngine() = default;
    
    // Execute a SQL query
    QueryResult ExecuteQuery(const std::string& sql);
    
    // Table management
    bool CreateTable(const std::string& name, const std::vector<Column>& columns);
    bool DropTable(const std::string& name);
    std::shared_ptr<Table> GetTable(const std::string& name);
    
    // Get list of all tables
    std::vector<std::string> GetTableNames() const;
    
private:
    // SQL parsing
    ParsedStatement ParseSQL(const std::string& sql);
    StatementType GetStatementType(const std::string& sql);
    
    // Statement parsing helpers
    ParsedStatement ParseSelect(const std::string& sql);
    ParsedStatement ParseInsert(const std::string& sql);
    ParsedStatement ParseCreateTable(const std::string& sql);
    
    // Execution
    QueryResult ExecuteSelect(const ParsedStatement& stmt);
    QueryResult ExecuteInsert(const ParsedStatement& stmt);
    QueryResult ExecuteCreateTable(const ParsedStatement& stmt);
    
    // Utility functions
    std::vector<std::string> Tokenize(const std::string& str);
    std::string ToUpper(const std::string& str);
    Value ParseValue(const std::string& str);
    DataType ParseDataType(const std::string& type_str);
    
private:
    std::unique_ptr<ExecutionContext> context_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

} // namespace minidb
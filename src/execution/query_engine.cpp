#include "execution/query_engine.h"
#include "execution/seq_scan_executor.h"
#include "execution/insert_executor.h"
#include "execution/expressions/expression.h"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace minidb {

QueryEngine::QueryEngine(BufferPoolManager* buffer_pool_manager) {
    context_ = std::make_unique<ExecutionContext>(buffer_pool_manager);
}

QueryResult QueryEngine::ExecuteQuery(const std::string& sql) {
    QueryResult result;
    
    try {
        ParsedStatement stmt = ParseSQL(sql);
        
        if (stmt.type == StatementType::INVALID) {
            result.error_message = "Invalid SQL statement";
            return result;
        }
        
        switch (stmt.type) {
            case StatementType::SELECT:
                result = ExecuteSelect(stmt);
                break;
            case StatementType::INSERT:
                result = ExecuteInsert(stmt);
                break;
            case StatementType::CREATE_TABLE:
                result = ExecuteCreateTable(stmt);
                break;
            default:
                result.error_message = "Unsupported statement type";
                break;
        }
    } catch (const std::exception& e) {
        result.error_message = e.what();
    }
    
    return result;
}

bool QueryEngine::CreateTable(const std::string& name, const std::vector<Column>& columns) {
    if (tables_.find(name) != tables_.end()) {
        return false; // Table already exists
    }
    
    auto schema = std::make_unique<Schema>(columns);
    auto table = std::make_shared<Table>(name, std::move(schema), context_->GetBufferPoolManager());
    
    tables_[name] = table;
    context_->RegisterTable(name, table);
    
    return true;
}

bool QueryEngine::DropTable(const std::string& name) {
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return false;
    }
    
    tables_.erase(it);
    return true;
}

std::shared_ptr<Table> QueryEngine::GetTable(const std::string& name) {
    auto it = tables_.find(name);
    return (it != tables_.end()) ? it->second : nullptr;
}

std::vector<std::string> QueryEngine::GetTableNames() const {
    std::vector<std::string> names;
    for (const auto& pair : tables_) {
        names.push_back(pair.first);
    }
    return names;
}

ParsedStatement QueryEngine::ParseSQL(const std::string& sql) {
    StatementType type = GetStatementType(sql);
    
    switch (type) {
        case StatementType::SELECT:
            return ParseSelect(sql);
        case StatementType::INSERT:
            return ParseInsert(sql);
        case StatementType::CREATE_TABLE:
            return ParseCreateTable(sql);
        default:
            return ParsedStatement{};
    }
}

StatementType QueryEngine::GetStatementType(const std::string& sql) {
    std::string upper_sql = ToUpper(sql);
    
    if (upper_sql.find("SELECT") == 0) {
        return StatementType::SELECT;
    } else if (upper_sql.find("INSERT") == 0) {
        return StatementType::INSERT;
    } else if (upper_sql.find("CREATE TABLE") == 0) {
        return StatementType::CREATE_TABLE;
    }
    
    return StatementType::INVALID;
}

ParsedStatement QueryEngine::ParseSelect(const std::string& sql) {
    ParsedStatement stmt;
    stmt.type = StatementType::SELECT;
    
    std::vector<std::string> tokens = Tokenize(sql);
    
    // Simple parsing: SELECT * FROM table_name [WHERE column = value]
    for (size_t i = 0; i < tokens.size(); i++) {
        std::string token = ToUpper(tokens[i]);
        
        if (token == "FROM" && i + 1 < tokens.size()) {
            stmt.table_name = tokens[i + 1];
        } else if (token == "WHERE" && i + 3 < tokens.size()) {
            stmt.where_column = tokens[i + 1];
            stmt.where_operator = tokens[i + 2];
            stmt.where_value = ParseValue(tokens[i + 3]);
        }
    }
    
    return stmt;
}

ParsedStatement QueryEngine::ParseInsert(const std::string& sql) {
    ParsedStatement stmt;
    stmt.type = StatementType::INSERT;
    
    std::vector<std::string> tokens = Tokenize(sql);
    
    // Simple parsing: INSERT INTO table_name VALUES (val1, val2, ...)
    for (size_t i = 0; i < tokens.size(); i++) {
        std::string token = ToUpper(tokens[i]);
        
        if (token == "INTO" && i + 1 < tokens.size()) {
            stmt.table_name = tokens[i + 1];
        } else if (token == "VALUES") {
            // Parse values - simplified parsing
            std::vector<Value> row_values;
            for (size_t j = i + 1; j < tokens.size(); j++) {
                if (tokens[j] != "(" && tokens[j] != ")" && tokens[j] != ",") {
                    row_values.push_back(ParseValue(tokens[j]));
                }
            }
            if (!row_values.empty()) {
                stmt.values.push_back(row_values);
            }
            break;
        }
    }
    
    return stmt;
}

ParsedStatement QueryEngine::ParseCreateTable(const std::string& sql) {
    ParsedStatement stmt;
    stmt.type = StatementType::CREATE_TABLE;
    
    std::vector<std::string> tokens = Tokenize(sql);
    
    // Simple parsing: CREATE TABLE table_name (col1 type1, col2 type2, ...)
    for (size_t i = 0; i < tokens.size(); i++) {
        std::string token = ToUpper(tokens[i]);
        
        if (token == "TABLE" && i + 1 < tokens.size()) {
            stmt.table_name = tokens[i + 1];
            
            // Parse column definitions
            for (size_t j = i + 2; j < tokens.size(); j += 2) {
                if (tokens[j] == "(" || tokens[j] == ")" || tokens[j] == ",") {
                    continue;
                }
                
                if (j + 1 < tokens.size()) {
                    std::string col_name = tokens[j];
                    DataType col_type = ParseDataType(tokens[j + 1]);
                    
                    if (col_type != DataType::NULL_TYPE) {
                        uint32_t size = (col_type == DataType::VARCHAR) ? 255 : sizeof(int);
                        stmt.table_columns.emplace_back(col_name, col_type, size);
                    }
                }
            }
            break;
        }
    }
    
    return stmt;
}

QueryResult QueryEngine::ExecuteSelect(const ParsedStatement& stmt) {
    QueryResult result;
    
    auto table = GetTable(stmt.table_name);
    if (!table) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    // Create predicate if WHERE clause exists
    std::unique_ptr<Expression> predicate = nullptr;
    if (!stmt.where_column.empty()) {
        auto column_expr = std::make_unique<ColumnExpression>(stmt.where_column);
        auto constant_expr = std::make_unique<ConstantExpression>(stmt.where_value);
        
        ComparisonExpression::ComparisonType comp_type = ComparisonExpression::EQUAL;
        if (stmt.where_operator == "=") {
            comp_type = ComparisonExpression::EQUAL;
        } else if (stmt.where_operator == "<") {
            comp_type = ComparisonExpression::LESS_THAN;
        } else if (stmt.where_operator == ">") {
            comp_type = ComparisonExpression::GREATER_THAN;
        }
        
        predicate = std::make_unique<ComparisonExpression>(comp_type, std::move(column_expr), std::move(constant_expr));
    }
    
    // Execute sequential scan
    SeqScanExecutor executor(context_.get(), stmt.table_name, std::move(predicate));
    executor.Init();
    
    Tuple tuple;
    RID rid;
    while (executor.Next(tuple, rid)) {
        result.tuples.push_back(tuple);
        result.rids.push_back(rid);
    }
    
    result.schema = std::make_unique<Schema>(executor.GetOutputSchema());
    result.success = true;
    
    return result;
}

QueryResult QueryEngine::ExecuteInsert(const ParsedStatement& stmt) {
    QueryResult result;
    
    auto table = GetTable(stmt.table_name);
    if (!table) {
        result.error_message = "Table not found: " + stmt.table_name;
        return result;
    }
    
    InsertExecutor executor(context_.get(), stmt.table_name, stmt.values);
    executor.Init();
    
    Tuple result_tuple;
    RID rid;
    if (executor.Next(result_tuple, rid)) {
        result.affected_rows = executor.GetInsertedCount();
        result.success = true;
    } else {
        result.error_message = "Insert failed";
    }
    
    return result;
}

QueryResult QueryEngine::ExecuteCreateTable(const ParsedStatement& stmt) {
    QueryResult result;
    
    if (CreateTable(stmt.table_name, stmt.table_columns)) {
        result.success = true;
        result.affected_rows = 1;
    } else {
        result.error_message = "Failed to create table: " + stmt.table_name;
    }
    
    return result;
}

std::vector<std::string> QueryEngine::Tokenize(const std::string& str) {
    std::vector<std::string> tokens;
    std::istringstream iss(str);
    std::string token;
    
    while (iss >> token) {
        // Remove punctuation and split on common delimiters
        std::string clean_token;
        for (char c : token) {
            if (c == '(' || c == ')' || c == ',' || c == ';') {
                if (!clean_token.empty()) {
                    tokens.push_back(clean_token);
                    clean_token.clear();
                }
                tokens.push_back(std::string(1, c));
            } else {
                clean_token += c;
            }
        }
        if (!clean_token.empty()) {
            tokens.push_back(clean_token);
        }
    }
    
    return tokens;
}

std::string QueryEngine::ToUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

Value QueryEngine::ParseValue(const std::string& str) {
    // Remove quotes if present
    std::string clean_str = str;
    if (clean_str.front() == '\'' && clean_str.back() == '\'') {
        clean_str = clean_str.substr(1, clean_str.length() - 2);
        return Value(DataType::VARCHAR, clean_str);
    }
    
    // Try to parse as integer
    try {
        int int_val = std::stoi(clean_str);
        return Value(DataType::INTEGER, int_val);
    } catch (...) {
        // Default to string
        return Value(DataType::VARCHAR, clean_str);
    }
}

DataType QueryEngine::ParseDataType(const std::string& type_str) {
    std::string upper_type = ToUpper(type_str);
    
    if (upper_type == "INT" || upper_type == "INTEGER") {
        return DataType::INTEGER;
    } else if (upper_type == "VARCHAR" || upper_type == "TEXT" || upper_type == "STRING") {
        return DataType::VARCHAR;
    } else if (upper_type == "BOOL" || upper_type == "BOOLEAN") {
        return DataType::BOOLEAN;
    }
    
    return DataType::NULL_TYPE;
}

} // namespace minidb
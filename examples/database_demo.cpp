#include "execution/query_engine.h"
#include "index/simple_btree.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk_manager.h"
#include <iostream>
#include <memory>

using namespace minidb;

void PrintQueryResult(const QueryResult& result) {
    if (!result.success) {
        std::cout << "Error: " << result.error_message << std::endl;
        return;
    }
    
    if (result.schema) {
        // Print column headers
        for (size_t i = 0; i < result.schema->GetColumnCount(); i++) {
            std::cout << result.schema->GetColumn(i).GetName();
            if (i < result.schema->GetColumnCount() - 1) {
                std::cout << "\t";
            }
        }
        std::cout << std::endl;
        
        // Print separator
        for (size_t i = 0; i < result.schema->GetColumnCount(); i++) {
            std::cout << "--------";
            if (i < result.schema->GetColumnCount() - 1) {
                std::cout << "\t";
            }
        }
        std::cout << std::endl;
        
        // Print tuples
        for (const auto& tuple : result.tuples) {
            for (size_t i = 0; i < tuple.GetSize(); i++) {
                std::cout << tuple.GetValue(i).ToString();
                if (i < tuple.GetSize() - 1) {
                    std::cout << "\t";
                }
            }
            std::cout << std::endl;
        }
    }
    
    if (result.affected_rows > 0) {
        std::cout << "Affected rows: " << result.affected_rows << std::endl;
    }
    
    std::cout << std::endl;
}

void DemoTableOperations(QueryEngine& engine) {
    std::cout << "=== Table Operations Demo ===" << std::endl;
    
    // Create a users table
    std::cout << "Creating users table..." << std::endl;
    auto result = engine.ExecuteQuery("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)");
    PrintQueryResult(result);
    
    // Insert some data
    std::cout << "Inserting users..." << std::endl;
    result = engine.ExecuteQuery("INSERT INTO users VALUES (1, 'Alice', 25)");
    PrintQueryResult(result);
    
    result = engine.ExecuteQuery("INSERT INTO users VALUES (2, 'Bob', 30)");
    PrintQueryResult(result);
    
    result = engine.ExecuteQuery("INSERT INTO users VALUES (3, 'Charlie', 35)");
    PrintQueryResult(result);
    
    // Query all users
    std::cout << "Selecting all users..." << std::endl;
    result = engine.ExecuteQuery("SELECT * FROM users");
    PrintQueryResult(result);
    
    // Query with WHERE clause
    std::cout << "Selecting users where age > 28..." << std::endl;
    result = engine.ExecuteQuery("SELECT * FROM users WHERE age > 28");
    PrintQueryResult(result);
}

void DemoBTreeOperations(BufferPoolManager* buffer_pool_manager) {
    std::cout << "=== B+ Tree Operations Demo ===" << std::endl;
    
    SimpleBTree btree(buffer_pool_manager);
    
    // Insert some keys
    std::cout << "Inserting keys into B+ tree..." << std::endl;
    std::vector<int> keys = {10, 20, 5, 15, 25, 30, 3, 7, 12, 18};
    
    for (int key : keys) {
        RID rid;
        rid.page_id = key; // Use key as page_id for demo
        rid.slot_num = 0;
        
        if (btree.Insert(key, rid)) {
            std::cout << "Inserted key: " << key << std::endl;
        } else {
            std::cout << "Failed to insert key: " << key << std::endl;
        }
    }
    
    // Search for keys
    std::cout << "\nSearching for keys..." << std::endl;
    std::vector<int> search_keys = {5, 15, 25, 100};
    
    for (int key : search_keys) {
        RID result;
        if (btree.Search(key, &result)) {
            std::cout << "Found key " << key << " -> RID(" << result.page_id << ", " << result.slot_num << ")" << std::endl;
        } else {
            std::cout << "Key " << key << " not found" << std::endl;
        }
    }
    
    // Range scan
    std::cout << "\nRange scan [10, 25]..." << std::endl;
    std::vector<RID> range_results;
    int count = btree.RangeScan(10, 25, range_results);
    std::cout << "Found " << count << " keys in range:" << std::endl;
    for (const auto& rid : range_results) {
        std::cout << "  RID(" << rid.page_id << ", " << rid.slot_num << ")" << std::endl;
    }
    
    // Get first N keys
    std::cout << "\nFirst 5 keys..." << std::endl;
    std::vector<RID> first_results;
    count = btree.GetFirst(5, first_results);
    std::cout << "First " << count << " keys:" << std::endl;
    for (const auto& rid : first_results) {
        std::cout << "  RID(" << rid.page_id << ", " << rid.slot_num << ")" << std::endl;
    }
    
    std::cout << std::endl;
}

void DemoComplexQueries(QueryEngine& engine) {
    std::cout << "=== Complex Query Demo ===" << std::endl;
    
    // Create products table
    std::cout << "Creating products table..." << std::endl;
    auto result = engine.ExecuteQuery("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)");
    PrintQueryResult(result);
    
    // Insert products
    std::cout << "Inserting products..." << std::endl;
    result = engine.ExecuteQuery("INSERT INTO products VALUES (1, 'Laptop', 1000)");
    PrintQueryResult(result);
    
    result = engine.ExecuteQuery("INSERT INTO products VALUES (2, 'Mouse', 25)");
    PrintQueryResult(result);
    
    result = engine.ExecuteQuery("INSERT INTO products VALUES (3, 'Keyboard', 75)");
    PrintQueryResult(result);
    
    result = engine.ExecuteQuery("INSERT INTO products VALUES (4, 'Monitor', 300)");
    PrintQueryResult(result);
    
    // Query expensive products
    std::cout << "Selecting products with price > 50..." << std::endl;
    result = engine.ExecuteQuery("SELECT * FROM products WHERE price > 50");
    PrintQueryResult(result);
    
    // Show all tables
    std::cout << "Available tables:" << std::endl;
    auto table_names = engine.GetTableNames();
    for (const auto& name : table_names) {
        std::cout << "  - " << name << std::endl;
    }
    std::cout << std::endl;
}

int main() {
    std::cout << "MiniDB Database Engine Demo" << std::endl;
    std::cout << "===========================" << std::endl << std::endl;
    
    try {
        // Initialize storage components
        auto disk_manager = std::make_unique<DiskManager>("demo.db");
        auto buffer_pool_manager = std::make_unique<BufferPoolManager>(128, disk_manager.get());
        
        // Initialize query engine
        QueryEngine engine(buffer_pool_manager.get());
        
        // Demo 1: Table operations
        DemoTableOperations(engine);
        
        // Demo 2: B+ Tree operations
        DemoBTreeOperations(buffer_pool_manager.get());
        
        // Demo 3: Complex queries
        DemoComplexQueries(engine);
        
        std::cout << "Demo completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
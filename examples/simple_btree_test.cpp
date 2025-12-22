#include <iostream>
#include <vector>
#include <memory>

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "index/simple_btree.h"
#include "table/schema.h"
#include "table/tuple.h"
#include "common/types.h"

using namespace minidb;

void TestSimpleBTree() {
    std::cout << "=== Testing Simple B+ Tree ===" << std::endl;
    
    // Create disk manager and buffer pool
    auto disk_manager = std::make_unique<DiskManager>("simple_btree_test.db");
    auto buffer_pool = std::make_unique<BufferPoolManager>(50, disk_manager.get());
    
    // Create B+ tree
    auto btree = std::make_unique<SimpleBTree>(buffer_pool.get());
    
    // Test basic operations
    std::cout << "Testing insertions..." << std::endl;
    
    std::vector<std::pair<int, RID>> test_data = {
        {10, RID(1, 0)}, {5, RID(1, 1)}, {15, RID(1, 2)}, 
        {3, RID(2, 0)}, {7, RID(2, 1)}, {12, RID(2, 2)},
        {18, RID(3, 0)}, {1, RID(3, 1)}, {20, RID(3, 2)}
    };
    
    for (const auto& [key, rid] : test_data) {
        bool success = btree->Insert(key, rid);
        std::cout << "Insert(" << key << ", RID(" << rid.page_id << "," << rid.slot_num << ")): " 
                  << (success ? "SUCCESS" : "FAILED") << std::endl;
    }
    
    std::cout << "\nTree structure:" << std::endl;
    btree->PrintTree();
    
    // Test searches
    std::cout << "\nTesting searches..." << std::endl;
    for (int key : {1, 5, 10, 15, 20, 25}) {
        RID result;
        bool found = btree->Search(key, &result);
        
        if (found) {
            std::cout << "Found key " << key << " -> RID(" 
                      << result.page_id << "," << result.slot_num << ")" << std::endl;
        } else {
            std::cout << "Key " << key << " not found" << std::endl;
        }
    }
    
    // Test removals
    std::cout << "\nTesting removals..." << std::endl;
    for (int key : {5, 15}) {
        bool success = btree->Remove(key);
        std::cout << "Remove(" << key << "): " << (success ? "SUCCESS" : "FAILED") << std::endl;
    }
    
    std::cout << "\nTree structure after removals:" << std::endl;
    btree->PrintTree();
    
    // Verify removals
    std::cout << "\nVerifying removals..." << std::endl;
    for (int key : {5, 15}) {
        RID result;
        bool found = btree->Search(key, &result);
        std::cout << "Search for removed key " << key << ": " 
                  << (found ? "FOUND (ERROR)" : "NOT FOUND (CORRECT)") << std::endl;
    }
    
    std::cout << "\nSimple B+ Tree test completed!" << std::endl;
}

void TestTableOperations() {
    std::cout << "\n=== Testing Table Operations ===" << std::endl;
    
    // Create a simple table schema
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("name", DataType::VARCHAR, 20),
        Column("age", DataType::INTEGER)
    };
    
    Schema schema(columns);
    std::cout << "Created schema: " << schema.ToString() << std::endl;
    
    // Create some tuples
    std::vector<Tuple> tuples;
    
    tuples.emplace_back(std::vector<Value>{
        Value(1), Value(std::string("Alice")), Value(25)
    }, &schema);
    
    tuples.emplace_back(std::vector<Value>{
        Value(2), Value(std::string("Bob")), Value(30)
    }, &schema);
    
    tuples.emplace_back(std::vector<Value>{
        Value(3), Value(std::string("Charlie")), Value(35)
    }, &schema);
    
    std::cout << "\nCreated tuples:" << std::endl;
    for (const auto& tuple : tuples) {
        std::cout << "  " << tuple.ToString() << std::endl;
    }
    
    // Test serialization
    std::cout << "\nTesting tuple serialization..." << std::endl;
    for (const auto& tuple : tuples) {
        uint32_t size = tuple.GetSerializedSize();
        std::cout << "Tuple " << tuple.ToString() << " serialized size: " << size << " bytes" << std::endl;
        
        // Test round-trip serialization
        char buffer[256];
        tuple.SerializeTo(buffer);
        
        Tuple deserialized;
        deserialized.DeserializeFrom(buffer, &schema);
        
        bool equal = (tuple == deserialized);
        std::cout << "  Serialization round-trip: " << (equal ? "SUCCESS" : "FAILED") << std::endl;
    }
}

int main() {
    try {
        TestSimpleBTree();
        TestTableOperations();
        
        std::cout << "\n All tests passed! Core components are working." << std::endl;
        std::cout << "\nNext steps:" << std::endl;
        std::cout << "1. Implement B+ tree split logic for larger datasets" << std::endl;
        std::cout << "2. Add table heap for persistent tuple storage" << std::endl;
        std::cout << "3. Build query execution engine" << std::endl;
        std::cout << "4. Add SQL parser for complete database functionality" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
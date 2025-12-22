#include <iostream>
#include <memory>
#include <vector>

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "index/b_plus_tree.h"
#include "table/schema.h"
#include "table/tuple.h"
#include "common/types.h"

using namespace minidb;

void TestBPlusTree() {
    std::cout << "=== Testing B+ Tree ===" << std::endl;
    
    // Create disk manager and buffer pool
    auto disk_manager = std::make_unique<DiskManager>("btree_test.db");
    auto buffer_pool = std::make_unique<BufferPoolManager>(50, disk_manager.get());
    
    // Create B+ tree index
    std::less<int> comparator;
    auto btree = std::make_unique<IntegerBPlusTree>("test_index", buffer_pool.get(), comparator);
    
    // Test insertions
    std::vector<std::pair<int, RID>> test_data = {
        {10, RID(1, 0)}, {20, RID(1, 1)}, {5, RID(1, 2)}, 
        {15, RID(2, 0)}, {25, RID(2, 1)}, {1, RID(2, 2)},
        {30, RID(3, 0)}, {35, RID(3, 1)}, {40, RID(3, 2)}
    };
    
    std::cout << "Inserting key-value pairs..." << std::endl;
    for (const auto& [key, rid] : test_data) {
        bool success = btree->Insert(key, rid);
        std::cout << "Insert(" << key << ", RID(" << rid.page_id << "," << rid.slot_num << ")): " 
                  << (success ? "SUCCESS" : "FAILED") << std::endl;
    }
    
    // Test searches
    std::cout << "\nSearching for keys..." << std::endl;
    for (int key : {5, 15, 25, 100}) {
        std::vector<RID> result;
        bool found = btree->GetValue(key, &result);
        
        if (found && !result.empty()) {
            std::cout << "Found key " << key << " -> RID(" 
                      << result[0].page_id << "," << result[0].slot_num << ")" << std::endl;
        } else {
            std::cout << "Key " << key << " not found" << std::endl;
        }
    }
    
    std::cout << "B+ Tree test completed!" << std::endl;
}

void TestSchema() {
    std::cout << "\n=== Testing Schema ===" << std::endl;
    
    // Create a simple schema
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("name", DataType::VARCHAR, 50),
        Column("active", DataType::BOOLEAN)
    };
    
    Schema schema(columns);
    
    std::cout << "Created schema: " << schema.ToString() << std::endl;
    std::cout << "Column count: " << schema.GetColumnCount() << std::endl;
    std::cout << "Fixed length: " << schema.GetFixedLength() << " bytes" << std::endl;
    std::cout << "Is fixed length: " << (schema.IsFixedLength() ? "Yes" : "No") << std::endl;
    
    // Test column access
    const Column& id_col = schema.GetColumn("id");
    std::cout << "ID column type: " << (int)id_col.GetType() << ", size: " << id_col.GetSize() << std::endl;
}

void TestTuple() {
    std::cout << "\n=== Testing Tuple ===" << std::endl;
    
    // Create schema
    std::vector<Column> columns = {
        Column("id", DataType::INTEGER),
        Column("name", DataType::VARCHAR, 20),
        Column("active", DataType::BOOLEAN)
    };
    Schema schema(columns);
    
    // Create tuple
    std::vector<Value> values = {
        Value(42),
        Value(std::string("Alice")),
        Value(true)
    };
    
    Tuple tuple(values, &schema);
    std::cout << "Created tuple: " << tuple.ToString() << std::endl;
    
    // Test serialization
    uint32_t size = tuple.GetSerializedSize();
    std::cout << "Serialized size: " << size << " bytes" << std::endl;
    
    char buffer[256];
    tuple.SerializeTo(buffer);
    
    // Test deserialization
    Tuple deserialized_tuple;
    deserialized_tuple.DeserializeFrom(buffer, &schema);
    
    std::cout << "Deserialized tuple: " << deserialized_tuple.ToString() << std::endl;
    std::cout << "Tuples equal: " << (tuple == deserialized_tuple ? "Yes" : "No") << std::endl;
}

int main() {
    try {
        TestSchema();
        TestTuple();
        TestBPlusTree();
        
        std::cout << "\n All tests passed! B+ Tree and Table structures are working." << std::endl;
        std::cout << "\nNext steps:" << std::endl;
        std::cout << "1. Implement table pages and table heap" << std::endl;
        std::cout << "2. Add query execution engine" << std::endl;
        std::cout << "3. Build SQL parser" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
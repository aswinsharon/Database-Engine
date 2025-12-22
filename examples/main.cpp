#include <iostream>
#include <memory>
#include <cassert>

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "common/types.h"

using namespace minidb;

void TestDiskManager() {
    std::cout << "=== Testing DiskManager ===" << std::endl;
    
    // Create disk manager
    auto disk_manager = std::make_unique<DiskManager>("test.db");
    
    // Allocate some pages
    page_id_t page1 = disk_manager->AllocatePage();
    page_id_t page2 = disk_manager->AllocatePage();
    
    std::cout << "Allocated pages: " << page1 << ", " << page2 << std::endl;
    
    // Write some data
    char write_data[PAGE_SIZE];
    memset(write_data, 0, PAGE_SIZE);
    strcpy(write_data + PAGE_HEADER_SIZE, "Hello, MiniDB!");
    
    // Set page header
    *reinterpret_cast<page_id_t*>(write_data) = page1;
    *reinterpret_cast<PageType*>(write_data + sizeof(page_id_t)) = PageType::TABLE_PAGE;
    
    disk_manager->WritePage(page1, write_data);
    
    // Read it back
    char read_data[PAGE_SIZE];
    disk_manager->ReadPage(page1, read_data);
    
    std::string content(read_data + PAGE_HEADER_SIZE);
    std::cout << "Read back: " << content << std::endl;
    
    assert(content == "Hello, MiniDB!");
    std::cout << "DiskManager test passed!" << std::endl;
}

void TestBufferPoolManager() {
    std::cout << "\n=== Testing BufferPoolManager ===" << std::endl;
    
    // Create disk manager and buffer pool
    auto disk_manager = std::make_unique<DiskManager>("test_buffer.db");
    auto buffer_pool = std::make_unique<BufferPoolManager>(10, disk_manager.get());
    
    // Create a new page
    page_id_t page_id;
    Page* page = buffer_pool->NewPage(&page_id);
    assert(page != nullptr);
    
    std::cout << "Created new page: " << page_id << std::endl;
    
    // Write some data to the page
    strcpy(page->GetDataArea(), "Buffer Pool Test Data");
    
    // Unpin the page (mark as dirty)
    bool success = buffer_pool->UnpinPage(page_id, true);
    assert(success);
    
    // Fetch the page again
    Page* fetched_page = buffer_pool->FetchPage(page_id);
    assert(fetched_page != nullptr);
    assert(fetched_page->GetPageId() == page_id);
    
    std::string content(fetched_page->GetDataArea());
    std::cout << "Fetched content: " << content << std::endl;
    
    assert(content == "Buffer Pool Test Data");
    
    // Unpin again
    buffer_pool->UnpinPage(page_id, false);
    
    std::cout << "BufferPoolManager test passed!" << std::endl;
}

void TestValue() {
    std::cout << "\n=== Testing Value ===" << std::endl;
    
    // Test integer value
    Value int_val(42);
    assert(int_val.GetType() == Value::INTEGER);
    assert(int_val.GetInteger() == 42);
    std::cout << "Integer value: " << int_val.ToString() << std::endl;
    
    // Test string value
    Value str_val(std::string("Hello"));
    assert(str_val.GetType() == Value::VARCHAR);
    assert(str_val.GetString() == "Hello");
    std::cout << "String value: " << str_val.ToString() << std::endl;
    
    // Test boolean value
    Value bool_val(true);
    assert(bool_val.GetType() == Value::BOOLEAN);
    assert(bool_val.GetBoolean() == true);
    std::cout << "Boolean value: " << bool_val.ToString() << std::endl;
    
    // Test serialization
    char buffer[256];
    uint32_t size = int_val.SerializeTo(buffer);
    
    Value deserialized_val;
    uint32_t read_size = deserialized_val.DeserializeFrom(buffer);
    
    assert(size == read_size);
    assert(deserialized_val == int_val);
    
    std::cout << "Value serialization test passed!" << std::endl;
}

int main() {
    try {
        TestValue();
        TestDiskManager();
        TestBufferPoolManager();
        
        std::cout << "\n All tests passed! MiniDB storage layer is working correctly." << std::endl;
        std::cout << "\nNext steps:" << std::endl;
        std::cout << "1. Implement B+ Tree index" << std::endl;
        std::cout << "2. Add table and row storage" << std::endl;
        std::cout << "3. Build query execution engine" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
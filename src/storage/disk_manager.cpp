#include "storage/disk_manager.h"
#include <iostream>
#include <cassert>

namespace minidb {

DiskManager::DiskManager(const std::string& db_file) 
    : file_name_(db_file), num_pages_(0) {
    
    // Try to open existing file first
    db_file_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    
    if (!db_file_.is_open()) {
        // File doesn't exist, create new one
        db_file_.clear();
        db_file_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_file_.close();
        
        // Reopen in read/write mode
        db_file_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        
        if (db_file_.is_open()) {
            InitializeHeaderPage();
        }
    } else {
        // Existing file, read header
        ReadHeaderPage();
    }
    
    if (!db_file_.is_open()) {
        throw std::runtime_error("Cannot open database file: " + db_file);
    }
}

DiskManager::~DiskManager() {
    if (db_file_.is_open()) {
        WriteHeaderPage();  // Save metadata
        db_file_.close();
    }
}

void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
    assert(page_data != nullptr);
    
    std::lock_guard<std::mutex> lock(db_io_latch_);
    
    if (page_id >= num_pages_) {
        throw std::runtime_error("Page ID out of range: " + std::to_string(page_id));
    }
    
    size_t offset = GetFileOffset(page_id);
    db_file_.seekg(offset);
    
    if (db_file_.fail()) {
        throw std::runtime_error("Failed to seek to page: " + std::to_string(page_id));
    }
    
    db_file_.read(page_data, PAGE_SIZE);
    
    if (db_file_.fail()) {
        throw std::runtime_error("Failed to read page: " + std::to_string(page_id));
    }
    
    // Verify page ID matches
    page_id_t stored_page_id = *reinterpret_cast<page_id_t*>(page_data);
    if (stored_page_id != page_id && page_id != HEADER_PAGE_ID) {
        std::cerr << "Warning: Page ID mismatch. Expected: " << page_id 
                  << ", Found: " << stored_page_id << std::endl;
    }
}

void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
    assert(page_data != nullptr);
    
    std::lock_guard<std::mutex> lock(db_io_latch_);
    
    size_t offset = GetFileOffset(page_id);
    db_file_.seekp(offset);
    
    if (db_file_.fail()) {
        throw std::runtime_error("Failed to seek to page: " + std::to_string(page_id));
    }
    
    db_file_.write(page_data, PAGE_SIZE);
    
    if (db_file_.fail()) {
        throw std::runtime_error("Failed to write page: " + std::to_string(page_id));
    }
    
    // Ensure data is written to disk
    db_file_.flush();
    
    // Update num_pages if this is a new page
    if (page_id >= num_pages_) {
        num_pages_ = page_id + 1;
    }
}

page_id_t DiskManager::AllocatePage() {
    std::lock_guard<std::mutex> lock(db_io_latch_);
    
    page_id_t new_page_id;
    
    if (!free_list_.empty()) {
        // Reuse a freed page
        new_page_id = free_list_.back();
        free_list_.pop_back();
    } else {
        // Allocate a new page at the end
        new_page_id = num_pages_;
        num_pages_++;
    }
    
    return new_page_id;
}

void DiskManager::DeallocatePage(page_id_t page_id) {
    std::lock_guard<std::mutex> lock(db_io_latch_);
    
    if (page_id == HEADER_PAGE_ID) {
        throw std::runtime_error("Cannot deallocate header page");
    }
    
    if (page_id >= num_pages_) {
        throw std::runtime_error("Page ID out of range: " + std::to_string(page_id));
    }
    
    // Add to free list
    free_list_.push_back(page_id);
}

void DiskManager::FlushLog() {
    std::lock_guard<std::mutex> lock(db_io_latch_);
    db_file_.flush();
}

void DiskManager::InitializeHeaderPage() {
    // Create header page with metadata
    char header_data[PAGE_SIZE];
    memset(header_data, 0, PAGE_SIZE);
    
    // Write magic number
    *reinterpret_cast<uint32_t*>(header_data) = MAGIC_NUMBER;
    
    // Write initial metadata
    *reinterpret_cast<uint32_t*>(header_data + 4) = 1;  // num_pages (header page)
    *reinterpret_cast<uint32_t*>(header_data + 8) = 0;  // free_list_size
    
    // Write header page to disk
    db_file_.seekp(0);
    db_file_.write(header_data, PAGE_SIZE);
    db_file_.flush();
    
    num_pages_ = 1;  // Header page
    free_list_.clear();
}

void DiskManager::ReadHeaderPage() {
    char header_data[PAGE_SIZE];
    
    db_file_.seekg(0);
    db_file_.read(header_data, PAGE_SIZE);
    
    if (db_file_.fail()) {
        throw std::runtime_error("Failed to read header page");
    }
    
    // Verify magic number
    uint32_t magic = *reinterpret_cast<uint32_t*>(header_data);
    if (magic != MAGIC_NUMBER) {
        throw std::runtime_error("Invalid database file format");
    }
    
    // Read metadata
    num_pages_ = *reinterpret_cast<uint32_t*>(header_data + 4);
    uint32_t free_list_size = *reinterpret_cast<uint32_t*>(header_data + 8);
    
    // Read free list
    free_list_.clear();
    free_list_.reserve(free_list_size);
    
    for (uint32_t i = 0; i < free_list_size; ++i) {
        page_id_t free_page = *reinterpret_cast<page_id_t*>(header_data + 12 + i * sizeof(page_id_t));
        free_list_.push_back(free_page);
    }
}

void DiskManager::WriteHeaderPage() {
    char header_data[PAGE_SIZE];
    memset(header_data, 0, PAGE_SIZE);
    
    // Write magic number
    *reinterpret_cast<uint32_t*>(header_data) = MAGIC_NUMBER;
    
    // Write metadata
    *reinterpret_cast<uint32_t*>(header_data + 4) = num_pages_;
    *reinterpret_cast<uint32_t*>(header_data + 8) = static_cast<uint32_t>(free_list_.size());
    
    // Write free list
    for (size_t i = 0; i < free_list_.size(); ++i) {
        *reinterpret_cast<page_id_t*>(header_data + 12 + i * sizeof(page_id_t)) = free_list_[i];
    }
    
    // Write to disk
    std::lock_guard<std::mutex> lock(db_io_latch_);
    db_file_.seekp(0);
    db_file_.write(header_data, PAGE_SIZE);
    db_file_.flush();
}

}  // namespace minidb
#pragma once

#include <cstdint>
#include <limits>

namespace minidb {

// Page configuration
static constexpr uint32_t PAGE_SIZE = 4096;  // 4KB pages
static constexpr uint32_t PAGE_HEADER_SIZE = 24;
static constexpr uint32_t PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

// Buffer pool configuration
static constexpr uint32_t DEFAULT_BUFFER_POOL_SIZE = 128;  // 128 pages = 512KB

// Invalid values
static constexpr uint32_t INVALID_PAGE_ID = std::numeric_limits<uint32_t>::max();
static constexpr uint32_t INVALID_FRAME_ID = std::numeric_limits<uint32_t>::max();
static constexpr uint64_t INVALID_LSN = std::numeric_limits<uint64_t>::max();

// Page types
enum class PageType : uint8_t {
    INVALID = 0,
    TABLE_PAGE = 1,
    INDEX_INTERNAL_PAGE = 2,
    INDEX_LEAF_PAGE = 3,
    HEADER_PAGE = 4
};

// Data types
enum class DataType : uint8_t {
    INTEGER = 0,
    VARCHAR = 1,
    BOOLEAN = 2
};

// B+ Tree configuration
static constexpr uint32_t INTERNAL_PAGE_HEADER_SIZE = 16;
static constexpr uint32_t LEAF_PAGE_HEADER_SIZE = 20;

}  // namespace minidb
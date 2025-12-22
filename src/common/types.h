#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "common/config.h"

namespace minidb {

using page_id_t = uint32_t;
using frame_id_t = uint32_t;
using lsn_t = uint64_t;
using txn_id_t = uint64_t;
using slot_offset_t = uint16_t;

// Forward declarations
class Page;
class DiskManager;
class BufferPoolManager;
class Table;

// Smart pointer aliases
using PagePtr = std::shared_ptr<Page>;
using DiskManagerPtr = std::unique_ptr<DiskManager>;
using BufferPoolManagerPtr = std::unique_ptr<BufferPoolManager>;

// Value type for database values
class Value {
public:
    enum Type { INTEGER, VARCHAR, BOOLEAN, NULL_TYPE };
    
    Value() : type_(NULL_TYPE) {}
    explicit Value(int32_t val) : type_(INTEGER), int_val_(val) {}
    explicit Value(const std::string& val) : type_(VARCHAR), str_val_(val) {}
    explicit Value(bool val) : type_(BOOLEAN), bool_val_(val) {}
    
    Type GetType() const { return type_; }
    int32_t GetInteger() const { return int_val_; }
    const std::string& GetString() const { return str_val_; }
    bool GetBoolean() const { return bool_val_; }
    
    bool IsNull() const { return type_ == NULL_TYPE; }
    
    // Comparison operators
    bool operator==(const Value& other) const;
    bool operator<(const Value& other) const;
    bool operator>(const Value& other) const;
    
    // Serialization
    uint32_t SerializeTo(char* data) const;
    uint32_t DeserializeFrom(const char* data);
    uint32_t GetSerializedSize() const;
    
    std::string ToString() const;

private:
    Type type_;
    union {
        int32_t int_val_;
        bool bool_val_;
    };
    std::string str_val_;
};

// Row identifier
struct RID {
    page_id_t page_id;
    slot_offset_t slot_num;
    
    RID() : page_id(INVALID_PAGE_ID), slot_num(0) {}
    RID(page_id_t page_id, slot_offset_t slot_num) 
        : page_id(page_id), slot_num(slot_num) {}
    
    bool IsValid() const { return page_id != INVALID_PAGE_ID; }
    
    bool operator==(const RID& other) const {
        return page_id == other.page_id && slot_num == other.slot_num;
    }
};

}  // namespace minidb
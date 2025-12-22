#pragma once

#include "common/config.h"
#include "common/types.h"
#include <vector>
#include <string>

namespace minidb {

/**
 * Column definition in a table schema
 */
class Column {
public:
    Column(std::string name, DataType type, uint32_t size = 0)
        : name_(std::move(name)), type_(type), size_(size) {
        
        // Set default sizes for fixed-length types
        if (type_ == DataType::INTEGER && size_ == 0) {
            size_ = sizeof(int32_t);
        } else if (type_ == DataType::BOOLEAN && size_ == 0) {
            size_ = sizeof(bool);
        }
    }

    const std::string& GetName() const { return name_; }
    DataType GetType() const { return type_; }
    uint32_t GetSize() const { return size_; }
    
    bool IsFixedLength() const {
        return type_ == DataType::INTEGER || type_ == DataType::BOOLEAN;
    }

private:
    std::string name_;
    DataType type_;
    uint32_t size_;  // For VARCHAR, this is max length; for others, actual size
};

/**
 * Schema defines the structure of a table
 */
class Schema {
public:
    explicit Schema(const std::vector<Column>& columns);
    
    const std::vector<Column>& GetColumns() const { return columns_; }
    const Column& GetColumn(uint32_t col_idx) const;
    const Column& GetColumn(const std::string& col_name) const;
    
    uint32_t GetColumnCount() const { return static_cast<uint32_t>(columns_.size()); }
    uint32_t GetColumnIndex(const std::string& col_name) const;
    
    // Get the fixed-length portion size (for tuple header calculation)
    uint32_t GetFixedLength() const { return fixed_length_; }
    
    // Check if all columns are fixed-length
    bool IsFixedLength() const;
    
    std::string ToString() const;

private:
    std::vector<Column> columns_;
    uint32_t fixed_length_;  // Total size of fixed-length columns
    
    void CalculateFixedLength();
};

}  // namespace minidb
#pragma once

#include "common/config.h"
#include "common/types.h"
#include "table/schema.h"
#include <vector>
#include <memory>

namespace minidb {

/**
 * Tuple represents a row of data in a table.
 * It contains the actual data values and provides methods for serialization.
 */
class Tuple {
public:
    // Constructors
    Tuple() = default;
    Tuple(std::vector<Value> values, const Schema* schema);
    
    // Copy constructor and assignment
    Tuple(const Tuple& other);
    Tuple& operator=(const Tuple& other);
    
    // Move constructor and assignment
    Tuple(Tuple&& other) noexcept;
    Tuple& operator=(Tuple&& other) noexcept;
    
    ~Tuple() = default;

    // Value access
    const Value& GetValue(uint32_t col_idx) const;
    void SetValue(uint32_t col_idx, const Value& value);
    
    // Schema access
    const Schema* GetSchema() const { return schema_; }
    void SetSchema(const Schema* schema) { schema_ = schema; }
    
    // Serialization
    void SerializeTo(char* storage) const;
    void DeserializeFrom(const char* storage, const Schema* schema);
    uint32_t GetSerializedSize() const;
    
    // Utility
    std::string ToString() const;
    bool IsValid() const { return schema_ != nullptr; }
    
    // Comparison (for indexing)
    bool operator==(const Tuple& other) const;

private:
    std::vector<Value> values_;
    const Schema* schema_{nullptr};
    
    void ValidateColumnIndex(uint32_t col_idx) const;
};

/**
 * TupleIterator provides iteration over tuples in a page
 */
class TupleIterator {
public:
    TupleIterator(const char* data, const Schema* schema, uint32_t offset = 0);
    
    bool HasNext() const;
    Tuple GetNext();
    void Reset();
    
private:
    const char* data_;
    const Schema* schema_;
    uint32_t current_offset_;
    uint32_t start_offset_;
};

}  // namespace minidb
#include "table/tuple.h"
#include <sstream>
#include <stdexcept>
#include <cstring>

namespace minidb {

Tuple::Tuple(std::vector<Value> values, const Schema* schema)
    : values_(std::move(values)), schema_(schema) {
    
    if (schema_ && values_.size() != schema_->GetColumnCount()) {
        throw std::runtime_error("Value count doesn't match schema column count");
    }
}

Tuple::Tuple(const Tuple& other) : values_(other.values_), schema_(other.schema_) {}

Tuple& Tuple::operator=(const Tuple& other) {
    if (this != &other) {
        values_ = other.values_;
        schema_ = other.schema_;
    }
    return *this;
}

Tuple::Tuple(Tuple&& other) noexcept 
    : values_(std::move(other.values_)), schema_(other.schema_) {
    other.schema_ = nullptr;
}

Tuple& Tuple::operator=(Tuple&& other) noexcept {
    if (this != &other) {
        values_ = std::move(other.values_);
        schema_ = other.schema_;
        other.schema_ = nullptr;
    }
    return *this;
}

const Value& Tuple::GetValue(uint32_t col_idx) const {
    ValidateColumnIndex(col_idx);
    return values_[col_idx];
}

void Tuple::SetValue(uint32_t col_idx, const Value& value) {
    ValidateColumnIndex(col_idx);
    values_[col_idx] = value;
}

void Tuple::SerializeTo(char* storage) const {
    if (!IsValid()) {
        throw std::runtime_error("Cannot serialize invalid tuple");
    }
    
    uint32_t offset = 0;
    
    // Write tuple header (size + flags)
    uint32_t tuple_size = GetSerializedSize();
    *reinterpret_cast<uint32_t*>(storage + offset) = tuple_size;
    offset += sizeof(uint32_t);
    
    uint32_t flags = 0;  // No flags for now
    *reinterpret_cast<uint32_t*>(storage + offset) = flags;
    offset += sizeof(uint32_t);
    
    // Write values
    for (const auto& value : values_) {
        offset += value.SerializeTo(storage + offset);
    }
}

void Tuple::DeserializeFrom(const char* storage, const Schema* schema) {
    schema_ = schema;
    uint32_t offset = 0;
    
    // Read tuple header
    uint32_t tuple_size = *reinterpret_cast<const uint32_t*>(storage + offset);
    offset += sizeof(uint32_t);
    
    uint32_t flags = *reinterpret_cast<const uint32_t*>(storage + offset);
    offset += sizeof(uint32_t);
    
    // Read values
    values_.clear();
    values_.reserve(schema_->GetColumnCount());
    
    for (uint32_t i = 0; i < schema_->GetColumnCount(); ++i) {
        Value value;
        offset += value.DeserializeFrom(storage + offset);
        values_.push_back(value);
    }
}

uint32_t Tuple::GetSerializedSize() const {
    if (!IsValid()) {
        return 0;
    }
    
    uint32_t size = 2 * sizeof(uint32_t);  // Header (size + flags)
    
    for (const auto& value : values_) {
        size += value.GetSerializedSize();
    }
    
    return size;
}

std::string Tuple::ToString() const {
    if (!IsValid()) {
        return "Invalid Tuple";
    }
    
    std::ostringstream oss;
    oss << "(";
    
    for (size_t i = 0; i < values_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << values_[i].ToString();
    }
    
    oss << ")";
    return oss.str();
}

bool Tuple::operator==(const Tuple& other) const {
    if (schema_ != other.schema_ || values_.size() != other.values_.size()) {
        return false;
    }
    
    for (size_t i = 0; i < values_.size(); ++i) {
        if (!(values_[i] == other.values_[i])) {
            return false;
        }
    }
    
    return true;
}

void Tuple::ValidateColumnIndex(uint32_t col_idx) const {
    if (!IsValid()) {
        throw std::runtime_error("Tuple is not valid");
    }
    
    if (col_idx >= values_.size()) {
        throw std::out_of_range("Column index out of range");
    }
}

// TupleIterator implementation
TupleIterator::TupleIterator(const char* data, const Schema* schema, uint32_t offset)
    : data_(data), schema_(schema), current_offset_(offset), start_offset_(offset) {}

bool TupleIterator::HasNext() const {
    // Simple implementation - in a real system, we'd check page boundaries
    return current_offset_ < PAGE_DATA_SIZE;
}

Tuple TupleIterator::GetNext() {
    if (!HasNext()) {
        throw std::runtime_error("No more tuples");
    }
    
    Tuple tuple;
    tuple.DeserializeFrom(data_ + current_offset_, schema_);
    
    current_offset_ += tuple.GetSerializedSize();
    
    return tuple;
}

void TupleIterator::Reset() {
    current_offset_ = start_offset_;
}

}  // namespace minidb
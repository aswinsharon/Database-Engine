#include "table/tuple.h"
#include <sstream>
#include <stdexcept>
#include <cstring>

namespace minidb {

/**
 * Constructor for Tuple with values and schema
 * @param values Vector of Value objects representing the tuple data
 * @param schema Pointer to the Schema that defines the structure
 * @throws std::runtime_error if value count doesn't match schema column count
 */
Tuple::Tuple(std::vector<Value> values, const Schema* schema)
    : values_(std::move(values)), schema_(schema) {
    
    if (schema_ && values_.size() != schema_->GetColumnCount()) {
        throw std::runtime_error("Value count doesn't match schema column count");
    }
}

/**
 * Copy constructor for Tuple
 * @param other The tuple to copy from
 */
Tuple::Tuple(const Tuple& other) : values_(other.values_), schema_(other.schema_) {}

/**
 * Copy assignment operator for Tuple
 * @param other The tuple to copy from
 * @return Reference to this tuple
 */
Tuple& Tuple::operator=(const Tuple& other) {
    if (this != &other) {
        values_ = other.values_;
        schema_ = other.schema_;
    }
    return *this;
}

/**
 * Move constructor for Tuple
 * @param other The tuple to move from
 */
Tuple::Tuple(Tuple&& other) noexcept 
    : values_(std::move(other.values_)), schema_(other.schema_) {
    other.schema_ = nullptr;
}

/**
 * Move assignment operator for Tuple
 * @param other The tuple to move from
 * @return Reference to this tuple
 */
Tuple& Tuple::operator=(Tuple&& other) noexcept {
    if (this != &other) {
        values_ = std::move(other.values_);
        schema_ = other.schema_;
        other.schema_ = nullptr;
    }
    return *this;
}

/**
 * Get a value from the tuple by column index
 * @param col_idx Zero-based column index
 * @return Reference to the Value at the specified column
 * @throws std::out_of_range if col_idx is invalid
 * @throws std::runtime_error if tuple is not valid
 */
const Value& Tuple::GetValue(uint32_t col_idx) const {
    ValidateColumnIndex(col_idx);
    return values_[col_idx];
}

/**
 * Set a value in the tuple at the specified column index
 * @param col_idx Zero-based column index
 * @param value The new Value to set
 * @throws std::out_of_range if col_idx is invalid
 * @throws std::runtime_error if tuple is not valid
 */
void Tuple::SetValue(uint32_t col_idx, const Value& value) {
    ValidateColumnIndex(col_idx);
    values_[col_idx] = value;
}

/**
 * Serialize the tuple to a byte buffer
 * Writes tuple header (size + flags) followed by serialized values
 * @param storage Buffer to write the serialized data (must be large enough)
 * @throws std::runtime_error if tuple is not valid
 */
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

/**
 * Deserialize a tuple from a byte buffer
 * Reads tuple header and then deserializes values according to schema
 * @param storage Buffer containing the serialized tuple data
 * @param schema Pointer to the Schema for interpreting the data
 */
void Tuple::DeserializeFrom(const char* storage, const Schema* schema) {
    schema_ = schema;
    uint32_t offset = 0;
    
    // Read tuple header
    uint32_t tuple_size = *reinterpret_cast<const uint32_t*>(storage + offset);
    offset += sizeof(uint32_t);
    
    uint32_t flags = *reinterpret_cast<const uint32_t*>(storage + offset);
    offset += sizeof(uint32_t);
    
    // Suppress unused variable warnings (these may be used in future versions)
    (void)tuple_size;
    (void)flags;
    
    // Read values
    values_.clear();
    values_.reserve(schema_->GetColumnCount());
    
    for (uint32_t i = 0; i < schema_->GetColumnCount(); ++i) {
        Value value;
        offset += value.DeserializeFrom(storage + offset);
        values_.push_back(value);
    }
}

/**
 * Calculate the size needed to serialize this tuple
 * @return Size in bytes required for serialization
 */
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

/**
 * Generate a string representation of the tuple
 * @return Human-readable string in format "(value1, value2, ...)"
 */
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

/**
 * Equality comparison operator for tuples
 * @param other The tuple to compare with
 * @return true if tuples have same schema and all values are equal
 */
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

/**
 * Validate that a column index is within valid range
 * @param col_idx The column index to validate
 * @throws std::runtime_error if tuple is not valid
 * @throws std::out_of_range if col_idx is out of range
 */
void Tuple::ValidateColumnIndex(uint32_t col_idx) const {
    if (!IsValid()) {
        throw std::runtime_error("Tuple is not valid");
    }
    
    if (col_idx >= values_.size()) {
        throw std::out_of_range("Column index out of range");
    }
}

// TupleIterator implementation

/**
 * Constructor for TupleIterator
 * @param data Pointer to the page data containing tuples
 * @param schema Pointer to the Schema for interpreting tuple data
 * @param offset Starting offset within the page data
 */
TupleIterator::TupleIterator(const char* data, const Schema* schema, uint32_t offset)
    : data_(data), schema_(schema), current_offset_(offset), start_offset_(offset) {}

/**
 * Check if there are more tuples to iterate over
 * @return true if more tuples are available, false otherwise
 */
bool TupleIterator::HasNext() const {
    // Simple implementation - in a real system, we'd check page boundaries
    return current_offset_ < PAGE_DATA_SIZE;
}

/**
 * Get the next tuple from the iterator
 * @return The next Tuple object
 * @throws std::runtime_error if no more tuples are available
 */
Tuple TupleIterator::GetNext() {
    if (!HasNext()) {
        throw std::runtime_error("No more tuples");
    }
    
    Tuple tuple;
    tuple.DeserializeFrom(data_ + current_offset_, schema_);
    
    current_offset_ += tuple.GetSerializedSize();
    
    return tuple;
}

/**
 * Reset the iterator to the beginning
 * Sets the current position back to the starting offset
 */
void TupleIterator::Reset() {
    current_offset_ = start_offset_;
}

}  // namespace minidb
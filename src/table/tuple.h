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
 * Tuples are the fundamental unit of data storage and retrieval in the database.
 */
class Tuple {
public:
    /**
     * Default constructor - creates an invalid tuple
     */
    Tuple() = default;
    
    /**
     * Constructor with values and schema
     * @param values Vector of Value objects representing the row data
     * @param schema Pointer to the Schema defining the structure
     */
    Tuple(std::vector<Value> values, const Schema* schema);
    
    /**
     * Copy constructor
     * @param other The tuple to copy from
     */
    Tuple(const Tuple& other);
    
    /**
     * Copy assignment operator
     * @param other The tuple to copy from
     * @return Reference to this tuple
     */
    Tuple& operator=(const Tuple& other);
    
    /**
     * Move constructor
     * @param other The tuple to move from
     */
    Tuple(Tuple&& other) noexcept;
    
    /**
     * Move assignment operator
     * @param other The tuple to move from
     * @return Reference to this tuple
     */
    Tuple& operator=(Tuple&& other) noexcept;
    
    /**
     * Destructor
     */
    ~Tuple() = default;

    // Value access
    /**
     * Get a value by column index
     * @param col_idx Zero-based column index
     * @return Reference to the Value at the specified column
     * @throws std::out_of_range if col_idx is invalid
     */
    const Value& GetValue(uint32_t col_idx) const;
    
    /**
     * Set a value at the specified column index
     * @param col_idx Zero-based column index
     * @param value The new Value to set
     * @throws std::out_of_range if col_idx is invalid
     */
    void SetValue(uint32_t col_idx, const Value& value);
    
    // Schema access
    /**
     * Get the schema associated with this tuple
     * @return Pointer to the Schema, or nullptr if not set
     */
    const Schema* GetSchema() const { return schema_; }
    
    /**
     * Set the schema for this tuple
     * @param schema Pointer to the Schema to associate with this tuple
     */
    void SetSchema(const Schema* schema) { schema_ = schema; }
    
    // Serialization
    /**
     * Serialize the tuple to a byte buffer
     * @param storage Buffer to write the serialized data (must be large enough)
     * @throws std::runtime_error if tuple is invalid
     */
    void SerializeTo(char* storage) const;
    
    /**
     * Deserialize a tuple from a byte buffer
     * @param storage Buffer containing the serialized tuple data
     * @param schema Pointer to the Schema for interpreting the data
     */
    void DeserializeFrom(const char* storage, const Schema* schema);
    
    /**
     * Calculate the size needed to serialize this tuple
     * @return Size in bytes required for serialization
     */
    uint32_t GetSerializedSize() const;
    
    // Utility
    /**
     * Generate a string representation of the tuple
     * @return Human-readable string representation
     */
    std::string ToString() const;
    
    /**
     * Check if the tuple is valid (has a schema)
     * @return true if tuple has a valid schema, false otherwise
     */
    bool IsValid() const { return schema_ != nullptr; }
    
    /**
     * Equality comparison operator
     * @param other The tuple to compare with
     * @return true if tuples are equal (same schema and values)
     */
    bool operator==(const Tuple& other) const;

private:
    std::vector<Value> values_;    ///< The actual data values in the tuple
    const Schema* schema_{nullptr}; ///< Pointer to the schema defining the structure
    
    /**
     * Validate that a column index is within valid range
     * @param col_idx The column index to validate
     * @throws std::runtime_error if tuple is invalid
     * @throws std::out_of_range if col_idx is out of range
     */
    void ValidateColumnIndex(uint32_t col_idx) const;
};

/**
 * TupleIterator provides iteration over tuples stored in a page
 * Allows sequential access to tuples without loading them all into memory
 */
class TupleIterator {
public:
    /**
     * Constructor for TupleIterator
     * @param data Pointer to the page data containing tuples
     * @param schema Pointer to the Schema for interpreting tuple data
     * @param offset Starting offset within the page data (default: 0)
     */
    TupleIterator(const char* data, const Schema* schema, uint32_t offset = 0);
    
    /**
     * Check if there are more tuples to iterate over
     * @return true if more tuples are available, false otherwise
     */
    bool HasNext() const;
    
    /**
     * Get the next tuple from the iterator
     * @return The next Tuple object
     * @throws std::runtime_error if no more tuples are available
     */
    Tuple GetNext();
    
    /**
     * Reset the iterator to the beginning
     * Sets the current position back to the starting offset
     */
    void Reset();
    
private:
    const char* data_;        ///< Pointer to the page data
    const Schema* schema_;    ///< Pointer to the schema for tuple interpretation
    uint32_t current_offset_; ///< Current position in the page data
    uint32_t start_offset_;   ///< Starting position for reset functionality
};

}  // namespace minidb
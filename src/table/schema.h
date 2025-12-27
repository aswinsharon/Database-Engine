#pragma once

#include "common/config.h"
#include "common/types.h"
#include <vector>
#include <string>

namespace minidb {

/**
 * Column definition in a table schema
 * Represents a single column with name, data type, and size information
 */
class Column {
public:
    /**
     * Constructor for Column
     * @param name The name of the column
     * @param type The data type of the column (INTEGER, VARCHAR, BOOLEAN)
     * @param size The size in bytes (for VARCHAR: max length, for others: actual size)
     *             If 0, default sizes are used for fixed-length types
     */
    Column(std::string name, DataType type, uint32_t size = 0)
        : name_(std::move(name)), type_(type), size_(size) {
        
        // Set default sizes for fixed-length types
        if (type_ == DataType::INTEGER && size_ == 0) {
            size_ = sizeof(int32_t);
        } else if (type_ == DataType::BOOLEAN && size_ == 0) {
            size_ = sizeof(bool);
        }
    }

    /**
     * Get the name of the column
     * @return Reference to the column name string
     */
    const std::string& GetName() const { return name_; }
    
    /**
     * Get the data type of the column
     * @return The DataType enum value
     */
    DataType GetType() const { return type_; }
    
    /**
     * Get the size of the column in bytes
     * @return Size in bytes (max length for VARCHAR, actual size for fixed types)
     */
    uint32_t GetSize() const { return size_; }
    
    /**
     * Check if the column has a fixed length
     * @return true for INTEGER and BOOLEAN types, false for VARCHAR
     */
    bool IsFixedLength() const {
        return type_ == DataType::INTEGER || type_ == DataType::BOOLEAN;
    }

private:
    std::string name_;   ///< Name of the column
    DataType type_;      ///< Data type of the column
    uint32_t size_;      ///< Size in bytes (max length for VARCHAR, actual size for others)
};

/**
 * Schema defines the structure of a table
 * Contains column definitions and provides methods to access column information
 */
class Schema {
public:
    /**
     * Constructor for Schema
     * @param columns Vector of Column objects that define the table structure
     */
    explicit Schema(const std::vector<Column>& columns);
    
    /**
     * Get all columns in the schema
     * @return Reference to the vector of Column objects
     */
    const std::vector<Column>& GetColumns() const { return columns_; }
    
    /**
     * Get a column by its index position
     * @param col_idx Zero-based index of the column
     * @return Reference to the Column object
     * @throws std::out_of_range if index is invalid
     */
    const Column& GetColumn(uint32_t col_idx) const;
    
    /**
     * Get a column by its name
     * @param col_name Name of the column to retrieve
     * @return Reference to the Column object
     * @throws std::runtime_error if column name is not found
     */
    const Column& GetColumn(const std::string& col_name) const;
    
    /**
     * Get the total number of columns in the schema
     * @return Number of columns
     */
    uint32_t GetColumnCount() const { return static_cast<uint32_t>(columns_.size()); }
    
    /**
     * Get the index of a column by its name
     * @param col_name Name of the column
     * @return Zero-based index of the column
     * @throws std::runtime_error if column name is not found
     */
    uint32_t GetColumnIndex(const std::string& col_name) const;
    
    /**
     * Get the total size of all fixed-length columns
     * Used for tuple header calculation and storage optimization
     * @return Total size in bytes of fixed-length columns
     */
    uint32_t GetFixedLength() const { return fixed_length_; }
    
    /**
     * Check if all columns in the schema are fixed-length
     * @return true if all columns are fixed-length, false if any are variable-length
     */
    bool IsFixedLength() const;
    
    /**
     * Generate a string representation of the schema
     * @return Human-readable string describing the schema structure
     */
    std::string ToString() const;

private:
    std::vector<Column> columns_;  ///< Vector of column definitions
    uint32_t fixed_length_;        ///< Total size of fixed-length columns in bytes
    
    /**
     * Calculate the total fixed length of all fixed-length columns
     * Called internally during construction and when schema changes
     */
    void CalculateFixedLength();
};

}  // namespace minidb
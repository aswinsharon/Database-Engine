#include "table/schema.h"
#include <sstream>
#include <stdexcept>

namespace minidb {

/**
 * Constructor for Schema
 * @param columns Vector of Column objects that define the schema structure
 */
Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    CalculateFixedLength();
}

/**
 * Get a column by its index position
 * @param col_idx The zero-based index of the column
 * @return Reference to the Column object at the specified index
 * @throws std::out_of_range if col_idx is >= number of columns
 */
const Column& Schema::GetColumn(uint32_t col_idx) const {
    if (col_idx >= columns_.size()) {
        throw std::out_of_range("Column index out of range");
    }
    return columns_[col_idx];
}

/**
 * Get a column by its name
 * @param col_name The name of the column to retrieve
 * @return Reference to the Column object with the specified name
 * @throws std::runtime_error if column with given name is not found
 */
const Column& Schema::GetColumn(const std::string& col_name) const {
    for (const auto& col : columns_) {
        if (col.GetName() == col_name) {
            return col;
        }
    }
    throw std::runtime_error("Column not found: " + col_name);
}

/**
 * Get the index position of a column by its name
 * @param col_name The name of the column
 * @return Zero-based index of the column
 * @throws std::runtime_error if column with given name is not found
 */
uint32_t Schema::GetColumnIndex(const std::string& col_name) const {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].GetName() == col_name) {
            return i;
        }
    }
    throw std::runtime_error("Column not found: " + col_name);
}

/**
 * Check if all columns in the schema have fixed length
 * @return true if all columns are fixed-length, false if any column is variable-length
 */
bool Schema::IsFixedLength() const {
    for (const auto& col : columns_) {
        if (!col.IsFixedLength()) {
            return false;
        }
    }
    return true;
}

/**
 * Calculate the total fixed length of the schema
 * Sums up the sizes of all fixed-length columns
 * Variable-length columns are not included in this calculation
 */
void Schema::CalculateFixedLength() {
    fixed_length_ = 0;
    for (const auto& col : columns_) {
        if (col.IsFixedLength()) {
            fixed_length_ += col.GetSize();
        }
    }
}

/**
 * Generate a string representation of the schema
 * @return String in format "Schema(col1:TYPE, col2:TYPE(size), ...)"
 */
std::string Schema::ToString() const {
    std::ostringstream oss;
    oss << "Schema(";
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << columns_[i].GetName() << ":";
        
        switch (columns_[i].GetType()) {
            case DataType::INTEGER:
                oss << "INTEGER";
                break;
            case DataType::VARCHAR:
                oss << "VARCHAR(" << columns_[i].GetSize() << ")";
                break;
            case DataType::BOOLEAN:
                oss << "BOOLEAN";
                break;
        }
    }
    oss << ")";
    return oss.str();
}

}  // namespace minidb
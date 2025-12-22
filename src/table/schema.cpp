#include "table/schema.h"
#include <sstream>
#include <stdexcept>

namespace minidb {

Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    CalculateFixedLength();
}

const Column& Schema::GetColumn(uint32_t col_idx) const {
    if (col_idx >= columns_.size()) {
        throw std::out_of_range("Column index out of range");
    }
    return columns_[col_idx];
}

const Column& Schema::GetColumn(const std::string& col_name) const {
    for (const auto& col : columns_) {
        if (col.GetName() == col_name) {
            return col;
        }
    }
    throw std::runtime_error("Column not found: " + col_name);
}

uint32_t Schema::GetColumnIndex(const std::string& col_name) const {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].GetName() == col_name) {
            return i;
        }
    }
    throw std::runtime_error("Column not found: " + col_name);
}

bool Schema::IsFixedLength() const {
    for (const auto& col : columns_) {
        if (!col.IsFixedLength()) {
            return false;
        }
    }
    return true;
}

void Schema::CalculateFixedLength() {
    fixed_length_ = 0;
    for (const auto& col : columns_) {
        if (col.IsFixedLength()) {
            fixed_length_ += col.GetSize();
        }
    }
}

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
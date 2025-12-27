#include "common/types.h"
#include <sstream>
#include <cstring>

namespace minidb {

/**
 * Equality comparison operator for Value objects
 * @param other The Value to compare with
 * @return true if both values have the same type and equal data
 */
bool Value::operator==(const Value& other) const {
    if (type_ != other.type_) {
        return false;
    }
    
    switch (type_) {
        case INTEGER:
            return int_val_ == other.int_val_;
        case VARCHAR:
            return str_val_ == other.str_val_;
        case BOOLEAN:
            return bool_val_ == other.bool_val_;
        case NULL_TYPE:
            return true;
    }
    return false;
}

/**
 * Less-than comparison operator for Value objects
 * Used for sorting and indexing operations
 * @param other The Value to compare with
 * @return true if this value is less than the other value
 */
bool Value::operator<(const Value& other) const {
    if (type_ != other.type_) {
        return type_ < other.type_;
    }
    
    switch (type_) {
        case INTEGER:
            return int_val_ < other.int_val_;
        case VARCHAR:
            return str_val_ < other.str_val_;
        case BOOLEAN:
            return bool_val_ < other.bool_val_;
        case NULL_TYPE:
            return false;
    }
    return false;
}

/**
 * Greater-than comparison operator for Value objects
 * @param other The Value to compare with
 * @return true if this value is greater than the other value
 */
bool Value::operator>(const Value& other) const {
    return other < *this;
}

/**
 * Serialize the Value to a byte buffer
 * Writes the type followed by the actual data
 * @param data Buffer to write the serialized data
 * @return Number of bytes written
 */
uint32_t Value::SerializeTo(char* data) const {
    uint32_t offset = 0;
    
    // Write type identifier first
    *reinterpret_cast<Type*>(data + offset) = type_;
    offset += sizeof(Type);
    
    switch (type_) {
        case INTEGER:
            *reinterpret_cast<int32_t*>(data + offset) = int_val_;
            offset += sizeof(int32_t);
            break;
        case VARCHAR: {
            // Write string length followed by string data
            uint32_t str_len = static_cast<uint32_t>(str_val_.length());
            *reinterpret_cast<uint32_t*>(data + offset) = str_len;
            offset += sizeof(uint32_t);
            memcpy(data + offset, str_val_.c_str(), str_len);
            offset += str_len;
            break;
        }
        case BOOLEAN:
            *reinterpret_cast<bool*>(data + offset) = bool_val_;
            offset += sizeof(bool);
            break;
        case NULL_TYPE:
            // No additional data for NULL values
            break;
    }
    
    return offset;
}

/**
 * Deserialize a Value from a byte buffer
 * Reads the type and then the corresponding data
 * @param data Buffer containing the serialized Value data
 * @return Number of bytes read
 */
uint32_t Value::DeserializeFrom(const char* data) {
    uint32_t offset = 0;
    
    // Read type identifier first
    type_ = *reinterpret_cast<const Type*>(data + offset);
    offset += sizeof(Type);
    
    switch (type_) {
        case INTEGER:
            int_val_ = *reinterpret_cast<const int32_t*>(data + offset);
            offset += sizeof(int32_t);
            break;
        case VARCHAR: {
            // Read string length followed by string data
            uint32_t str_len = *reinterpret_cast<const uint32_t*>(data + offset);
            offset += sizeof(uint32_t);
            str_val_ = std::string(data + offset, str_len);
            offset += str_len;
            break;
        }
        case BOOLEAN:
            bool_val_ = *reinterpret_cast<const bool*>(data + offset);
            offset += sizeof(bool);
            break;
        case NULL_TYPE:
            // No additional data for NULL values
            break;
    }
    
    return offset;
}

/**
 * Calculate the size needed to serialize this Value
 * @return Size in bytes required for serialization
 */
uint32_t Value::GetSerializedSize() const {
    uint32_t size = sizeof(Type);  // Always need space for type identifier
    
    switch (type_) {
        case INTEGER:
            size += sizeof(int32_t);
            break;
        case VARCHAR:
            size += sizeof(uint32_t) + static_cast<uint32_t>(str_val_.length());
            break;
        case BOOLEAN:
            size += sizeof(bool);
            break;
        case NULL_TYPE:
            // No additional data for NULL values
            break;
    }
    
    return size;
}

/**
 * Generate a string representation of the Value
 * @return Human-readable string representation of the value
 */
std::string Value::ToString() const {
    switch (type_) {
        case INTEGER:
            return std::to_string(int_val_);
        case VARCHAR:
            return str_val_;
        case BOOLEAN:
            return bool_val_ ? "true" : "false";
        case NULL_TYPE:
            return "NULL";
    }
    return "UNKNOWN";
}

}  // namespace minidb
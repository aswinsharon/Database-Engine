#pragma once

#include "table/tuple.h"
#include "table/schema.h"
#include "common/types.h"

namespace minidb {

/**
 * Base class for all expressions
 */
class Expression {
public:
    virtual ~Expression() = default;
    
    // Evaluate the expression against a tuple
    virtual Value Evaluate(const Tuple& tuple, const Schema& schema) const = 0;
    
    // Get the return type of this expression
    virtual DataType GetReturnType() const = 0;
};

/**
 * Column reference expression - references a column by name
 */
class ColumnExpression : public Expression {
public:
    explicit ColumnExpression(const std::string& column_name)
        : column_name_(column_name) {}
    
    Value Evaluate(const Tuple& tuple, const Schema& schema) const override {
        int column_idx = schema.GetColumnIndex(column_name_);
        if (column_idx == -1) {
            return Value(DataType::NULL_TYPE);
        }
        return tuple.GetValue(column_idx);
    }
    
    DataType GetReturnType() const override {
        // Return type depends on the column, determined at runtime
        return DataType::NULL_TYPE;
    }
    
private:
    std::string column_name_;
};

/**
 * Constant value expression
 */
class ConstantExpression : public Expression {
public:
    explicit ConstantExpression(const Value& value) : value_(value) {}
    
    Value Evaluate(const Tuple& tuple, const Schema& schema) const override {
        return value_;
    }
    
    DataType GetReturnType() const override {
        return value_.GetType();
    }
    
private:
    Value value_;
};

/**
 * Comparison expression (=, <, >, <=, >=, !=)
 */
class ComparisonExpression : public Expression {
public:
    enum ComparisonType {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL
    };
    
    ComparisonExpression(ComparisonType type,
                        std::unique_ptr<Expression> left,
                        std::unique_ptr<Expression> right)
        : type_(type), left_(std::move(left)), right_(std::move(right)) {}
    
    Value Evaluate(const Tuple& tuple, const Schema& schema) const override {
        Value left_val = left_->Evaluate(tuple, schema);
        Value right_val = right_->Evaluate(tuple, schema);
        
        bool result = false;
        switch (type_) {
            case EQUAL:
                result = (left_val == right_val);
                break;
            case NOT_EQUAL:
                result = !(left_val == right_val);
                break;
            case LESS_THAN:
                result = (left_val < right_val);
                break;
            case LESS_THAN_OR_EQUAL:
                result = (left_val < right_val) || (left_val == right_val);
                break;
            case GREATER_THAN:
                result = (right_val < left_val);
                break;
            case GREATER_THAN_OR_EQUAL:
                result = (right_val < left_val) || (left_val == right_val);
                break;
        }
        
        return Value(DataType::BOOLEAN, result);
    }
    
    DataType GetReturnType() const override {
        return DataType::BOOLEAN;
    }
    
private:
    ComparisonType type_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

} // namespace minidb
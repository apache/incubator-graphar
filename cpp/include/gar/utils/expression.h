/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <string>

#include "arrow/compute/api.h"

#ifndef GAR_UTILS_EXPRESSION_H_
#define GAR_UTILS_EXPRESSION_H_

namespace GAR_NAMESPACE_INTERNAL {

/**
 * CompareOperator is an enum class that represents the relational operators
 * that can be used to compare two values.
 */
enum class CompareOperator : std::uint8_t {
  equal,          //"="
  not_equal,      //"<>"
  less,           //"<"
  less_equal,     //"<="
  greater,        //">"
  greater_equal,  //">="
};

/**
 * This class wraps an arrow::compute::Expression and provides methods for
 * reading arrow::compute::Expression objects
 */
class Expression {
  friend class FilterBuilder;

 public:
  Expression() = default;
  Expression(const Expression& other) : expr_(other.expr_) {}
  ~Expression() = default;

  bool Equals(const Expression& other) { return expr_.Equals(other.expr_); }
  arrow::compute::Expression GetExpr() const { return expr_; }

 private:
  explicit Expression(arrow::compute::Expression expr) : expr_(expr) {}

  arrow::compute::Expression expr_;
};

/**
 * This class builds an expression tree for a filter.
 */
class FilterBuilder {
 public:
  FilterBuilder() = default;
  ~FilterBuilder() = default;

  static auto OperatorTypeToArrowOpFunc(CompareOperator op) {
    switch (op) {
    case CompareOperator::equal:
      return arrow::compute::equal;
    case CompareOperator::not_equal:
      return arrow::compute::not_equal;
    case CompareOperator::less:
      return arrow::compute::less;
    case CompareOperator::less_equal:
      return arrow::compute::less_equal;
    case CompareOperator::greater:
      return arrow::compute::greater;
    case CompareOperator::greater_equal:
      return arrow::compute::greater_equal;
    }
  }

  template <typename T>
  static Expression Make(const std::string& property, CompareOperator op,
                         const T value) {
    auto func = OperatorTypeToArrowOpFunc(op);
    return Expression(func(arrow::compute::field_ref(property),
                           arrow::compute::literal(value)));
  }

  static Expression And(const Expression& left, const Expression& right) {
    return Expression(arrow::compute::and_(left.GetExpr(), right.GetExpr()));
  }

  static Expression Or(const Expression& left, const Expression& right) {
    return Expression(arrow::compute::or_(left.GetExpr(), right.GetExpr()));
  }

  static Expression Not(const Expression& expr) {
    return Expression(arrow::compute::not_(expr.GetExpr()));
  }

 private:
  Expression expr_;
};
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_EXPRESSION_H_

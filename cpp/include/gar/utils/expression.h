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

#include "gar/graph_info.h"

#ifndef GAR_UTILS_EXPRESSION_H_
#define GAR_UTILS_EXPRESSION_H_

namespace GAR_NAMESPACE_INTERNAL {

/**
 * CompareOperator is an enum class that represents the relational operators
 * that can be used to compare two values.
 */
enum class CompareOperator : std::uint8_t {
  EQUAL,          //"="
  NOT_EQUAL,      //"<>"
  LESS,           //"<"
  LESS_EQUAL,     //"<="
  GREATER,        //">"
  GREATER_EQUAL,  //">="
};

/**
 * This class wraps an arrow::compute::Expression and provides methods for
 * reading arrow::compute::Expression objects
 */
class Expression {
  friend class FileSystem;
  friend Expression And(const Expression& lhs, const Expression& rhs);
  friend Expression Or(const Expression& lhs, const Expression& rhs);
  friend Expression Not(const Expression& expr);

 public:
  Expression() = default;
  Expression(const Expression& other) = default;
  ~Expression() = default;
  static auto OperatorTypeToArrowOpFunc(CompareOperator op) {
    switch (op) {
    case CompareOperator::EQUAL:
      return arrow::compute::equal;
    case CompareOperator::NOT_EQUAL:
      return arrow::compute::not_equal;
    case CompareOperator::LESS:
      return arrow::compute::less;
    case CompareOperator::LESS_EQUAL:
      return arrow::compute::less_equal;
    case CompareOperator::GREATER:
      return arrow::compute::greater;
    case CompareOperator::GREATER_EQUAL:
      return arrow::compute::greater_equal;
    }
  }

  template <typename T>
  static Expression Make(const Property& property, CompareOperator op,
                         const T value) {
    auto func = OperatorTypeToArrowOpFunc(op);
    return Expression(func(arrow::compute::field_ref(property.name),
                           arrow::compute::literal(value)));
  }

  template <typename T>
  static Expression Make(const T value, CompareOperator op,
                         const Property& property) {
    return Make(property, op, value);
  }

  static Expression Make(const Property& lhs, CompareOperator op,
                         const Property& rhs) {
    auto func = OperatorTypeToArrowOpFunc(op);
    return Expression(func(arrow::compute::field_ref(lhs.name),
                           arrow::compute::field_ref(rhs.name)));
  }

 private:
  explicit Expression(arrow::compute::Expression expr)
      : arrow_expr_(std::move(expr)) {}

  arrow::compute::Expression arrow_expr_;
};

/**
 * This class builds an expression tree for a filter.
 */

inline Expression And(const Expression& lhs, const Expression& rhs) {
  return Expression(arrow::compute::and_(lhs.arrow_expr_, rhs.arrow_expr_));
}

inline Expression Or(const Expression& lhs, const Expression& rhs) {
  return Expression(arrow::compute::or_(lhs.arrow_expr_, rhs.arrow_expr_));
}

inline Expression Not(const Expression& expr) {
  return Expression(arrow::compute::not_(expr.arrow_expr_));
}
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_EXPRESSION_H_

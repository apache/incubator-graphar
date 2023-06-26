/** Copyright 2023 Alibaba Group Holding Limited.

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

#include <memory>
#include <string>

#include "arrow/compute/api.h"

#include "gar/graph_info.h"

#ifndef GAR_UTILS_EXPRESSION_H_
#define GAR_UTILS_EXPRESSION_H_

namespace GAR_NAMESPACE_INTERNAL {

using ArrowExpression = arrow::compute::Expression;
class BinaryOperator;

/**
 * This class wraps an arrow::compute::Expression and provides methods for
 * reading arrow::compute::Expression objects
 */
class Expression {
 public:
  Expression() = default;
  Expression(const Expression& other) = default;
  virtual ~Expression() = default;

  /**
   * @brief Make a new expression from a property and a value
   *
   * @tparam OpType The type of the operator, only binary operators are allowed
   * e.g. OperatorEqual
   * @tparam ValType The type of the value, e.g. int64_t
   * @param property The property to compare
   * @param value The value to compare
   * @return A predicate expression for filter pushdown
   */
  template <
      typename OpType,
      typename = std::enable_if_t<std::is_base_of_v<BinaryOperator, OpType>>,
      typename ValType>
  static inline Expression* Make(const Property& property,
                                 const ValType& value);

  /**
   * @brief Make a new expression from a property and a value
   *
   * @tparam OpType The type of the operator, only binary operators are allowed
   * e.g. OperatorEqual
   * @tparam ValType The type of the value, e.g. int64_t
   * @param value The value to compare
   * @param property The property to compare
   * @return A predicate expression for filter pushdown
   */
  template <
      typename OpType,
      typename = std::enable_if_t<std::is_base_of_v<BinaryOperator, OpType>>,
      typename ValType>
  static inline Expression* Make(const ValType& value,
                                 const Property& property);
  /**
   * @brief Make a new expression from a property and a value
   *
   * @tparam OpType The type of the operator, only binary operators are allowed
   * e.g. OperatorEq
   * @param p1 The first property to compare
   * @param p2 The second property to compare
   * @return A predicate expression for filter pushdown
   */
  template <typename OpType, typename = std::enable_if_t<
                                 std::is_base_of_v<BinaryOperator, OpType>>>
  static inline Expression* Make(const Property& p1, const Property& p2);

  /**
   * @brief Parse predicates based on attributes, operators, and values e,g. new
   * OperatorEqual(new ExpressionProperty(Property("a")), new
   * ExpressionLiteral<int64_t>(1)) will be parsed as
   * arrow::compute::equal(arrow::compute::field_ref("a"),
   * arrow::compute::literal(1))
   *
   * @return The arrow::compute::Expression object
   */
  virtual ArrowExpression Evaluate() = 0;
};

class ExpressionProperty : public Expression {
 public:
  explicit ExpressionProperty(const Property& property) : property_(property) {}
  ExpressionProperty(const ExpressionProperty& other) = default;
  ~ExpressionProperty() = default;

  ArrowExpression Evaluate() override;

 private:
  Property property_;
};

template <typename T>
class ExpressionLiteral : public Expression {
 public:
  explicit ExpressionLiteral(const T value) : value_(value) {}
  ExpressionLiteral(const ExpressionLiteral& other) = default;
  ~ExpressionLiteral() = default;

  ArrowExpression Evaluate() { return arrow::compute::literal(value_); }

 private:
  T value_;
};

class UnaryOperator : public Expression {
 public:
  UnaryOperator() = default;
  explicit UnaryOperator(Expression* expr) : expr_(expr) {}
  UnaryOperator(const UnaryOperator& other) = default;
  virtual ~UnaryOperator() {}

 protected:
  std::shared_ptr<Expression> expr_;
};

class OperatorNot : public UnaryOperator {
 public:
  OperatorNot() = default;
  explicit OperatorNot(Expression* expr) : UnaryOperator(expr) {}
  OperatorNot(const OperatorNot& other) = default;
  ~OperatorNot() = default;

  ArrowExpression Evaluate() override;
};

class OperatorIsNull : public UnaryOperator {
 public:
  OperatorIsNull() = default;
  explicit OperatorIsNull(Expression* expr, bool nan_is_null = false)
      : UnaryOperator(expr), nan_is_null_(nan_is_null) {}
  OperatorIsNull(const OperatorIsNull& other) = default;
  ~OperatorIsNull() = default;

  ArrowExpression Evaluate() override;

 private:
  bool nan_is_null_;
};

class BinaryOperator : public Expression {
 public:
  BinaryOperator() = default;
  BinaryOperator(Expression* lhs, Expression* rhs) : lhs_(lhs), rhs_(rhs) {}
  BinaryOperator(const BinaryOperator& other) = default;
  ~BinaryOperator() = default;

 protected:
  std::shared_ptr<Expression> lhs_;
  std::shared_ptr<Expression> rhs_;
};

class OperatorEqual : public BinaryOperator {
 public:
  OperatorEqual() = default;
  OperatorEqual(Expression* lhs, Expression* rhs) : BinaryOperator(lhs, rhs) {}
  OperatorEqual(const OperatorEqual& other) = default;
  ~OperatorEqual() = default;

  ArrowExpression Evaluate() override;
};

class OperatorNotEqual : public BinaryOperator {
 public:
  OperatorNotEqual() = default;
  OperatorNotEqual(Expression* lhs, Expression* rhs)
      : BinaryOperator(lhs, rhs) {}
  OperatorNotEqual(const OperatorNotEqual& other) = default;
  ~OperatorNotEqual() = default;

  ArrowExpression Evaluate() override;
};

class OperatorGreater : public BinaryOperator {
 public:
  OperatorGreater() = default;
  OperatorGreater(Expression* lhs, Expression* rhs)
      : BinaryOperator(lhs, rhs) {}
  OperatorGreater(const OperatorGreater& other) = default;
  ~OperatorGreater() = default;

  ArrowExpression Evaluate() override;
};

class OperatorGreaterEqual : public BinaryOperator {
 public:
  OperatorGreaterEqual() = default;
  OperatorGreaterEqual(Expression* lhs, Expression* rhs)
      : BinaryOperator(lhs, rhs) {}
  OperatorGreaterEqual(const OperatorGreaterEqual& other) = default;
  ~OperatorGreaterEqual() = default;

  ArrowExpression Evaluate() override;
};

class OperatorLess : public BinaryOperator {
 public:
  OperatorLess() = default;
  OperatorLess(Expression* lhs, Expression* rhs) : BinaryOperator(lhs, rhs) {}
  OperatorLess(const OperatorLess& other) = default;
  ~OperatorLess() = default;

  ArrowExpression Evaluate() override;
};

class OperatorLessEqual : public BinaryOperator {
 public:
  OperatorLessEqual() = default;
  OperatorLessEqual(Expression* lhs, Expression* rhs)
      : BinaryOperator(lhs, rhs) {}
  OperatorLessEqual(const OperatorLessEqual& other) = default;
  ~OperatorLessEqual() = default;

  ArrowExpression Evaluate() override;
};

class OperatorAnd : public BinaryOperator {
 public:
  OperatorAnd() = default;
  OperatorAnd(Expression* lhs, Expression* rhs) : BinaryOperator(lhs, rhs) {}
  OperatorAnd(const OperatorAnd& other) = default;
  ~OperatorAnd() = default;

  ArrowExpression Evaluate() override;
};

class OperatorOr : public BinaryOperator {
 public:
  OperatorOr() = default;
  OperatorOr(Expression* lhs, Expression* rhs) : BinaryOperator(lhs, rhs) {}
  OperatorOr(const OperatorOr& other) = default;
  ~OperatorOr() = default;

  ArrowExpression Evaluate() override;
};

using Equal = OperatorEqual;
using NotEqual = OperatorNotEqual;
using Greater = OperatorGreater;
using GreaterThan = OperatorGreaterEqual;
using Less = OperatorLess;
using LessEqual = OperatorLessEqual;

/**
 * Helper functions to Construct Expression.
 */
template <typename OpType, typename, typename ValType>
inline Expression* Expression::Make(const Property& property,
                                    const ValType& value) {
  return new OpType(new ExpressionProperty(property),
                    new ExpressionLiteral<ValType>(value));
}

template <typename OpType, typename, typename ValType>
inline Expression* Expression::Make(const ValType& value,
                                    const Property& property) {
  return new OpType(new ExpressionLiteral<ValType>(value),
                    new ExpressionProperty(property));
}

template <typename OpType, typename>
inline Expression* Expression::Make(const Property& p1, const Property& p2) {
  return new OpType(new ExpressionProperty(p1), new ExpressionProperty(p2));
}

static inline Expression* Not(Expression* expr) {
  return new OperatorNot(expr);
}

static inline Expression* IsNull(Expression* expr, bool nan_is_null = false) {
  return new OperatorIsNull(expr, nan_is_null);
}

static inline Expression* And(Expression* lhs, Expression* rhs) {
  return new OperatorAnd(lhs, rhs);
}

static inline Expression* Or(Expression* lhs, Expression* rhs) {
  return new OperatorOr(lhs, rhs);
}

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_EXPRESSION_H_

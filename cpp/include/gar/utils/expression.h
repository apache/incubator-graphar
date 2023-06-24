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

/**
 * CompareOperator is an enum class that represents the relational operators
 * that can be used to compare two values.
 */

using ArrowExpression = arrow::compute::Expression;
enum class Operator : std::uint8_t {
  Equal,         // "="
  NotEqual,      // "<>"
  Less,          // "<"
  LessEqual,     // "<="
  Greater,       // ">"
  GreaterEqual,  // ">="
  And,           // "and"
  Or,            // "or"
  Not,           // "not"
  IsNull         // "is null"
};

/**
 * This class wraps an arrow::compute::Expression and provides methods for
 * reading arrow::compute::Expression objects
 */
class Expression {
 public:
  Expression() = default;
  Expression(const Expression& other) = default;
  virtual ~Expression() = default;

  template <typename T>
  static inline Result<Expression*> Make(const Property& property, Operator op,
                                         const T& value);

  template <typename T>
  static inline Result<Expression*> Make(const T& value, Operator op,
                                         const Property& property);

  static inline Result<Expression*> Make(const Property& p1, Operator op,
                                         const Property& p2);

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

  ArrowExpression Evaluate() override {
    return arrow::compute::not_(expr_->Evaluate());
  }
};

class OperatorIsNull : public UnaryOperator {
 public:
  OperatorIsNull() = default;
  explicit OperatorIsNull(Expression* expr, bool nan_is_null = false)
      : UnaryOperator(expr), nan_is_null_(nan_is_null) {}
  OperatorIsNull(const OperatorIsNull& other) = default;
  ~OperatorIsNull() = default;

  ArrowExpression Evaluate() override {
    return arrow::compute::is_null(expr_->Evaluate(), nan_is_null_);
  }

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

/**
 * Helper functions to Construct Expression.
 */
template <typename T>
inline Result<Expression*> Expression::Make(const Property& property,
                                            Operator op, const T& value) {
  switch (op) {
#define TO_OPERATOR_CASE_PV(_op)                                 \
  case Operator::_op: {                                          \
    return new Operator##_op(new ExpressionProperty((property)), \
                             new ExpressionLiteral<T>((value))); \
  }
    TO_OPERATOR_CASE_PV(Equal)
    TO_OPERATOR_CASE_PV(NotEqual)
    TO_OPERATOR_CASE_PV(Less)
    TO_OPERATOR_CASE_PV(LessEqual)
    TO_OPERATOR_CASE_PV(Greater)
    TO_OPERATOR_CASE_PV(GreaterEqual)
    TO_OPERATOR_CASE_PV(And)
    TO_OPERATOR_CASE_PV(Or)
  default:
    break;
  }
  return Status::Invalid("Unrecognized binary operator");
}

template <typename T>
inline Result<Expression*> Expression::Make(const T& value, Operator op,
                                            const Property& property) {
  return Expression::Make(property, op, value);
}

inline Result<Expression*> Expression::Make(const Property& p1, Operator op,
                                            const Property& p2) {
  switch (op) {
#define TO_OPERATOR_CASE_PP(_op)                            \
  case Operator::_op: {                                     \
    return new Operator##_op(new ExpressionProperty((p1)),  \
                             new ExpressionProperty((p2))); \
  }
    TO_OPERATOR_CASE_PP(Equal);
    TO_OPERATOR_CASE_PP(NotEqual);
    TO_OPERATOR_CASE_PP(Less);
    TO_OPERATOR_CASE_PP(LessEqual);
    TO_OPERATOR_CASE_PP(Greater);
    TO_OPERATOR_CASE_PP(GreaterEqual);
    TO_OPERATOR_CASE_PP(And);
    TO_OPERATOR_CASE_PP(Or);
  default:
    break;
  }
  return Status::Invalid("Unrecognized binary operator");
}

static inline Result<Expression*> Not(Expression* expr) {
  return new OperatorNot(expr);
}

static inline Result<Expression*> IsNull(Expression* expr,
                                         bool nan_is_null = false) {
  return new OperatorIsNull(expr, nan_is_null);
}

static inline Result<Expression*> And(Expression* lhs, Expression* rhs) {
  return new OperatorAnd(lhs, rhs);
}

static inline Result<Expression*> Or(Expression* lhs, Expression* rhs) {
  return new OperatorOr(lhs, rhs);
}

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_EXPRESSION_H_

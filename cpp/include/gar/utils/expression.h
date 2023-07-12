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

/**
 * This class wraps an arrow::compute::Expression and provides a way to
 * construct it
 */
class Expression {
 public:
  Expression() = default;
  Expression(const Expression& other) = default;
  virtual ~Expression() = default;

  /**
   * @brief Evaluate Expression as arrow::compute::Expression e.g. new
   * ExpressionEqual(new ExpressionProperty("a"), new
   * ExpressionLiteral(1)) will be parsed as
   * arrow::compute::equal(arrow::compute::field_ref("a"),
   * arrow::compute::literal(1))
   *
   * @return The arrow::compute::Expression instance
   */
  virtual ArrowExpression Evaluate() = 0;
};

/**
 * This class wraps the Property and provides a way to construct property
 * expression
 */
class ExpressionProperty : public Expression {
 public:
  explicit ExpressionProperty(const Property& property) : property_(property) {}
  explicit ExpressionProperty(const std::string& name)
      : property_(Property(name)) {}
  ExpressionProperty(const ExpressionProperty& other) = default;
  ~ExpressionProperty() = default;

  ArrowExpression Evaluate() override;

 private:
  Property property_;
};

/**
 * This class wraps the literal. Only bool, int32, int64, float, double and
 * string are allowed.
 */
template <typename T>
class ExpressionLiteral : public Expression {
 public:
  explicit ExpressionLiteral(T value) : value_(value) {}
  ExpressionLiteral(const ExpressionLiteral& other) = default;
  ~ExpressionLiteral() = default;

  ArrowExpression Evaluate() { return arrow::compute::literal(value_); }

 private:
  T value_;
};

/**
 * This class constructs a unary operator expression that accepts only one
 * expression
 */
class ExpressionUnaryOp : public Expression {
 public:
  ExpressionUnaryOp() = default;
  explicit ExpressionUnaryOp(std::shared_ptr<Expression> expr) : expr_(expr) {}
  ExpressionUnaryOp(const ExpressionUnaryOp& other) = default;
  virtual ~ExpressionUnaryOp() {}

 protected:
  std::shared_ptr<Expression> expr_;
};

/**
 * This class constructs a NOT operator expression. e.g. new ExpressionNot(new
 * ExpressionLiteral(true)) => NOT TRUE
 */
class ExpressionNot : public ExpressionUnaryOp {
 public:
  ExpressionNot() = default;
  explicit ExpressionNot(std::shared_ptr<Expression> expr)
      : ExpressionUnaryOp(expr) {}
  ExpressionNot(const ExpressionNot& other) = default;
  ~ExpressionNot() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a IS NULL operator expression. e.g. new
 * ExpressionIsNull(new ExpressionProperty("a")) => a IS NULL
 */
class ExpressionIsNull : public ExpressionUnaryOp {
 public:
  ExpressionIsNull() = default;
  explicit ExpressionIsNull(std::shared_ptr<Expression> expr,
                            bool nan_is_null = false)
      : ExpressionUnaryOp(expr), nan_is_null_(nan_is_null) {}
  ExpressionIsNull(const ExpressionIsNull& other) = default;
  ~ExpressionIsNull() = default;

  ArrowExpression Evaluate() override;

 private:
  bool nan_is_null_;
};

/**
 * This class constructs a binary operator expression that accepts two
 * expressions e.g. a = 1, a > 1, a AND b, a OR b
 */
class ExpressionBinaryOp : public Expression {
 public:
  ExpressionBinaryOp() = default;
  ExpressionBinaryOp(std::shared_ptr<Expression> lhs,
                     std::shared_ptr<Expression> rhs)
      : lhs_(lhs), rhs_(rhs) {}
  ExpressionBinaryOp(const ExpressionBinaryOp& other) = default;
  ~ExpressionBinaryOp() = default;

 protected:
  std::shared_ptr<Expression> lhs_;
  std::shared_ptr<Expression> rhs_;
};

/**
 * This class constructs a EQUAL operator expression.
 * e.g. new ExpressionEqual(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a = 1
 */
class ExpressionEqual : public ExpressionBinaryOp {
 public:
  ExpressionEqual() = default;
  ExpressionEqual(std::shared_ptr<Expression> lhs,
                  std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionEqual(const ExpressionEqual& other) = default;
  ~ExpressionEqual() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a NOT EQUAL operator expression.
 * e.g. new ExpressionNotEqual(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a != 1
 */
class ExpressionNotEqual : public ExpressionBinaryOp {
 public:
  ExpressionNotEqual() = default;
  ExpressionNotEqual(std::shared_ptr<Expression> lhs,
                     std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionNotEqual(const ExpressionNotEqual& other) = default;
  ~ExpressionNotEqual() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a GREATER THAN operator expression.
 * e.g. new ExpressionGreaterThan(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a > 1
 */
class ExpressionGreaterThan : public ExpressionBinaryOp {
 public:
  ExpressionGreaterThan() = default;
  ExpressionGreaterThan(std::shared_ptr<Expression> lhs,
                        std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionGreaterThan(const ExpressionGreaterThan& other) = default;
  ~ExpressionGreaterThan() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a GREATER EQUAL operator expression.
 * e.g. new ExpressionGreaterEqual(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a >= 1
 */
class ExpressionGreaterEqual : public ExpressionBinaryOp {
 public:
  ExpressionGreaterEqual() = default;
  ExpressionGreaterEqual(std::shared_ptr<Expression> lhs,
                         std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionGreaterEqual(const ExpressionGreaterEqual& other) = default;
  ~ExpressionGreaterEqual() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a LESS THAN operator expression.
 * e.g. new ExpressionLessThan(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a < 1
 */
class ExpressionLessThan : public ExpressionBinaryOp {
 public:
  ExpressionLessThan() = default;
  ExpressionLessThan(std::shared_ptr<Expression> lhs,
                     std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionLessThan(const ExpressionLessThan& other) = default;
  ~ExpressionLessThan() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a LESS EQUAL operator expression.
 * e.g. new ExpressionLessEqual(new ExpressionProperty("a"), new
 * ExpressionLiteral(1)) => a <= 1
 */
class ExpressionLessEqual : public ExpressionBinaryOp {
 public:
  ExpressionLessEqual() = default;
  ExpressionLessEqual(std::shared_ptr<Expression> lhs,
                      std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionLessEqual(const ExpressionLessEqual& other) = default;
  ~ExpressionLessEqual() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a AND operator expression.
 * e.g. new ExpressionAnd(new ExpressionLiteral(true), new
 * ExpressionLiteral(1)) => TRUE AND 1
 */
class ExpressionAnd : public ExpressionBinaryOp {
 public:
  ExpressionAnd() = default;
  ExpressionAnd(std::shared_ptr<Expression> lhs,
                std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionAnd(const ExpressionAnd& other) = default;
  ~ExpressionAnd() = default;

  ArrowExpression Evaluate() override;
};

/**
 * This class constructs a OR operator expression.
 * e.g. new ExpressionOr(new ExpressionLiteral(0), new
 * ExpressionLiteral(true)) => 0 OR TRUE
 */
class ExpressionOr : public ExpressionBinaryOp {
 public:
  ExpressionOr() = default;
  ExpressionOr(std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs)
      : ExpressionBinaryOp(lhs, rhs) {}
  ExpressionOr(const ExpressionOr& other) = default;
  ~ExpressionOr() = default;

  ArrowExpression Evaluate() override;
};

/**
 * Helper functions to construct a Expression.
 */
[[nodiscard]] static inline std::shared_ptr<Expression> _Property(
    const Property& property) {
  return std::make_shared<ExpressionProperty>(property);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _Property(
    const std::string& name) {
  return std::make_shared<ExpressionProperty>(name);
}

template <typename T>
[[nodiscard]] static inline std::shared_ptr<Expression> _Literal(T value) {
  return std::make_shared<ExpressionLiteral<T>>(value);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _Not(
    std::shared_ptr<Expression> expr) {
  return std::make_shared<ExpressionNot>(expr);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _IsNull(
    std::shared_ptr<Expression> expr, bool nan_is_null = false) {
  return std::make_shared<ExpressionIsNull>(expr, nan_is_null);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _Equal(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionEqual>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _NotEqual(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionNotEqual>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _GreaterThan(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionGreaterThan>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _GreaterEqual(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionGreaterEqual>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _LessThan(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionLessThan>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _LessEqual(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionLessEqual>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> _And(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionAnd>(lhs, rhs);
}

[[nodiscard]] static inline std::shared_ptr<Expression> Or(
    std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs) {
  return std::make_shared<ExpressionOr>(lhs, rhs);
}
}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTILS_EXPRESSION_H_

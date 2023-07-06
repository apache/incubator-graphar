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
 * This class wraps an arrow::compute::Expression and provides methods for
 * reading arrow::compute::Expression objects
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

class UnaryOperator : public Expression {
 public:
  UnaryOperator() = default;
  explicit UnaryOperator(std::shared_ptr<Expression> expr) : expr_(expr) {}
  UnaryOperator(const UnaryOperator& other) = default;
  virtual ~UnaryOperator() {}

 protected:
  std::shared_ptr<Expression> expr_;
};

class ExpressionNot : public UnaryOperator {
 public:
  ExpressionNot() = default;
  explicit ExpressionNot(std::shared_ptr<Expression> expr)
      : UnaryOperator(expr) {}
  ExpressionNot(const ExpressionNot& other) = default;
  ~ExpressionNot() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionIsNull : public UnaryOperator {
 public:
  ExpressionIsNull() = default;
  explicit ExpressionIsNull(std::shared_ptr<Expression> expr,
                            bool nan_is_null = false)
      : UnaryOperator(expr), nan_is_null_(nan_is_null) {}
  ExpressionIsNull(const ExpressionIsNull& other) = default;
  ~ExpressionIsNull() = default;

  ArrowExpression Evaluate() override;

 private:
  bool nan_is_null_;
};

class BinaryOperator : public Expression {
 public:
  BinaryOperator() = default;
  BinaryOperator(std::shared_ptr<Expression> lhs,
                 std::shared_ptr<Expression> rhs)
      : lhs_(lhs), rhs_(rhs) {}
  BinaryOperator(const BinaryOperator& other) = default;
  ~BinaryOperator() = default;

 protected:
  std::shared_ptr<Expression> lhs_;
  std::shared_ptr<Expression> rhs_;
};

class ExpressionEqual : public BinaryOperator {
 public:
  ExpressionEqual() = default;
  ExpressionEqual(std::shared_ptr<Expression> lhs,
                  std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionEqual(const ExpressionEqual& other) = default;
  ~ExpressionEqual() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionNotEqual : public BinaryOperator {
 public:
  ExpressionNotEqual() = default;
  ExpressionNotEqual(std::shared_ptr<Expression> lhs,
                     std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionNotEqual(const ExpressionNotEqual& other) = default;
  ~ExpressionNotEqual() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionGreaterThan : public BinaryOperator {
 public:
  ExpressionGreaterThan() = default;
  ExpressionGreaterThan(std::shared_ptr<Expression> lhs,
                        std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionGreaterThan(const ExpressionGreaterThan& other) = default;
  ~ExpressionGreaterThan() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionGreaterEqual : public BinaryOperator {
 public:
  ExpressionGreaterEqual() = default;
  ExpressionGreaterEqual(std::shared_ptr<Expression> lhs,
                         std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionGreaterEqual(const ExpressionGreaterEqual& other) = default;
  ~ExpressionGreaterEqual() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionLessThan : public BinaryOperator {
 public:
  ExpressionLessThan() = default;
  ExpressionLessThan(std::shared_ptr<Expression> lhs,
                     std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionLessThan(const ExpressionLessThan& other) = default;
  ~ExpressionLessThan() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionLessEqual : public BinaryOperator {
 public:
  ExpressionLessEqual() = default;
  ExpressionLessEqual(std::shared_ptr<Expression> lhs,
                      std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionLessEqual(const ExpressionLessEqual& other) = default;
  ~ExpressionLessEqual() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionAnd : public BinaryOperator {
 public:
  ExpressionAnd() = default;
  ExpressionAnd(std::shared_ptr<Expression> lhs,
                std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionAnd(const ExpressionAnd& other) = default;
  ~ExpressionAnd() = default;

  ArrowExpression Evaluate() override;
};

class ExpressionOr : public BinaryOperator {
 public:
  ExpressionOr() = default;
  ExpressionOr(std::shared_ptr<Expression> lhs, std::shared_ptr<Expression> rhs)
      : BinaryOperator(lhs, rhs) {}
  ExpressionOr(const ExpressionOr& other) = default;
  ~ExpressionOr() = default;

  ArrowExpression Evaluate() override;
};

/**
 * Helper functions to Construct Expression.
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

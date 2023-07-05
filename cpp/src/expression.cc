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
#include "gar/utils/expression.h"

namespace GAR_NAMESPACE_INTERNAL {

ArrowExpression ExpressionProperty::Evaluate() {
  return arrow::compute::field_ref(property_.name);
}
ArrowExpression ExpressionNot::Evaluate() {
  return arrow::compute::not_(expr_->Evaluate());
}

ArrowExpression ExpressionIsNull::Evaluate() {
  return arrow::compute::is_null(expr_->Evaluate());
}

ArrowExpression ExpressionEqual::Evaluate() {
  return arrow::compute::equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionNotEqual::Evaluate() {
  return arrow::compute::not_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionGreaterThan::Evaluate() {
  return arrow::compute::greater(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionGreaterEqual::Evaluate() {
  return arrow::compute::greater_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionLessThan::Evaluate() {
  return arrow::compute::less(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionLessEqual::Evaluate() {
  return arrow::compute::less_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionAnd::Evaluate() {
  return arrow::compute::and_(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression ExpressionOr::Evaluate() {
  return arrow::compute::or_(lhs_->Evaluate(), rhs_->Evaluate());
}

}  // namespace GAR_NAMESPACE_INTERNAL

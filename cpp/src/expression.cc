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
ArrowExpression OperatorNot::Evaluate() {
  return arrow::compute::not_(expr_->Evaluate());
}

ArrowExpression OperatorIsNull::Evaluate() {
  return arrow::compute::is_null(expr_->Evaluate());
}

ArrowExpression OperatorEqual::Evaluate() {
  return arrow::compute::equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorNotEqual::Evaluate() {
  return arrow::compute::not_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorGreater::Evaluate() {
  return arrow::compute::greater(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorGreaterEqual::Evaluate() {
  return arrow::compute::greater_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorLess::Evaluate() {
  return arrow::compute::less(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorLessEqual::Evaluate() {
  return arrow::compute::less_equal(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorAnd::Evaluate() {
  return arrow::compute::and_(lhs_->Evaluate(), rhs_->Evaluate());
}

ArrowExpression OperatorOr::Evaluate() {
  return arrow::compute::or_(lhs_->Evaluate(), rhs_->Evaluate());
}

}  // namespace GAR_NAMESPACE_INTERNAL

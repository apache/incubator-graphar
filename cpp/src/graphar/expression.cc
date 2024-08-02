/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "graphar/expression.h"
#include "graphar/result.h"

namespace graphar {

Result<ArrowExpression> ExpressionProperty::Evaluate() {
  return arrow::compute::field_ref(property_.name);
}
Result<ArrowExpression> ExpressionNot::Evaluate() {
  GAR_ASSIGN_OR_RAISE(auto expr, expr_->Evaluate());
  return arrow::compute::not_(expr);
}

Result<ArrowExpression> ExpressionEqual::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::equal(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionNotEqual::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::not_equal(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionGreaterThan::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::greater(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionGreaterEqual::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::greater_equal(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionLessThan::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::less(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionLessEqual::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::less_equal(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionAnd::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::and_(lexpr, rexpr);
}

Result<ArrowExpression> ExpressionOr::Evaluate() {
  GAR_RETURN_NOT_OK(CheckNullArgs(lhs_, rhs_));
  GAR_ASSIGN_OR_RAISE(auto lexpr, lhs_->Evaluate());
  GAR_ASSIGN_OR_RAISE(auto rexpr, rhs_->Evaluate());
  return arrow::compute::or_(lexpr, rexpr);
}

}  // namespace graphar

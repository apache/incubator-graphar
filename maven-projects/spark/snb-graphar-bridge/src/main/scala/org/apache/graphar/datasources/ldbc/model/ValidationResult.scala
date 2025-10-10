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

package org.apache.graphar.datasources.ldbc.model

/** 验证结果密封特质 */
sealed trait ValidationResult {
  def isSuccess: Boolean  // 是否验证成功
  def getErrors: List[String]  // 错误信息列表
}

/** 验证成功结果 */
case object ValidationSuccess extends ValidationResult {
  override def isSuccess: Boolean = true
  override def getErrors: List[String] = List.empty
}

/** 验证失败结果 */
case class ValidationFailure(errors: List[String]) extends ValidationResult {
  override def isSuccess: Boolean = false
  override def getErrors: List[String] = errors
}

/** 验证结果伴生对象 */
object ValidationResult {
  def success(): ValidationResult = ValidationSuccess  // 创建成功结果

  def failure(error: String): ValidationResult = ValidationFailure(List(error))  // 创建单错误失败结果

  def failure(errors: List[String]): ValidationResult = ValidationFailure(errors)  // 创建多错误失败结果

  // 合并多个验证结果
  def combine(results: ValidationResult*): ValidationResult = {
    val allErrors = results.collect { case ValidationFailure(errors) => errors }.flatten.toList
    if (allErrors.nonEmpty) ValidationFailure(allErrors) else ValidationSuccess
  }
}
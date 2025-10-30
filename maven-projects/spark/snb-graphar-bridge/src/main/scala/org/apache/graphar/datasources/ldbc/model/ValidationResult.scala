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

/** Validation resultsealed trait */
sealed trait ValidationResult {
  def isSuccess: Boolean // whethervalidation successful
  def getErrors: List[String] // error informationlist
}

/** validation successfulresult */
case object ValidationSuccess extends ValidationResult {
  override def isSuccess: Boolean = true
  override def getErrors: List[String] = List.empty
}

/** VerifyFailureresult */
case class ValidationFailure(errors: List[String]) extends ValidationResult {
  override def isSuccess: Boolean = false
  override def getErrors: List[String] = errors
}

/** Validation resultCompanion object */
object ValidationResult {
  def success(): ValidationResult = ValidationSuccess // CreateSuccessresult

  def failure(error: String): ValidationResult = ValidationFailure(
    List(error)
  ) // Create single error failure result

  def failure(errors: List[String]): ValidationResult = ValidationFailure(
    errors
  ) // Create multiple errors failure result

  // MergemultipleValidation result
  def combine(results: ValidationResult*): ValidationResult = {
    val allErrors = results
      .collect { case ValidationFailure(errors) => errors }
      .flatten
      .toList
    if (allErrors.nonEmpty) ValidationFailure(allErrors) else ValidationSuccess
  }
}

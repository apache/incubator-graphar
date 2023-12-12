/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GAR_UTIL_RESULT_H_
#define GAR_UTIL_RESULT_H_

#include <utility>

#include "gar/external/result.hpp"

#include "gar/util/status.h"

#define GAR_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr)          \
  auto&& result_name = (rexpr);                                    \
  GAR_RETURN_IF_((result_name).has_error(), (result_name).error(), \
                 GAR_STRINGIFY(rexpr));                            \
  lhs = std::move(result_name).value();

#define GAR_ASSIGN_OR_RAISE_ERROR_IMPL(result_name, lhs, rexpr)         \
  auto&& result_name = (rexpr);                                         \
  GAR_RAISE_ERROR_IF_((result_name).has_error(), (result_name).error(), \
                      GAR_STRINGIFY(rexpr));                            \
  lhs = std::move(result_name).value();

#define GAR_ASSIGN_OR_RAISE_NAME(x, y) GAR_CONCAT(x, y)

/**
 * @brief Execute an expression that returns a Result, extracting its value
 * into the variable defined by `lhs` (or returning a Status on error).
 *
 * Example: Assigning to a new value:
 *   GAR_ASSIGN_OR_RAISE(auto value, MaybeGetValue(arg));
 *
 * Example: Assigning to an existing value:
 *   ValueType value;
 *   GAR_ASSIGN_OR_RAISE(value, MaybeGetValue(arg));
 *
 * WARNING: GAR_ASSIGN_OR_RAISE expands into multiple statements;
 * it cannot be used in a single statement (e.g. as the body of an if
 * statement without {})!
 *
 * WARNING: GAR_ASSIGN_OR_RAISE `std::move`s its right operand. If you have
 * an lvalue Result which you *don't* want to move out of cast appropriately.
 *
 * WARNING: GAR_ASSIGN_OR_RAISE is not a single expression; it will not
 * maintain lifetimes of all temporaries in `rexpr` (e.g.
 * `GAR_ASSIGN_OR_RAISE(auto x, MakeTemp().GetResultRef());`
 * will most likely segfault)!
 */
#define GAR_ASSIGN_OR_RAISE(lhs, rexpr) \
  GAR_ASSIGN_OR_RAISE_IMPL(             \
      GAR_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

/**
 * @brief Execute an expression that returns a Result, extracting its value
 * into the variable defined by `lhs` (or throw an runtime error).
 *
 * Example: Assigning to a new value:
 *   GAR_ASSIGN_OR_RAISE_ERROR(auto value, MaybeGetValue(arg));
 *
 * Example: Assigning to an existing value:
 *   ValueType value;
 *   GAR_ASSIGN_OR_RAISE_ERROR(value, MaybeGetValue(arg));
 */
#define GAR_ASSIGN_OR_RAISE_ERROR(lhs, rexpr) \
  GAR_ASSIGN_OR_RAISE_ERROR_IMPL(             \
      GAR_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

#define GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                                            \
  if (!result_name.status().ok()) {                                        \
    return ::GAR_NAMESPACE_INTERNAL::Status::ArrowError(                   \
        result_name.status().ToString());                                  \
  }                                                                        \
  lhs = std::move(result_name).ValueOrDie();

/**
 * @brief Execute an expression that returns a Arrow Result, extracting its
 * value into the variable defined by `lhs` (or returning a Status on error).
 *
 * Example: Assigning to a new value:
 *   GAR_ASSIGN_OR_RAISE(auto value, MaybeGetValue(arg));
 *
 * Example: Assigning to an existing value:
 *   ValueType value;
 *   GAR_ASSIGN_OR_RAISE(value, MaybeGetValue(arg));
 */
#define GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(lhs, rexpr) \
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN_IMPL(             \
      GAR_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

namespace GAR_NAMESPACE_INTERNAL {

/**
 * @A class for representing either a usable value, or an error.
 *
 * A Result object either contains a value of type `T` or a Status object
 * explaining why such a value is not present. The type `T` must be
 * copy-constructible and/or move-constructible.
 *
 * The state of a Result object may be determined by calling has_error() or
 * status(). The has_error() method returns false if the object contains a
 * valid value. The status() method returns the internal Status object. A
 * Result object that contains a valid value will return an OK Status for a
 * call to status().
 *
 * A value of type `T` may be extracted from a Result object through a call
 * to value(). This function should only be called if a call to has_error()
 * returns false. Sample usage:
 *
 * ```
 *   gar::Result<Foo> result = CalculateFoo();
 *   if (!result.has_error()) {
 *     Foo foo = result.value();
 *     foo.DoSomethingCool();
 *   } else {
 *     std::err << result.status();
 *  }
 * ```
 */
template <typename T>
using Result = cpp::result<T, Status>;

namespace internal {

// Extract Status from Status or Result<T>
// Useful for the status check macros such as RETURN_NOT_OK.
template <typename T>
inline const Status& GenericToStatus(const Result<T>& res) {
  return res.status();
}
template <typename T>
inline Status GenericToStatus(Result<T>&& res) {
  return std::move(res).status();
}

}  // namespace internal

}  // namespace GAR_NAMESPACE_INTERNAL
#endif  // GAR_UTIL_RESULT_H_

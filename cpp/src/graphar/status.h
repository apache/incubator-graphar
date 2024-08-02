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

#pragma once

#include <sstream>
#include <string>
#include <utility>

#include "graphar/macros.h"

#define GAR_RETURN_IF_(condition, status, _) \
  do {                                       \
    if (GAR_PREDICT_FALSE(condition)) {      \
      return (status);                       \
    }                                        \
  } while (0)

/** @brief Propagate any non-successful Status to the caller. */
#define GAR_RETURN_NOT_OK(status)                                         \
  do {                                                                    \
    ::graphar::Status __s = ::graphar::internal::GenericToStatus(status); \
    GAR_RETURN_IF_(!__s.ok(), __s, GAR_STRINGIFY(status));                \
  } while (false)

/** @brief Propagate any non-successful Arrow Status to the caller. */
#define RETURN_NOT_ARROW_OK(status)                            \
  do {                                                         \
    if (GAR_PREDICT_FALSE(!status.ok())) {                     \
      return ::graphar::Status::ArrowError(status.ToString()); \
    }                                                          \
  } while (false)

#define GAR_RAISE_ERROR_IF_(condition, status, _) \
  do {                                            \
    if (GAR_PREDICT_FALSE(condition)) {           \
      throw std::runtime_error(status.message()); \
    }                                             \
  } while (0)

/** @brief Throw runtime error if Status not OK. */
#define GAR_RAISE_ERROR_NOT_OK(status)                                    \
  do {                                                                    \
    ::graphar::Status __s = ::graphar::internal::GenericToStatus(status); \
    GAR_RAISE_ERROR_IF_(!__s.ok(), __s, GAR_STRINGIFY(status));           \
  } while (false)

namespace graphar::util {
template <typename Head>
void StringBuilderRecursive(std::ostringstream& stream, Head&& head) {
  stream << head;
}

template <typename Head, typename... Tail>
void StringBuilderRecursive(std::ostringstream& stream, Head&& head,
                            Tail&&... tail) {
  StringBuilderRecursive(stream, std::forward<Head>(head));
  StringBuilderRecursive(stream, std::forward<Tail>(tail)...);
}

template <typename... Args>
std::string StringBuilder(Args&&... args) {
  std::ostringstream ss;
  StringBuilderRecursive(ss, std::forward<Args>(args)...);
  return ss.str();
}
}  // namespace  graphar::util

namespace graphar {
/**
 * An enum class representing the status codes for success or error outcomes.
 */
enum class StatusCode : unsigned char {
  // success status
  kOK = 0,
  // error status for failed key lookups
  kKeyError,
  // error status for type errors
  kTypeError,
  // error status for invalid data
  kInvalid,
  // error status when an index is out of bounds
  kIndexError,
  // error status for out-of-memory conditions
  kOutOfMemory,
  // error status when some IO-related operation failed
  kIOError,
  // error status when some yaml parse related operation failed
  kYamlError,
  // error status when some arrow-related operation failed
  kArrowError,

  // error status for unknown errors
  kUnknownError,
};

/** @brief Status outcome object (success or error)
 *
 * The Status object is an object holding the outcome of an operation.
 * The outcome is represented as a StatusCode, either success
 * (StatusCode::OK) or an error (any other of the StatusCode enumeration
 * values).
 *
 * Additionally, if an error occurred, a specific error message is generally
 * attached.
 */
class Status {
 public:
  /** Create a success status. */
  Status() noexcept : state_(nullptr) {}
  /** Destructor. */
  ~Status() noexcept {
    if (state_ != nullptr) {
      deleteState();
    }
  }
  /**
   * @brief Constructs a status with the specified error code and message.
   * @param code The error code of the status.
   * @param msg The error message of the status.
   */
  Status(StatusCode code, const std::string& msg) {
    state_ = new State;
    state_->code = code;
    state_->msg = msg;
  }
  /** Copy the specified status. */
  inline Status(const Status& s)
      : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}
  /**  Move the specified status. */
  inline Status(Status&& s) noexcept : state_(s.state_) { s.state_ = nullptr; }
  /** Move assignment operator. */
  inline Status& operator=(Status&& s) noexcept {
    delete state_;
    state_ = s.state_;
    s.state_ = nullptr;
    return *this;
  }

  /** Returns a success status. */
  inline static Status OK() { return Status(); }

  template <typename... Args>
  static Status FromArgs(StatusCode code, Args... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...));
  }

  /** Returns an error status when some IO-related operation failed. */
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status::FromArgs(StatusCode::kIOError, std::forward<Args>(args)...);
  }

  /** Returns an error status for failed key lookups. */
  template <typename... Args>
  static Status KeyError(Args&&... args) {
    return Status::FromArgs(StatusCode::kKeyError, std::forward<Args>(args)...);
  }

  /** Returns an error status for failed type matches. */
  template <typename... Args>
  static Status TypeError(Args&&... args) {
    return Status::FromArgs(StatusCode::kTypeError,
                            std::forward<Args>(args)...);
  }

  /**
   * Returns an error status for invalid data (for example a string that fails
   * parsing).
   */
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status::FromArgs(StatusCode::kInvalid, std::forward<Args>(args)...);
  }

  /**
   * Return an error status when an index is out of bounds.
   *
   */
  template <typename... Args>
  static Status IndexError(Args&&... args) {
    return Status::FromArgs(StatusCode::kIndexError,
                            std::forward<Args>(args)...);
  }

  /** Return an error status when some yaml parse related operation failed. */
  template <typename... Args>
  static Status YamlError(Args&&... args) {
    return Status::FromArgs(StatusCode::kYamlError,
                            std::forward<Args>(args)...);
  }

  /** Return an error status when some arrow-related operation failed. */
  template <typename... Args>
  static Status ArrowError(Args&&... args) {
    return Status::FromArgs(StatusCode::kArrowError,
                            std::forward<Args>(args)...);
  }

  /** Return an error status for unknown errors. */
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status::FromArgs(StatusCode::kUnknownError,
                            std::forward<Args>(args)...);
  }

  /** Return true iff the status indicates success. */
  constexpr bool ok() const { return (state_ == nullptr); }

  /** Return true iff the status indicates a key lookup error. */
  constexpr bool IsKeyError() const { return code() == StatusCode::kKeyError; }
  /** Return true iff the status indicates a type match error. */
  constexpr bool IsTypeError() const {
    return code() == StatusCode::kTypeError;
  }
  /** Return true iff the status indicates invalid data. */
  constexpr bool IsInvalid() const { return code() == StatusCode::kInvalid; }
  /** Return true iff the status indicates an index out of bounds. */
  constexpr bool IsIndexError() const {
    return code() == StatusCode::kIndexError;
  }
  /** Return true iff the status indicates an yaml parse related failure. */
  constexpr bool IsYamlError() const {
    return code() == StatusCode::kYamlError;
  }
  /** Return true iff the status indicates an arrow-related failure. */
  constexpr bool IsArrowError() const {
    return code() == StatusCode::kArrowError;
  }

  /** Return the StatusCode value attached to this status. */
  constexpr StatusCode code() const {
    return ok() ? StatusCode::kOK : state_->code;
  }

  /** Return the specific error message attached to this status. */
  const std::string& message() const {
    static const std::string no_message = "";
    return ok() ? no_message : state_->msg;
  }

 private:
  void deleteState() {
    delete state_;
    state_ = nullptr;
  }

  struct State {
    StatusCode code;
    std::string msg;
  };
  State* state_;
};

}  // namespace graphar

namespace graphar::internal {

// Extract Status from Status or Result<T>
// Useful for the status check macros such as RETURN_NOT_OK.
inline const Status& GenericToStatus(const Status& st) { return st; }
inline Status GenericToStatus(Status&& st) { return std::move(st); }

}  // namespace graphar::internal

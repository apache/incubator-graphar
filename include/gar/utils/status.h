/** Copyright 2022 Alibaba Group Holding Limited.

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

#ifndef GAR_UTILS_STATUS_H_
#define GAR_UTILS_STATUS_H_

#include <string>
#include <utility>

#include "gar/utils/macros.h"

#define GAR_RETURN_IF_(condition, status, _) \
  do {                                       \
    if (GAR_PREDICT_FALSE(condition)) {      \
      return (status);                       \
    }                                        \
  } while (0)

/// \brief Propagate any non-successful Status to the caller
#define GAR_RETURN_NOT_OK(status)                                    \
  do {                                                               \
    ::GAR_NAMESPACE_INTERNAL::Status __s =                           \
        ::GAR_NAMESPACE_INTERNAL::internal::GenericToStatus(status); \
    GAR_RETURN_IF_(!__s.ok(), __s, GAR_STRINGIFY(status));           \
  } while (false)

/// \brief Propagate any non-successful Arrow Status to the caller
#define RETURN_NOT_ARROW_OK(status)                                           \
  do {                                                                        \
    if (GAR_PREDICT_FALSE(!status.ok())) {                                    \
      return ::GAR_NAMESPACE_INTERNAL::Status::ArrowError(status.ToString()); \
    }                                                                         \
  } while (false)

#define GAR_RAISE_ERROR_IF_(condition, status, _) \
  do {                                            \
    if (GAR_PREDICT_FALSE(condition)) {           \
      throw std::runtime_error(status.message()); \
    }                                             \
  } while (0)

/// \brief Throw runtime error if Status not OK
#define GAR_RAISE_ERROR_NOT_OK(status)                               \
  do {                                                               \
    ::GAR_NAMESPACE_INTERNAL::Status __s =                           \
        ::GAR_NAMESPACE_INTERNAL::internal::GenericToStatus(status); \
    GAR_RAISE_ERROR_IF_(!__s.ok(), __s, GAR_STRINGIFY(status));      \
  } while (false)

namespace GAR_NAMESPACE_INTERNAL {

enum class StatusCode : unsigned char {
  kOK = 0,
  kKeyError,
  kTypeError,
  kInvalid,
  kInvalidValue,
  kInvalidArgument,
  kInvalidOperation,
  kOutOfRange,
  kOutOfMemory,
  kEndOfChunk,
  kIOError,
  kYamlError,
  kArrowError,

  kUnknownError,
};

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration
/// values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class Status {
 public:
  // Create a success status.
  Status() noexcept : state_(nullptr) {}
  ~Status() noexcept {
    if (state_ != nullptr) {
      deleteState();
    }
  }
  /// Create a status with the specified error code and message.
  Status(StatusCode code, const std::string& msg) {
    state_ = new State;
    state_->code = code;
    state_->msg = msg;
  }
  /// Copy the specified status.
  inline Status(const Status& s)
      : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}
  /// Move the specified status.
  inline Status(Status&& s) noexcept : state_(s.state_) { s.state_ = nullptr; }
  /// Move assignment operator.
  inline Status& operator=(Status&& s) noexcept {
    delete state_;
    state_ = s.state_;
    s.state_ = nullptr;
    return *this;
  }

  /// Return a success status
  inline static Status OK() { return Status(); }

  /// Return an error status when some IO-related operation failed
  static Status IOError(const std::string& msg = "") {
    return Status(StatusCode::kIOError, msg);
  }

  /// Return an error status for failed key lookups
  static Status KeyError(const std::string& msg = "") {
    return Status(StatusCode::kKeyError, msg);
  }

  /// Return an error status for failed type matches
  static Status TypeError(const std::string& msg = "") {
    return Status(StatusCode::kTypeError, msg);
  }

  /// Return an error status for invalid data (for example a string that fails
  /// parsing)
  static Status Invalid(const std::string& msg = "") {
    return Status(StatusCode::kInvalid, msg);
  }

  static Status InvalidValue(const std::string& msg = "") {
    return Status(StatusCode::kInvalidValue, msg);
  }

  static Status InvalidArgument(const std::string& msg = "") {
    return Status(StatusCode::kInvalidArgument, msg);
  }

  static Status InvalidOperation(const std::string& msg = "") {
    return Status(StatusCode::kInvalidOperation, msg);
  }

  /// Return an error status for value is out of range (for example next_chunk
  /// is out of range)
  static Status OutOfRange(const std::string& msg = "") {
    return Status(StatusCode::kOutOfRange, msg);
  }

  static Status EndOfChunk(const std::string& msg = "") {
    return Status(StatusCode::kEndOfChunk, msg);
  }

  /// Return an error status when some yaml-cpp related operation failed
  static Status YamlError(const std::string& msg = "") {
    return Status(StatusCode::kYamlError, msg);
  }

  /// Return an error status when some arrow-related operation failed
  static Status ArrowError(const std::string& msg = "") {
    return Status(StatusCode::kArrowError, msg);
  }

  /// Return an error status for unknown errors
  static Status UnknownError(const std::string& msg = "") {
    return Status(StatusCode::kArrowError, msg);
  }

  /// Return true iff the status indicates success.
  bool ok() const { return (state_ == nullptr); }

  /// Return true iff the status indicates a key lookup error.
  bool IsKeyError() const { return code() == StatusCode::kKeyError; }
  /// Return true iff the status indicates a type match error.
  bool IsTypeError() const { return code() == StatusCode::kTypeError; }
  /// Return true iff the status indicates invalid data.
  bool IsInvalid() const { return code() == StatusCode::kInvalid; }
  bool IsInvalidValue() const { return code() == StatusCode::kInvalidValue; }
  bool IsInvalidArgument() const {
    return code() == StatusCode::kInvalidArgument;
  }
  bool IsInvalidOperation() const {
    return code() == StatusCode::kInvalidOperation;
  }
  bool IsOutOfRange() const { return code() == StatusCode::kOutOfRange; }
  bool IsEndOfChunk() const { return code() == StatusCode::kEndOfChunk; }
  /// Return true iff the status indicates an yaml-cpp related failure.
  bool IsYamlError() const { return code() == StatusCode::kYamlError; }
  /// Return true iff the status indicates an arrow-related failure.
  bool IsArrowError() const { return code() == StatusCode::kArrowError; }

  /// Return the StatusCode value attached to this status.
  StatusCode code() const { return ok() ? StatusCode::kOK : state_->code; }

  /// Return the specific error message attached to this status.
  std::string message() const { return ok() ? "" : state_->msg; }

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

namespace internal {

// Extract Status from Status or Result<T>
// Useful for the status check macros such as RETURN_NOT_OK.
inline const Status& GenericToStatus(const Status& st) { return st; }
inline Status GenericToStatus(Status&& st) { return std::move(st); }

}  // namespace internal

}  // namespace GAR_NAMESPACE_INTERNAL

#endif  // GAR_UTILS_STATUS_H_

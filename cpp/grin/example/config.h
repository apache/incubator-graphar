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

#include <filesystem>
#include <string>

#ifndef CPP_GRIN_EXAMPLE_CONFIG_H_
#define CPP_GRIN_EXAMPLE_CONFIG_H_

#define gar_get_internal_id_from_original_id(x) (x & 0xFFFFF)

// Define a new macro that is just like the standard C assert macro,
// except that it works even in optimized builds (where NDEBUG is
// defined) and it prints the failed assertion to stderr.
#ifndef ASSERT
#define ASSERT(x)                                    \
  if (!(x)) {                                        \
    char buf[2048];                                  \
    snprintf(buf, sizeof(buf),                       \
             "Assertion failed in \"%s\", line %d\n" \
             "\tProbable bug in software.\n",        \
             __FILE__, __LINE__);                    \
    ABORT(buf);                                      \
  } else  // NOLINT
// The 'else' exists to catch the user's following semicolon
#endif

// Define a new macro that is just like the standard C abort macro,
// except that it prints the failed assertion to stderr.
#ifndef ABORT
#define ABORT(msg)              \
  do {                          \
    fprintf(stderr, "%s", msg); \
    fflush(stderr);             \
    abort();                    \
  } while (0)
#endif

// DASSERT is like ASSERT, but it only works in debug builds.
#ifdef DEBUG
#define DASSERT(x) ASSERT(x)
#else
#define DASSERT(x)
#endif

static const std::string TEST_DATA_PATH =  // NOLINT
    std::filesystem::path(__FILE__)
        .parent_path()
        .parent_path()
        .parent_path()
        .parent_path()
        .string() +
    "/testing/ldbc/ldbc.graph.yml";

static const std::string TEST_DATA_SMALL_PATH =  // NOLINT
    std::filesystem::path(__FILE__)
        .parent_path()
        .parent_path()
        .parent_path()
        .parent_path()
        .string() +
    "/testing/ldbc_sample/parquet/ldbc_sample.graph.yml";

static const std::string VERTEX_OID_NAME = "id";  // NOLINT

static const std::string PR_VERTEX_TYPE = "comment";          // NOLINT
static const std::string PR_EDGE_TYPE = "replyOf";            // NOLINT
static const std::string PR_TEST_DATA_PATH = TEST_DATA_PATH;  // NOLINT
static const int PR_MAX_ITERS = 30;

static const std::string BFS_VERTEX_TYPE = "person";                 // NOLINT
static const std::string BFS_EDGE_TYPE = "knows";                    // NOLINT
static const std::string BFS_TEST_DATA_PATH = TEST_DATA_SMALL_PATH;  // NOLINT
static const int64_t BFS_ROOT_ID = 0;

static const std::string CC_VERTEX_TYPE = "person";                 // NOLINT
static const std::string CC_EDGE_TYPE = "knows";                    // NOLINT
static const std::string CC_TEST_DATA_PATH = TEST_DATA_SMALL_PATH;  // NOLINT

static const std::string DIS_PR_VERTEX_TYPE = "person";  // NOLINT
static const std::string DIS_PR_EDGE_TYPE = "knows";     // NOLINT
static const std::string DIS_PR_TEST_DATA_PATH =
    TEST_DATA_SMALL_PATH;  // NOLINT
static const int DIS_PR_MAX_ITERS = 10;

static const std::string PROPERTY_TEST_DATA_PATH = TEST_DATA_PATH;  // NOLINT

#endif  // CPP_GRIN_EXAMPLE_CONFIG_H_

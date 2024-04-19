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

#include <cstdint>

// namespace config

#define GAR_EXPAND(x) x
#define GAR_STRINGIFY(x) #x
#define GAR_CONCAT(x, y) x##y

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define GAR_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#define GAR_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#elif defined(_MSC_VER)
#define GAR_PREDICT_FALSE(x) (x)
#define GAR_PREDICT_TRUE(x) (x)
#else
#define GAR_PREDICT_FALSE(x) (x)
#define GAR_PREDICT_TRUE(x) (x)
#endif

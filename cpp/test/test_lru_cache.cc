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

#include <string>

#include "graphar/lru_cache.h"

#include <catch2/catch_test_macros.hpp>

namespace graphar {

TEST_CASE("LRUCache basic operations") {
  LRUCache<int, std::string> cache(3);

  SECTION("Empty cache returns nullptr") {
    REQUIRE(cache.Get(1) == nullptr);
    REQUIRE(cache.Size() == 0);
  }

  SECTION("Put and Get") {
    cache.Put(1, "one");
    auto* value = cache.Get(1);
    REQUIRE(value != nullptr);
    REQUIRE(*value == "one");
    REQUIRE(cache.Size() == 1);
  }

  SECTION("Multiple Put and Get") {
    cache.Put(1, "one");
    cache.Put(2, "two");
    cache.Put(3, "three");

    auto* v1 = cache.Get(1);
    auto* v2 = cache.Get(2);
    auto* v3 = cache.Get(3);

    REQUIRE(v1 != nullptr);
    REQUIRE(v2 != nullptr);
    REQUIRE(v3 != nullptr);
    REQUIRE(*v1 == "one");
    REQUIRE(*v2 == "two");
    REQUIRE(*v3 == "three");
    REQUIRE(cache.Size() == 3);
  }

  SECTION("Update existing key") {
    cache.Put(1, "one");
    cache.Put(1, "updated");

    auto* value = cache.Get(1);
    REQUIRE(value != nullptr);
    REQUIRE(*value == "updated");
    REQUIRE(cache.Size() == 1);
  }

  SECTION("Get updates recency") {
    cache.Put(1, "one");
    cache.Put(2, "two");
    cache.Put(3, "three");

    cache.Get(1);
    cache.Put(4, "four");

    REQUIRE(cache.Get(1) != nullptr);
    REQUIRE(cache.Get(2) == nullptr);
    REQUIRE(cache.Get(3) != nullptr);
    REQUIRE(cache.Get(4) != nullptr);
    REQUIRE(cache.Size() == 3);
  }
}

TEST_CASE("LRUCache eviction") {
  SECTION("Evict when exceeding capacity") {
    LRUCache<int, std::string> cache(2);

    cache.Put(1, "one");
    cache.Put(2, "two");
    cache.Put(3, "three");

    REQUIRE(cache.Get(1) == nullptr);
    REQUIRE(cache.Get(2) != nullptr);
    REQUIRE(cache.Get(3) != nullptr);
    REQUIRE(cache.Size() == 2);
  }

  SECTION("Evict least recently used") {
    LRUCache<int, std::string> cache(3);

    cache.Put(1, "one");
    cache.Put(2, "two");
    cache.Put(3, "three");

    cache.Get(1);
    cache.Get(2);
    cache.Put(4, "four");

    REQUIRE(cache.Get(1) != nullptr);
    REQUIRE(cache.Get(2) != nullptr);
    REQUIRE(cache.Get(3) == nullptr);
    REQUIRE(cache.Get(4) != nullptr);
  }
}

TEST_CASE("LRUCache Clear") {
  LRUCache<int, std::string> cache(3);

  cache.Put(1, "one");
  cache.Put(2, "two");
  cache.Clear();

  REQUIRE(cache.Get(1) == nullptr);
  REQUIRE(cache.Get(2) == nullptr);
  REQUIRE(cache.Size() == 0);
}

TEST_CASE("LRUCache with string keys") {
  LRUCache<std::string, int> cache(2);

  cache.Put("one", 1);
  cache.Put("two", 2);

  auto* v1 = cache.Get("one");
  auto* v2 = cache.Get("two");

  REQUIRE(v1 != nullptr);
  REQUIRE(v2 != nullptr);
  REQUIRE(*v1 == 1);
  REQUIRE(*v2 == 2);
}

TEST_CASE("LRUCache with PairHash") {
  LRUCache<std::pair<int, int>, std::string, PairHash> cache(2);

  cache.Put({1, 2}, "value1");
  cache.Put({3, 4}, "value2");

  auto* v1 = cache.Get({1, 2});
  auto* v2 = cache.Get({3, 4});

  REQUIRE(v1 != nullptr);
  REQUIRE(v2 != nullptr);
  REQUIRE(*v1 == "value1");
  REQUIRE(*v2 == "value2");

  REQUIRE(cache.Get({5, 6}) == nullptr);
}

TEST_CASE("LRUCache move semantics") {
  LRUCache<int, std::string> cache(2);

  std::string value = "test_value";
  cache.Put(1, std::move(value));

  auto* cached = cache.Get(1);
  REQUIRE(cached != nullptr);
  REQUIRE(*cached == "test_value");
}

TEST_CASE("LRUCache zero capacity edge case") {
  LRUCache<int, std::string> cache(0);

  cache.Put(1, "one");

  REQUIRE(cache.Size() == 0);
  REQUIRE(cache.Get(1) == nullptr);
}

}  // namespace graphar

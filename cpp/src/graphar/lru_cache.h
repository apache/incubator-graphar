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

#include <cstddef>
#include <list>
#include <unordered_map>
#include <utility>

namespace graphar {

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class LruCache {
 public:
  explicit LruCache(size_t capacity) : capacity_(capacity) {}

  Value* Get(const Key& key) {
    auto it = map_.find(key);
    if (it == map_.end()) {
      return nullptr;
    }
    // Move accessed item to front (most recently used)
    items_.splice(items_.begin(), items_, it->second);
    return &it->second->second;
  }

  void Put(const Key& key, Value value) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      // Update existing entry and move to front
      it->second->second = std::move(value);
      items_.splice(items_.begin(), items_, it->second);
      return;
    }
    // Evict least recently used if at capacity
    if (map_.size() >= capacity_) {
      auto& back = items_.back();
      map_.erase(back.first);
      items_.pop_back();
    }
    items_.emplace_front(key, std::move(value));
    map_[key] = items_.begin();
  }

  void Clear() {
    map_.clear();
    items_.clear();
  }

  size_t Size() const { return map_.size(); }

 private:
  size_t capacity_;
  std::list<std::pair<Key, Value>> items_;
  std::unordered_map<Key, typename std::list<std::pair<Key, Value>>::iterator,
                     Hash>
      map_;
};

struct PairHash {
  template <typename T1, typename T2>
  size_t operator()(const std::pair<T1, T2>& p) const {
    auto h1 = std::hash<T1>{}(p.first);
    auto h2 = std::hash<T2>{}(p.second);
    return h1 ^ (h2 << 32);
  }
};

}  // namespace graphar

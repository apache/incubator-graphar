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

#include <iosfwd>
#include <memory>
#include <string>

#include "graphar/result.h"

// forward declaration
namespace Yaml {
class Node;
}

namespace graphar {

/** A wrapper of ::Yaml::Node to provide functions to parse yaml. */
class Yaml {
 public:
  explicit Yaml(std::shared_ptr<::Yaml::Node> root_node)
      : root_node_(root_node) {}

  ~Yaml() = default;

  const ::Yaml::Node operator[](const std::string& key) const;

  /**
   * Loads the input string as Yaml instance.
   *
   * Return Status::YamlError if input string can not be loaded(malformed).
   */
  static Result<std::shared_ptr<Yaml>> Load(const std::string& input);

  /**
   * Loads the input stream as Yaml instance.
   *
   * Return Status::YamlError if input string can not be loaded(malformed).
   */
  static Result<std::shared_ptr<Yaml>> Load(std::iostream& input);

  /**
   * Loads the input file as a single Yaml instance.
   *
   * Return Status::YamlError if the file can not be loaded(malformed).
   */
  static Result<std::shared_ptr<Yaml>> LoadFile(const std::string& file_name);

 private:
  std::shared_ptr<::Yaml::Node> root_node_;
};

}  // namespace graphar

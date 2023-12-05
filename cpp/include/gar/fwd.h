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

#ifndef GAR_FWD_H_
#define GAR_FWD_H_

#include <memory>

#include "gar/external/result.hpp"

#include "gar/util/macros.h"
#include "gar/util/status.h"

namespace GAR_NAMESPACE_INTERNAL {

class Status;

template <typename T>
using Result = cpp::result<T, Status>; 

class Yaml;
class FileSystem;

/** Type of vertex id or vertex index. */
using IdType = int64_t;
enum class Type;
class DataType;
/** Type of file format */
enum FileType { CSV = 0, PARQUET = 1, ORC = 2 };
enum class AdjListType : uint8_t;

class InfoVersion;

class Property;
class PropertyGroup;
class AdjacentList;

class VertexInfo;
class EdgeInfo;
class GraphInfo;

using PropertyGroupVector = std::vector<std::shared_ptr<PropertyGroup>>;
using AdjacentListVector = std::vector<std::shared_ptr<AdjacentList>>;
using VertexInfoVector = std::vector<std::shared_ptr<VertexInfo>>;
using EdgeInfoVector = std::vector<std::shared_ptr<EdgeInfo>>;

}  // namespace GAR_NAMESPACE_INTERNAL


#endif // GAR_FWD_H_

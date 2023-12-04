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

namespace cpp {
template <typename T, typename E>
class result;
} // namespace cpp

namespace GAR_NAMESPACE_INTERNAL {

class Status;

template <typename T>
using Result = cpp::result<T, Status>;

class Yaml;
class FileSystem;

enum class Type;
class DataType;
enum FileType;
enum class AdjListType;

struct Property;
class PropertyGroup;
class AdjacentList;

class VertexInfo;
class EdgeInfo;
class GraphInfo;

using PropertyGroupVector = std::vector<std::shared_ptr<PropertyGroup>>;
using VertexInfoVector = std::vector<std::shared_ptr<VertexInfo>>;
using EdgeInfoVector = std::vector<std::shared_ptr<EdgeInfo>>;

}  // namespace GAR_NAMESPACE_INTERNAL


#endif // GAR_FWD_H_

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

#include <iostream>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"

#include "gar/writer/arrow_chunk_writer.h"

namespace GAR_NAMESPACE_INTERNAL {
// common methods

#if defined(ARROW_VERSION) && ARROW_VERSION >= 10000000
using AsyncGeneratorType =
    arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>>;
#else
using AsyncGeneratorType =
    arrow::AsyncGenerator<arrow::util::optional<arrow::compute::ExecBatch>>;
#endif

/**
 * @brief Execute a compute plan and collect the results as a table.
 *
 * @param exec_context The execution context.
 * @param plan The compute pan to execute.
 * @param schema The schema the input table.
 * @param sink_gen The async generator.
 */
Result<std::shared_ptr<arrow::Table>> ExecutePlanAndCollectAsTable(
    const arrow::compute::ExecContext& exec_context,
    std::shared_ptr<arrow::compute::ExecPlan> plan,
    std::shared_ptr<arrow::Schema> schema, AsyncGeneratorType sink_gen) {
  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      arrow::compute::MakeGeneratorReader(schema, std::move(sink_gen),
                                          exec_context.memory_pool());

  // validate the ExecPlan
  RETURN_NOT_ARROW_OK(plan->Validate());
  //  start the ExecPlan
  RETURN_NOT_ARROW_OK(plan->StartProducing());

  // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      response_table, arrow::Table::FromRecordBatchReader(sink_reader.get()));

  // stop producing
  plan->StopProducing();
  // plan mark finished
  auto future = plan->finished();
  if (!future.status().ok()) {
    return Status::ArrowError(future.status().ToString());
  }
  return response_table;
}

// implementations for VertexPropertyChunkWriter

Status VertexPropertyWriter::Validate(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType chunk_index,
    ValidateLevel validate_level) const noexcept {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // validate
  if (validate_level_ == ValidateLevel::no_validate)
    return Status::OK();
  if (input_table->num_rows() > vertex_info_.GetChunkSize())
    return Status::OutOfRange();
  if (!vertex_info_.ContainPropertyGroup(property_group))
    return Status::Invalid("invalid property group");
  if (chunk_index < 0)
    return Status::Invalid("invalid chunk index");
  return Status::OK();
}

Status VertexPropertyWriter::WriteVerticesNum(const IdType& count) const
    noexcept {
  GAR_ASSIGN_OR_RAISE(auto suffix, vertex_info_.GetVerticesNumFilePath());
  std::string path = prefix_ + suffix;
  return fs_->WriteValueToFile<IdType>(count, path);
}

Status VertexPropertyWriter::WriteChunk(const std::string& file_name,
                                        const PropertyGroup& property_group,
                                        IdType chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(auto suffix,
                      vertex_info_.GetFilePath(property_group, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status VertexPropertyWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType chunk_index) const noexcept {
  GAR_RETURN_NOT_OK(Validate(input_table, property_group, chunk_index));
  auto file_type = property_group.GetFileType();

  std::vector<int> indices;
  indices.clear();
  auto schema = input_table->schema();
  for (auto& property : property_group.GetProperties()) {
    int indice = schema->GetFieldIndex(property.name);
    if (indice == -1)
      return Status::InvalidOperation("invalid property");
    indices.push_back(indice);
  }
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto in_table,
                                       input_table->SelectColumns(indices));
  GAR_ASSIGN_OR_RAISE(auto suffix,
                      vertex_info_.GetFilePath(property_group, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path);
}

Status VertexPropertyWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType chunk_index) const
    noexcept {
  auto property_groups = vertex_info_.GetPropertyGroups();
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(WriteChunk(input_table, property_group, chunk_index));
  }
  return Status::OK();
}

Status VertexPropertyWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType start_chunk_index) const
    noexcept {
  IdType chunk_size = vertex_info_.GetChunkSize();
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size);
    GAR_RETURN_NOT_OK(WriteChunk(in_chunk, property_group, chunk_index));
  }
  return Status::OK();
}

Status VertexPropertyWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table,
    IdType start_chunk_index) const noexcept {
  auto property_groups = vertex_info_.GetPropertyGroups();
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(
        WriteTable(input_table, property_group, start_chunk_index));
  }
  return Status::OK();
}

// implementations for EdgeChunkWriter

Status EdgeChunkWriter::Validate(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    ValidateLevel validate_level) const noexcept {
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  if (!edge_info_.ContainAdjList(adj_list_type_))
    return Status::InvalidOperation("invalid adj list type");
  if (input_table->num_rows() > edge_info_.GetChunkSize())
    return Status::OutOfRange();
  if (vertex_chunk_index < 0)
    return Status::InvalidOperation("invalid vertex chunk index");
  return Status::OK();
}

Status EdgeChunkWriter::WriteOffsetChunk(const std::string& file_name,
                                         IdType vertex_chunk_index) const
    noexcept {
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_.GetAdjListOffsetFilePath(
                                       vertex_chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status EdgeChunkWriter::WriteAdjListChunk(const std::string& file_name,
                                          IdType vertex_chunk_index,
                                          IdType chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(
      auto suffix, edge_info_.GetAdjListFilePath(vertex_chunk_index,
                                                 chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status EdgeChunkWriter::WritePropertyChunk(const std::string& file_name,
                                           const PropertyGroup& property_group,
                                           IdType vertex_chunk_index,
                                           IdType chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_.GetPropertyFilePath(
                                       property_group, adj_list_type_,
                                       vertex_chunk_index, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->CopyFile(file_name, path);
}

Status EdgeChunkWriter::WriteOffsetChunk(
    const std::shared_ptr<arrow::Table>& input_table,
    IdType vertex_chunk_index) const noexcept {
  GAR_RETURN_NOT_OK(Validate(input_table, vertex_chunk_index));
  GAR_ASSIGN_OR_RAISE(auto file_type,
                      edge_info_.GetAdjListFileType(adj_list_type_));
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_.GetAdjListOffsetFilePath(
                                       vertex_chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(input_table, file_type, path);
}

Status EdgeChunkWriter::WriteAdjListChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index) const noexcept {
  GAR_RETURN_NOT_OK(Validate(input_table, vertex_chunk_index));
  GAR_ASSIGN_OR_RAISE(auto file_type,
                      edge_info_.GetAdjListFileType(adj_list_type_));
  std::vector<int> indices;
  indices.clear();
  auto schema = input_table->schema();
  int index = schema->GetFieldIndex(GeneralParams::kSrcIndexCol);
  if (index == -1)
    return Status::InvalidOperation("sources not provided");
  indices.push_back(index);
  index = schema->GetFieldIndex(GeneralParams::kDstIndexCol);
  if (index == -1)
    return Status::InvalidOperation("destinations not provided");
  indices.push_back(index);
  auto in_table = input_table->SelectColumns(indices).ValueOrDie();

  GAR_ASSIGN_OR_RAISE(
      auto suffix, edge_info_.GetAdjListFilePath(vertex_chunk_index,
                                                 chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path);
}

Status EdgeChunkWriter::WritePropertyChunk(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType vertex_chunk_index,
    IdType chunk_index) const noexcept {
  GAR_RETURN_NOT_OK(Validate(input_table, property_group, vertex_chunk_index));
  auto file_type = property_group.GetFileType();

  std::vector<int> indices;
  indices.clear();
  auto schema = input_table->schema();
  for (auto& property : property_group.GetProperties()) {
    int indice = schema->GetFieldIndex(property.name);
    if (indice == -1)
      return Status::InvalidOperation("invalid property");
    indices.push_back(indice);
  }
  auto in_table = input_table->SelectColumns(indices).ValueOrDie();
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_.GetPropertyFilePath(
                                       property_group, adj_list_type_,
                                       vertex_chunk_index, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path);
}

Status EdgeChunkWriter::WritePropertyChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(auto& property_groups,
                      edge_info_.GetPropertyGroups(adj_list_type_));
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(WritePropertyChunk(input_table, property_group,
                                         vertex_chunk_index, chunk_index));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index) const noexcept {
  GAR_RETURN_NOT_OK(
      WriteAdjListChunk(input_table, vertex_chunk_index, chunk_index));
  return WritePropertyChunk(input_table, vertex_chunk_index, chunk_index);
}

Status EdgeChunkWriter::WriteAdjListTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(
        WriteAdjListChunk(in_chunk, vertex_chunk_index, chunk_index));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(WritePropertyChunk(in_chunk, property_group,
                                         vertex_chunk_index, chunk_index));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(
        WritePropertyChunk(in_chunk, vertex_chunk_index, chunk_index));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(WriteChunk(in_chunk, vertex_chunk_index, chunk_index));
  }
  return Status::OK();
}

Status EdgeChunkWriter::SortAndWriteAdjListTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));

  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::ordered_by_dest) {
    GAR_ASSIGN_OR_RAISE(
        auto offset_table,
        getOffsetTable(response_table, getSortColumnName(adj_list_type_),
                       vertex_chunk_index));
    GAR_RETURN_NOT_OK(WriteOffsetChunk(offset_table, vertex_chunk_index));
  }

  return WriteAdjListTable(response_table, vertex_chunk_index,
                           start_chunk_index);
}

Status EdgeChunkWriter::SortAndWritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const PropertyGroup& property_group, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));
  return WritePropertyTable(response_table, property_group, vertex_chunk_index,
                            start_chunk_index);
}

Status EdgeChunkWriter::SortAndWritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));
  return WritePropertyTable(response_table, vertex_chunk_index,
                            start_chunk_index);
}

Status EdgeChunkWriter::SortAndWriteTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index) const noexcept {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));

  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::ordered_by_dest) {
    GAR_ASSIGN_OR_RAISE(
        auto offset_table,
        getOffsetTable(response_table, getSortColumnName(adj_list_type_),
                       vertex_chunk_index));
    GAR_RETURN_NOT_OK(WriteOffsetChunk(offset_table, vertex_chunk_index));
  }

  return WriteTable(response_table, vertex_chunk_index, start_chunk_index);
}

Result<std::shared_ptr<arrow::Table>> EdgeChunkWriter::getOffsetTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::string& column_name, IdType vertex_chunk_index) const noexcept {
  std::shared_ptr<arrow::ChunkedArray> column =
      input_table->GetColumnByName(column_name);
  int64_t array_index = 0, index = 0;
  auto ids =
      std::static_pointer_cast<arrow::Int64Array>(column->chunk(array_index));

  arrow::Int64Builder builder;
  IdType begin_index = vertex_chunk_index * vertex_chunk_size_,
         end_index = begin_index + vertex_chunk_size_;
  RETURN_NOT_ARROW_OK(builder.Append(0));
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  std::string property = GeneralParams::kOffsetCol;
  schema_vector.push_back(arrow::field(
      property, DataType::DataTypeToArrowDataType(DataType::type::INT64)));

  int64_t global_index = 0;
  for (IdType i = begin_index; i < end_index; i++) {
    while (true) {
      if (array_index >= column->num_chunks())
        break;
      if (index >= ids->length()) {
        array_index++;
        if (array_index == column->num_chunks())
          break;
        ids = std::static_pointer_cast<arrow::Int64Array>(
            column->chunk(array_index));
        index = 0;
      }
      if (ids->IsNull(index) || !ids->IsValid(index)) {
        index++;
        global_index++;
        continue;
      }
      int64_t x = ids->Value(index);
      if (x <= i) {
        index++;
        global_index++;
      } else {
        break;
      }
    }
    RETURN_NOT_ARROW_OK(builder.Append(global_index));
  }

  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto array, builder.Finish());
  arrays.push_back(array);
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  return arrow::Table::Make(schema, arrays);
}

Result<std::shared_ptr<arrow::Table>> EdgeChunkWriter::sortTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::string& column_name) {
  auto exec_context = arrow::compute::default_exec_context();
  auto plan = arrow::compute::ExecPlan::Make(exec_context).ValueOrDie();
  int max_batch_size = 2;
  auto table_source_options =
      arrow::compute::TableSourceNodeOptions{input_table, max_batch_size};
  auto source = arrow::compute::MakeExecNode("table_source", plan.get(), {},
                                             table_source_options)
                    .ValueOrDie();
  AsyncGeneratorType sink_gen;
  if (!arrow::compute::MakeExecNode(
           "order_by_sink", plan.get(), {source},
           arrow::compute::OrderBySinkNodeOptions{
               arrow::compute::SortOptions{{arrow::compute::SortKey{
                   column_name, arrow::compute::SortOrder::Ascending}}},
               &sink_gen})
           .ok()) {
    return Status::InvalidOperation();
  }
  return ExecutePlanAndCollectAsTable(*exec_context, plan,
                                      input_table->schema(), sink_gen);
}

}  // namespace GAR_NAMESPACE_INTERNAL

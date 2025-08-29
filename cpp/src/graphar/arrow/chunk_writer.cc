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

#include <arrow/acero/api.h>
#include <cstddef>
#include <iostream>
#include <unordered_map>
#include <utility>
#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "graphar/fwd.h"
#include "graphar/writer_util.h"
#if defined(ARROW_VERSION) && ARROW_VERSION >= 12000000
#include "arrow/acero/exec_plan.h"
#else
#include "arrow/compute/exec/exec_plan.h"
#endif
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"

#include "graphar/arrow/chunk_writer.h"
#include "graphar/filesystem.h"
#include "graphar/general_params.h"
#include "graphar/graph_info.h"
#include "graphar/result.h"
#include "graphar/status.h"
#include "graphar/types.h"
#include "graphar/util.h"

namespace graphar {
// common methods

#if defined(ARROW_VERSION) && ARROW_VERSION >= 12000000
namespace arrow_acero_namespace = arrow::acero;
#else
namespace arrow_acero_namespace = arrow::compute;
#endif

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
    std::shared_ptr<arrow_acero_namespace::ExecPlan> plan,
    std::shared_ptr<arrow::Schema> schema, AsyncGeneratorType sink_gen) {
  // translate sink_gen (async) to sink_reader (sync)
  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      arrow_acero_namespace::MakeGeneratorReader(schema, std::move(sink_gen),
                                                 exec_context.memory_pool());

  // validate the ExecPlan
  RETURN_NOT_ARROW_OK(plan->Validate());
  //  start the ExecPlan
#if defined(ARROW_VERSION) && ARROW_VERSION >= 12000000
  plan->StartProducing();  // arrow 12.0.0 or later return void, not Status
#else
  RETURN_NOT_ARROW_OK(plan->StartProducing());
#endif

  // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      response_table, arrow::Table::FromRecordBatchReader(sink_reader.get()));

  // stop producing
  plan->StopProducing();
  // plan mark finished
  RETURN_NOT_ARROW_OK(plan->finished().status());
  return response_table;
}

// implementations for VertexPropertyChunkWriter

VertexPropertyWriter::VertexPropertyWriter(
    const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
    const std::shared_ptr<WriterOptions>& options,
    const ValidateLevel& validate_level)
    : vertex_info_(vertex_info),
      prefix_(prefix),
      validate_level_(validate_level),
      options_(options) {
  if (!options) {
    options_ = WriterOptions::DefaultWriterOption();
  }
  if (validate_level_ == ValidateLevel::default_validate) {
    throw std::runtime_error(
        "default_validate is not allowed to be set as the global validate "
        "level for VertexPropertyWriter");
  }
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
}

// Check if the operation of writing vertices number is allowed.
Status VertexPropertyWriter::validate(const IdType& count,
                                      ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // weak & strong validate
  if (count < 0) {
    return Status::Invalid("The number of vertices is negative.");
  }
  return Status::OK();
}

// Check if the operation of copying a file as a chunk is allowed.
Status VertexPropertyWriter::validate(
    const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // weak & strong validate
  if (!vertex_info_->HasPropertyGroup(property_group)) {
    return Status::KeyError("The property group", " does not exist in ",
                            vertex_info_->GetType(), " vertex info.");
  }
  if (chunk_index < 0) {
    return Status::IndexError("Negative chunk index ", chunk_index, ".");
  }
  return Status::OK();
}

// Check if the operation of writing a table as a chunk is allowed.
Status VertexPropertyWriter::validate(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate) {
    validate_level = validate_level_;
  }
  // no validate
  if (validate_level == ValidateLevel::no_validate) {
    return Status::OK();
  }
  // validate property_group & chunk_index
  GAR_RETURN_NOT_OK(validate(property_group, chunk_index, validate_level));
  // weak validate for the input_table
  if (input_table->num_rows() > vertex_info_->GetChunkSize()) {
    return Status::Invalid("The number of rows of input table is ",
                           input_table->num_rows(),
                           " which is larger than the vertex chunk size",
                           vertex_info_->GetChunkSize(), ".");
  }
  // strong validate for the input_table
  if (validate_level == ValidateLevel::strong_validate) {
    // validate the input table
    RETURN_NOT_ARROW_OK(input_table->Validate());
    // validate the schema
    auto schema = input_table->schema();
    for (auto& property : property_group->GetProperties()) {
      int indice = schema->GetFieldIndex(property.name);
      if (indice == -1) {
        return Status::Invalid("Column named ", property.name,
                               " of property group ", property_group,
                               " does not exist in the input table.");
      }
      auto field = schema->field(indice);
      auto schema_data_type = DataType::DataTypeToArrowDataType(property.type);
      if (property.cardinality != Cardinality::SINGLE) {
        schema_data_type = arrow::list(schema_data_type);
      }
      if (!DataType::ArrowDataTypeToDataType(field->type())
               ->Equals(DataType::ArrowDataTypeToDataType(schema_data_type))) {
        return Status::TypeError(
            "The data type of property: ", property.name, " is ",
            DataType::ArrowDataTypeToDataType(schema_data_type)->ToTypeName(),
            ", but got ",
            DataType::ArrowDataTypeToDataType(field->type())->ToTypeName(),
            ".");
      }
    }
  }
  return Status::OK();
}

Status VertexPropertyWriter::WriteVerticesNum(
    const IdType& count, ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(count, validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix, vertex_info_->GetVerticesNumFilePath());
  std::string path = prefix_ + suffix;
  return fs_->WriteValueToFile<IdType>(count, path);
}

Status VertexPropertyWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group, IdType chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(
      validate(input_table, property_group, chunk_index, validate_level));
  auto file_type = property_group->GetFileType();
  auto schema = input_table->schema();
  int indice = schema->GetFieldIndex(GeneralParams::kVertexIndexCol);
  if (indice == -1) {
    return Status::Invalid("The internal id Column named ",
                           GeneralParams::kVertexIndexCol,
                           " does not exist in the input table.");
  }

  std::vector<int> indices({indice});
  for (auto& property : property_group->GetProperties()) {
    int indice = schema->GetFieldIndex(property.name);
    if (indice == -1) {
      return Status::Invalid("Column named ", property.name,
                             " of property group ", property_group,
                             " of vertex ", vertex_info_->GetType(),
                             " does not exist in the input table.");
    }
    indices.push_back(indice);
  }
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto in_table,
                                       input_table->SelectColumns(indices));
  GAR_ASSIGN_OR_RAISE(auto suffix,
                      vertex_info_->GetFilePath(property_group, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path, options_);
}

Status VertexPropertyWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType chunk_index,
    ValidateLevel validate_level) const {
  auto property_groups = vertex_info_->GetPropertyGroups();
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(
        WriteChunk(input_table, property_group, chunk_index, validate_level));
  }
  return Status::OK();
}

Status VertexPropertyWriter::WriteLabelChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType chunk_index,
    FileType file_type, ValidateLevel validate_level) const {
  auto schema = input_table->schema();
  std::vector<int> indices;
  for (int i = 0; i < schema->num_fields(); i++) {
    indices.push_back(i);
  }

  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(auto in_table,
                                       input_table->SelectColumns(indices));
  std::string suffix =
      vertex_info_->GetPrefix() + "labels/chunk" + std::to_string(chunk_index);
  std::string path = prefix_ + suffix;
  return fs_->WriteLabelTableToFile(input_table, path);
}

Status VertexPropertyWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  auto schema = input_table->schema();
  int indice = schema->GetFieldIndex(GeneralParams::kVertexIndexCol);
  auto table_with_index = input_table;
  if (indice == -1) {
    // add index column
    GAR_ASSIGN_OR_RAISE(table_with_index,
                        AddIndexColumn(input_table, start_chunk_index,
                                       vertex_info_->GetChunkSize()));
  }
  IdType chunk_size = vertex_info_->GetChunkSize();
  int64_t length = table_with_index->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size, chunk_index++) {
    auto in_chunk = table_with_index->Slice(offset, chunk_size);
    GAR_RETURN_NOT_OK(
        WriteChunk(in_chunk, property_group, chunk_index, validate_level));
  }
  return Status::OK();
}

Status VertexPropertyWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType start_chunk_index,
    ValidateLevel validate_level) const {
  auto property_groups = vertex_info_->GetPropertyGroups();
  GAR_ASSIGN_OR_RAISE(auto table_with_index,
                      AddIndexColumn(input_table, start_chunk_index,
                                     vertex_info_->GetChunkSize()));
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(WriteTable(table_with_index, property_group,
                                 start_chunk_index, validate_level));
  }
  auto labels = vertex_info_->GetLabels();
  if (!labels.empty()) {
    GAR_ASSIGN_OR_RAISE(auto label_table, GetLabelTable(input_table, labels))
    GAR_RETURN_NOT_OK(WriteLabelTable(label_table, start_chunk_index,
                                      FileType::PARQUET, validate_level));
  }

  return Status::OK();
}

Status VertexPropertyWriter::WriteLabelTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType start_chunk_index,
    FileType file_type, ValidateLevel validate_level) const {
  auto schema = input_table->schema();
  int indice = schema->GetFieldIndex(GeneralParams::kVertexIndexCol);
  IdType chunk_size = vertex_info_->GetChunkSize();
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size);
    GAR_RETURN_NOT_OK(
        WriteLabelChunk(in_chunk, chunk_index, file_type, validate_level));
  }
  return Status::OK();
}

Result<std::shared_ptr<arrow::Table>> VertexPropertyWriter::GetLabelTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::vector<std::string>& labels) const {
  // Find the label column index
  auto label_col_idx =
      input_table->schema()->GetFieldIndex(GeneralParams::kLabelCol);
  if (label_col_idx == -1) {
    return Status::KeyError("label column not found in the input table.");
  }

  // Create a matrix of booleans with dimensions [number of rows, number of
  // labels]
  std::vector<std::vector<bool>> bool_matrix(
      input_table->num_rows(), std::vector<bool>(labels.size(), false));

  // Create a map for labels to column indices
  std::unordered_map<std::string, int> label_to_index;
  for (size_t i = 0; i < labels.size(); ++i) {
    label_to_index[labels[i]] = i;
  }

  int row_offset = 0;  // Offset for where to fill the bool_matrix
  // Iterate through each chunk of the :LABEL column
  for (int64_t chunk_idx = 0;
       chunk_idx < input_table->column(label_col_idx)->num_chunks();
       ++chunk_idx) {
    auto chunk = input_table->column(label_col_idx)->chunk(chunk_idx);
    auto label_column = std::static_pointer_cast<arrow::StringArray>(chunk);

    // Populate the matrix based on :LABEL column values
    // TODO(@yangxk):  store array in the label_column, split the string when
    // reading file
    for (int64_t row = 0; row < label_column->length(); ++row) {
      if (label_column->IsValid(row)) {
        std::string labels_string = label_column->GetString(row);
        auto row_labels = SplitString(labels_string, ';');
        for (const auto& lbl : row_labels) {
          if (label_to_index.find(lbl) != label_to_index.end()) {
            bool_matrix[row_offset + row][label_to_index[lbl]] = true;
          }
        }
      }
    }

    row_offset +=
        label_column->length();  // Update the row offset for the next chunk
  }

  // Create Arrow arrays for each label column
  arrow::FieldVector fields;
  arrow::ArrayVector arrays;

  for (const auto& label : labels) {
    arrow::BooleanBuilder builder;
    for (const auto& row : bool_matrix) {
      RETURN_NOT_ARROW_OK(builder.Append(row[label_to_index[label]]));
    }

    std::shared_ptr<arrow::Array> array;
    RETURN_NOT_ARROW_OK(builder.Finish(&array));
    fields.push_back(arrow::field(label, arrow::boolean()));
    arrays.push_back(array);
  }

  // Create the Arrow Table with the boolean columns
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto result_table = arrow::Table::Make(schema, arrays);

  return result_table;
}

Result<std::shared_ptr<VertexPropertyWriter>> VertexPropertyWriter::Make(
    const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
    const std::shared_ptr<WriterOptions>& options,
    const ValidateLevel& validate_level) {
  return std::make_shared<VertexPropertyWriter>(vertex_info, prefix, options,
                                                validate_level);
}

Result<std::shared_ptr<VertexPropertyWriter>> VertexPropertyWriter::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const std::shared_ptr<WriterOptions>& options,
    const ValidateLevel& validate_level) {
  auto vertex_info = graph_info->GetVertexInfo(type);
  if (!vertex_info) {
    return Status::KeyError("The vertex ", type, " doesn't exist.");
  }
  return Make(vertex_info, graph_info->GetPrefix(), options, validate_level);
}

Result<std::shared_ptr<VertexPropertyWriter>> VertexPropertyWriter::Make(
    const std::shared_ptr<VertexInfo>& vertex_info, const std::string& prefix,
    const ValidateLevel& validate_level) {
  return Make(vertex_info, prefix, WriterOptions::DefaultWriterOption(),
              validate_level);
}

Result<std::shared_ptr<VertexPropertyWriter>> VertexPropertyWriter::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& type,
    const ValidateLevel& validate_level) {
  return Make(graph_info, type, WriterOptions::DefaultWriterOption(),
              validate_level);
}

Result<std::shared_ptr<arrow::Table>> VertexPropertyWriter::AddIndexColumn(
    const std::shared_ptr<arrow::Table>& table, IdType chunk_index,
    IdType chunk_size) const {
  arrow::Int64Builder array_builder;
  RETURN_NOT_ARROW_OK(array_builder.Reserve(chunk_size));
  int64_t length = table->num_rows();
  for (IdType i = 0; i < length; i++) {
    RETURN_NOT_ARROW_OK(array_builder.Append(chunk_index * chunk_size + i));
  }
  std::shared_ptr<arrow::Array> array;
  RETURN_NOT_ARROW_OK(array_builder.Finish(&array));
  std::shared_ptr<arrow::ChunkedArray> chunked_array =
      std::make_shared<arrow::ChunkedArray>(array);
  GAR_RETURN_ON_ARROW_ERROR_AND_ASSIGN(
      auto ret, table->AddColumn(0,
                                 arrow::field(GeneralParams::kVertexIndexCol,
                                              arrow::int64(), false),
                                 chunked_array));
  return ret;
}

// implementations for EdgeChunkWriter

EdgeChunkWriter::EdgeChunkWriter(const std::shared_ptr<EdgeInfo>& edge_info,
                                 const std::string& prefix,
                                 AdjListType adj_list_type,
                                 const std::shared_ptr<WriterOptions>& options,
                                 const ValidateLevel& validate_level)
    : edge_info_(edge_info),
      adj_list_type_(adj_list_type),
      validate_level_(validate_level),
      options_(options) {
  if (!options) {
    options_ = WriterOptions::DefaultWriterOption();
  }
  if (validate_level_ == ValidateLevel::default_validate) {
    throw std::runtime_error(
        "default_validate is not allowed to be set as the global validate "
        "level for EdgeChunkWriter");
  }
  GAR_ASSIGN_OR_RAISE_ERROR(fs_, FileSystemFromUriOrPath(prefix, &prefix_));
  chunk_size_ = edge_info_->GetChunkSize();
  switch (adj_list_type) {
  case AdjListType::unordered_by_source:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    break;
  case AdjListType::ordered_by_source:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
    break;
  case AdjListType::unordered_by_dest:
    vertex_chunk_size_ = edge_info_->GetDstChunkSize();
    break;
  case AdjListType::ordered_by_dest:
    vertex_chunk_size_ = edge_info_->GetDstChunkSize();
    break;
  default:
    vertex_chunk_size_ = edge_info_->GetSrcChunkSize();
  }
}
// Check if the operation of writing number or copying a file is allowed.
Status EdgeChunkWriter::validate(IdType count_or_index1, IdType count_or_index2,
                                 ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // weak & strong validate for adj list type
  if (!edge_info_->HasAdjacentListType(adj_list_type_)) {
    return Status::KeyError(
        "Adj list type ", AdjListTypeToString(adj_list_type_),
        " does not exist in the ", edge_info_->GetEdgeType(), " edge info.");
  }
  // weak & strong validate for count or index
  if (count_or_index1 < 0 || count_or_index2 < 0) {
    return Status::IndexError(
        "The count or index must be non-negative, but got ", count_or_index1,
        " and ", count_or_index2, ".");
  }
  return Status::OK();
}

// Check if the operation of copying a file as a property chunk is allowed.
Status EdgeChunkWriter::validate(
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // validate for adj list type & index
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, chunk_index, validate_level));
  // weak & strong validate for property group
  if (!edge_info_->HasPropertyGroup(property_group)) {
    return Status::KeyError("Property group", " does not exist in the ",
                            edge_info_->GetEdgeType(), " edge info.");
  }
  return Status::OK();
}

// Check if the operation of writing a table as an offset chunk is allowed.
Status EdgeChunkWriter::validate(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // validate for adj list type & index
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, 0, validate_level));
  // weak validate for the input table
  if (adj_list_type_ != AdjListType::ordered_by_source &&
      adj_list_type_ != AdjListType::ordered_by_dest) {
    return Status::Invalid(
        "The adj list type has to be ordered_by_source or ordered_by_dest, but "
        "got " +
        std::string(AdjListTypeToString(adj_list_type_)));
  }
  if (adj_list_type_ == AdjListType::ordered_by_source &&
      input_table->num_rows() > edge_info_->GetSrcChunkSize() + 1) {
    return Status::Invalid(
        "The number of rows of input offset table is ", input_table->num_rows(),
        " which is larger than the offset size of source vertex chunk ",
        edge_info_->GetSrcChunkSize() + 1, ".");
  }
  if (adj_list_type_ == AdjListType::ordered_by_dest &&
      input_table->num_rows() > edge_info_->GetDstChunkSize() + 1) {
    return Status::Invalid(
        "The number of rows of input offset table is ", input_table->num_rows(),
        " which is larger than the offset size of destination vertex chunk ",
        edge_info_->GetSrcChunkSize() + 1, ".");
  }
  // strong validate for the input_table
  if (validate_level == ValidateLevel::strong_validate) {
    // validate the input table
    RETURN_NOT_ARROW_OK(input_table->Validate());
    // validate the schema
    auto schema = input_table->schema();
    int index = schema->GetFieldIndex(GeneralParams::kOffsetCol);
    if (index == -1) {
      return Status::Invalid("The offset column ", GeneralParams::kOffsetCol,
                             " does not exist in the input table");
    }
    auto field = schema->field(index);
    if (field->type()->id() != arrow::Type::INT64) {
      return Status::TypeError(
          "The data type for offset column should be INT64, but got ",
          field->type()->name());
    }
  }
  return Status::OK();
}

// Check if the operation of writing a table as an adj list chunk is allowed.
Status EdgeChunkWriter::validate(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index, ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // validate for adj list type & index
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, chunk_index, validate_level));
  // weak validate for the input table
  if (input_table->num_rows() > edge_info_->GetChunkSize()) {
    return Status::Invalid(
        "The number of rows of input table is ", input_table->num_rows(),
        " which is larger than the ", edge_info_->GetEdgeType(),
        " edge chunk size ", edge_info_->GetChunkSize(), ".");
  }
  // strong validate for the input table
  if (validate_level == ValidateLevel::strong_validate) {
    auto schema = input_table->schema();
    int index = schema->GetFieldIndex(GeneralParams::kSrcIndexCol);
    if (index == -1) {
      return Status::Invalid("The source index column ",
                             GeneralParams::kSrcIndexCol,
                             " does not exist in the input table");
    }
    auto field = schema->field(index);
    if (field->type()->id() != arrow::Type::INT64) {
      return Status::TypeError(
          "The data type for source index column should be INT64, but got ",
          field->type()->name());
    }
    index = schema->GetFieldIndex(GeneralParams::kDstIndexCol);
    if (index == -1) {
      return Status::Invalid("The destination index column ",
                             GeneralParams::kDstIndexCol,
                             " does not exist in the input table");
    }
    field = schema->field(index);
    if (field->type()->id() != arrow::Type::INT64) {
      return Status::TypeError(
          "The data type for destination index column should be INT64, but "
          "got ",
          field->type()->name());
    }
  }
  return Status::OK();
}

// Check if the operation of writing a table as a property chunk is allowed.
Status EdgeChunkWriter::validate(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  // use the writer's validate level
  if (validate_level == ValidateLevel::default_validate)
    validate_level = validate_level_;
  // no validate
  if (validate_level == ValidateLevel::no_validate)
    return Status::OK();
  // validate for property group, adj list type & index
  GAR_RETURN_NOT_OK(validate(property_group, vertex_chunk_index, chunk_index,
                             validate_level));
  // weak validate for the input table
  if (input_table->num_rows() > edge_info_->GetChunkSize()) {
    return Status::Invalid(
        "The number of rows of input table is ", input_table->num_rows(),
        " which is larger than the ", edge_info_->GetEdgeType(),
        " edge chunk size ", edge_info_->GetChunkSize(), ".");
  }
  // strong validate for the input table
  if (validate_level == ValidateLevel::strong_validate) {
    // validate the input table
    RETURN_NOT_ARROW_OK(input_table->Validate());
    // validate the schema
    auto schema = input_table->schema();
    for (auto& property : property_group->GetProperties()) {
      int indice = schema->GetFieldIndex(property.name);
      if (indice == -1) {
        return Status::Invalid("Column named ", property.name,
                               " of property group ", property_group,
                               " does not exist in the input table.");
      }
      auto field = schema->field(indice);
      if (DataType::ArrowDataTypeToDataType(field->type()) != property.type) {
        return Status::TypeError(
            "The data type of property: ", property.name, " is ",
            property.type->ToTypeName(), ", but got ",
            DataType::ArrowDataTypeToDataType(field->type())->ToTypeName(),
            ".");
      }
    }
  }
  return Status::OK();
}

Status EdgeChunkWriter::WriteEdgesNum(IdType vertex_chunk_index,
                                      const IdType& count,
                                      ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(vertex_chunk_index, count, validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_->GetEdgesNumFilePath(
                                       vertex_chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteValueToFile<IdType>(count, path);
}

Status EdgeChunkWriter::WriteVerticesNum(const IdType& count,
                                         ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(0, count, validate_level));
  GAR_ASSIGN_OR_RAISE(auto suffix,
                      edge_info_->GetVerticesNumFilePath(adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteValueToFile<IdType>(count, path);
}

Status EdgeChunkWriter::WriteOffsetChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(input_table, vertex_chunk_index, validate_level));
  auto file_type = edge_info_->GetAdjacentList(adj_list_type_)->GetFileType();
  auto schema = input_table->schema();
  int index = schema->GetFieldIndex(GeneralParams::kOffsetCol);
  if (index == -1) {
    return Status::Invalid("The offset column ", GeneralParams::kOffsetCol,
                           " does not exist in the input table");
  }
  auto in_table = input_table->SelectColumns({index}).ValueOrDie();
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_->GetAdjListOffsetFilePath(
                                       vertex_chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path, options_);
}

Status EdgeChunkWriter::WriteAdjListChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index, ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(
      validate(input_table, vertex_chunk_index, chunk_index, validate_level));
  auto file_type = edge_info_->GetAdjacentList(adj_list_type_)->GetFileType();
  std::vector<int> indices;
  indices.clear();
  auto schema = input_table->schema();
  int index = schema->GetFieldIndex(GeneralParams::kSrcIndexCol);
  if (index == -1) {
    return Status::Invalid("The source index column ",
                           GeneralParams::kSrcIndexCol,
                           " does not exist in the input table");
  }
  indices.push_back(index);
  index = schema->GetFieldIndex(GeneralParams::kDstIndexCol);
  if (index == -1) {
    return Status::Invalid("The destination index column ",
                           GeneralParams::kDstIndexCol,
                           " does not exist in the input table");
  }
  indices.push_back(index);
  auto in_table = input_table->SelectColumns(indices).ValueOrDie();

  GAR_ASSIGN_OR_RAISE(
      auto suffix, edge_info_->GetAdjListFilePath(vertex_chunk_index,
                                                  chunk_index, adj_list_type_));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path, options_);
}

Status EdgeChunkWriter::WritePropertyChunk(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType chunk_index,
    ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(validate(input_table, property_group, vertex_chunk_index,
                             chunk_index, validate_level));
  auto file_type = property_group->GetFileType();

  std::vector<int> indices;
  indices.clear();
  auto schema = input_table->schema();
  for (auto& property : property_group->GetProperties()) {
    int indice = schema->GetFieldIndex(property.name);
    if (indice == -1) {
      return Status::Invalid("Column named ", property.name,
                             " of property group ", property_group, " of edge ",
                             edge_info_->GetEdgeType(),
                             " does not exist in the input table.");
    }
    indices.push_back(indice);
  }
  auto in_table = input_table->SelectColumns(indices).ValueOrDie();
  GAR_ASSIGN_OR_RAISE(auto suffix, edge_info_->GetPropertyFilePath(
                                       property_group, adj_list_type_,
                                       vertex_chunk_index, chunk_index));
  std::string path = prefix_ + suffix;
  return fs_->WriteTableToFile(in_table, file_type, path, options_);
}

Status EdgeChunkWriter::WritePropertyChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index, ValidateLevel validate_level) const {
  const auto& property_groups = edge_info_->GetPropertyGroups();
  for (auto& property_group : property_groups) {
    GAR_RETURN_NOT_OK(WritePropertyChunk(input_table, property_group,
                                         vertex_chunk_index, chunk_index,
                                         validate_level));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WriteChunk(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType chunk_index, ValidateLevel validate_level) const {
  GAR_RETURN_NOT_OK(WriteAdjListChunk(input_table, vertex_chunk_index,
                                      chunk_index, validate_level));
  return WritePropertyChunk(input_table, vertex_chunk_index, chunk_index,
                            validate_level);
}

Status EdgeChunkWriter::WriteAdjListTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(WriteAdjListChunk(in_chunk, vertex_chunk_index,
                                        chunk_index, validate_level));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType start_chunk_index,
    ValidateLevel validate_level) const {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(WritePropertyChunk(in_chunk, property_group,
                                         vertex_chunk_index, chunk_index,
                                         validate_level));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(WritePropertyChunk(in_chunk, vertex_chunk_index,
                                         chunk_index, validate_level));
  }
  return Status::OK();
}

Status EdgeChunkWriter::WriteTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  int64_t length = input_table->num_rows();
  IdType chunk_index = start_chunk_index;
  for (int64_t offset = 0; offset < length;
       offset += chunk_size_, chunk_index++) {
    auto in_chunk = input_table->Slice(offset, chunk_size_);
    GAR_RETURN_NOT_OK(
        WriteChunk(in_chunk, vertex_chunk_index, chunk_index, validate_level));
  }
  return Status::OK();
}

Status EdgeChunkWriter::SortAndWriteAdjListTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));
  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::ordered_by_dest) {
    GAR_ASSIGN_OR_RAISE(
        auto offset_table,
        getOffsetTable(response_table, getSortColumnName(adj_list_type_),
                       vertex_chunk_index));
    GAR_RETURN_NOT_OK(
        WriteOffsetChunk(offset_table, vertex_chunk_index, validate_level));
  }
  return WriteAdjListTable(response_table, vertex_chunk_index,
                           start_chunk_index, validate_level);
}

Status EdgeChunkWriter::SortAndWritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::shared_ptr<PropertyGroup>& property_group,
    IdType vertex_chunk_index, IdType start_chunk_index,
    ValidateLevel validate_level) const {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));
  return WritePropertyTable(response_table, property_group, vertex_chunk_index,
                            start_chunk_index, validate_level);
}

Status EdgeChunkWriter::SortAndWritePropertyTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));
  return WritePropertyTable(response_table, vertex_chunk_index,
                            start_chunk_index, validate_level);
}

Status EdgeChunkWriter::SortAndWriteTable(
    const std::shared_ptr<arrow::Table>& input_table, IdType vertex_chunk_index,
    IdType start_chunk_index, ValidateLevel validate_level) const {
  GAR_ASSIGN_OR_RAISE(
      auto response_table,
      sortTable(input_table, getSortColumnName(adj_list_type_)));

  if (adj_list_type_ == AdjListType::ordered_by_source ||
      adj_list_type_ == AdjListType::ordered_by_dest) {
    GAR_ASSIGN_OR_RAISE(
        auto offset_table,
        getOffsetTable(response_table, getSortColumnName(adj_list_type_),
                       vertex_chunk_index));
    GAR_RETURN_NOT_OK(
        WriteOffsetChunk(offset_table, vertex_chunk_index, validate_level));
  }

  return WriteTable(response_table, vertex_chunk_index, start_chunk_index,
                    validate_level);
}

Result<std::shared_ptr<arrow::Table>> EdgeChunkWriter::getOffsetTable(
    const std::shared_ptr<arrow::Table>& input_table,
    const std::string& column_name, IdType vertex_chunk_index) const {
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
  schema_vector.push_back(
      arrow::field(property, DataType::DataTypeToArrowDataType(int64())));

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
#if ARROW_VERSION >= 21000000
  RETURN_NOT_ARROW_OK(arrow::compute::Initialize());
#endif
  auto exec_context = arrow::compute::default_exec_context();
  auto plan = arrow_acero_namespace::ExecPlan::Make(exec_context).ValueOrDie();
  auto table_source_options =
      arrow_acero_namespace::TableSourceNodeOptions{input_table};
  auto source = arrow_acero_namespace::MakeExecNode("table_source", plan.get(),
                                                    {}, table_source_options)
                    .ValueOrDie();
  AsyncGeneratorType sink_gen;
  RETURN_NOT_ARROW_OK(
      arrow_acero_namespace::MakeExecNode(
          "order_by_sink", plan.get(), {source},
          arrow_acero_namespace::OrderBySinkNodeOptions{
              arrow::compute::SortOptions{{arrow::compute::SortKey{
                  column_name, arrow::compute::SortOrder::Ascending}}},
              &sink_gen})
          .status());
  return ExecutePlanAndCollectAsTable(*exec_context, plan,
                                      input_table->schema(), sink_gen);
}

Result<std::shared_ptr<EdgeChunkWriter>> EdgeChunkWriter::Make(
    const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
    AdjListType adj_list_type, const std::shared_ptr<WriterOptions>& options,
    const ValidateLevel& validate_level) {
  if (!edge_info->HasAdjacentListType(adj_list_type)) {
    return Status::KeyError(
        "The adjacent list type ", AdjListTypeToString(adj_list_type),
        " doesn't exist in edge ", edge_info->GetEdgeType(), ".");
  }
  return std::make_shared<EdgeChunkWriter>(edge_info, prefix, adj_list_type,
                                           options, validate_level);
}

Result<std::shared_ptr<EdgeChunkWriter>> EdgeChunkWriter::Make(
    const std::shared_ptr<EdgeInfo>& edge_info, const std::string& prefix,
    AdjListType adj_list_type, const ValidateLevel& validate_level) {
  return Make(edge_info, prefix, adj_list_type,
              WriterOptions::DefaultWriterOption(), validate_level);
}

Result<std::shared_ptr<EdgeChunkWriter>> EdgeChunkWriter::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    AdjListType adj_list_type, const std::shared_ptr<WriterOptions>& options,
    const ValidateLevel& validate_level) {
  auto edge_info = graph_info->GetEdgeInfo(src_type, edge_type, dst_type);
  if (!edge_info) {
    return Status::KeyError("The edge ", src_type, " ", edge_type, " ",
                            dst_type, " doesn't exist.");
  }
  return Make(edge_info, graph_info->GetPrefix(), adj_list_type, options,
              validate_level);
}

Result<std::shared_ptr<EdgeChunkWriter>> EdgeChunkWriter::Make(
    const std::shared_ptr<GraphInfo>& graph_info, const std::string& src_type,
    const std::string& edge_type, const std::string& dst_type,
    AdjListType adj_list_type, const ValidateLevel& validate_level) {
  return Make(graph_info, src_type, edge_type, dst_type, adj_list_type,
              WriterOptions::DefaultWriterOption(), validate_level);
}

std::string EdgeChunkWriter::getSortColumnName(AdjListType adj_list_type) {
  switch (adj_list_type) {
  case AdjListType::unordered_by_source:
    return GeneralParams::kSrcIndexCol;
  case AdjListType::ordered_by_source:
    return GeneralParams::kSrcIndexCol;
  case AdjListType::unordered_by_dest:
    return GeneralParams::kDstIndexCol;
  case AdjListType::ordered_by_dest:
    return GeneralParams::kDstIndexCol;
  default:
    return GeneralParams::kSrcIndexCol;
  }
  return GeneralParams::kSrcIndexCol;
}
}  // namespace graphar

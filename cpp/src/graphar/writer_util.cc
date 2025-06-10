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

#include "graphar/writer_util.h"
namespace graphar {
std::shared_ptr<WriterOptions::CSVBuilder>
WriterOptions::Builder::getCsvOptionBuilder() {
  if (!csvBuilder) {
    csvBuilder = std::make_shared<CSVBuilder>();
  }
  return csvBuilder;
}
std::shared_ptr<WriterOptions::ParquetBuilder>
WriterOptions::Builder::getParquetOptionBuilder() {
  if (!parquetBuilder) {
    parquetBuilder = std::make_shared<ParquetBuilder>();
  }
  return parquetBuilder;
}
#ifdef ARROW_ORC
std::shared_ptr<WriterOptions::OrcBuilder>
WriterOptions::Builder::getOrcOptionBuilder() {
  if (!orcBuilder) {
    orcBuilder = std::make_shared<OrcBuilder>();
  }
  return orcBuilder;
}
#endif
std::shared_ptr<WriterOptions> WriterOptions::Builder::Build() {
  return std::make_shared<WriterOptions>(this);
}

WriterOptions::WriterOptions(WriterOptions::Builder* builder) {
  if (builder->csvBuilder) {
    auto csvBuilder = builder->csvBuilder;
    this->csvOption = csvBuilder->option;
  }
  if (builder->parquetBuilder) {
    auto parquetBuilder = builder->parquetBuilder;
    this->parquetOption = parquetBuilder->option;
  }
#ifdef ARROW_ORC
  if (builder->orcBuilder) {
    auto orcBuilder = builder->orcBuilder;
    this->orcOption = orcBuilder->option;
  }
#endif
}
}  // namespace graphar

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

#include <google/protobuf/util/json_util.h>
#include <iostream>
#include "vertex_info.pb.h"

using google::protobuf::util::JsonStringToMessage;

bool proto_to_json(const google::protobuf::Message& message, std::string& json) {
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    options.preserve_proto_field_names = true;
    return MessageToJsonString(message, &json, options).ok();
}

bool json_to_proto(const std::string& json, google::protobuf::Message& message) {
    return JsonStringToMessage(json, &message).ok();
}

int main() {
    graphar::VertexInfo vertex;
    vertex.set_type("person");
    vertex.set_chunk_size(100);
    vertex.set_prefix("./person");

    std::string json_string;
   /* protobuf to json。 */
    if (!proto_to_json(vertex, json_string)) {
        std::cout << "protobuf convert json failed!" << std::endl;
        return 1;
    }
    std::cout << "protobuf convert json done!" << std::endl
              << json_string << std::endl;

    vertex.Clear();
    std::cout << "-----" << std::endl;

    return 0;
}
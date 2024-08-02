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

package org.apache.graphar.cli.util

case class GraphAr(
    path: String,
    name: String,
    vertexChunkSize: Int,
    edgeChunkSize: Int,
    fileType: String
)

case class Neo4j(url: String, username: String, password: String)

case class Property(
    name: String,
    `type`: String,
    nullable: Option[Boolean] = None
)

case class Source(
    `type`: String,
    path: Option[String] = None,
    url: Option[String] = None,
    delimiter: String = ","
)

case class Vertex(
    label: String,
    primary: String,
    properties: List[Property],
    propertyGroups: Option[List[List[String]]] = None,
    source: Source
)

case class Edge(
    label: String,
    adjListType: String = "ordered_by_source",
    srcLabel: String,
    srcProp: String,
    dstLabel: String,
    dstProp: String,
    properties: List[Property],
    propertyGroups: Option[List[List[String]]] = None,
    source: Source
)

case class Schema(
    vertices: List[Vertex],
    edges: List[Edge]
)

case class Config(
    graphar: GraphAr,
    sourceType: String,
    schema: Schema,
    neo4j: Option[Neo4j]
)

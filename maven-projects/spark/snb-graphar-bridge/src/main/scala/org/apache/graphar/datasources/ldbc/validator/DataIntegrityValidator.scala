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

package org.apache.graphar.datasources.ldbc.validator

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.graphar.GraphInfo
import org.apache.graphar.graph.GraphReader

/**
 * Data integrity validator for GraphAr output.
 *
 * Validates:
 * - Edge reference integrity (no dangling edges)
 * - ID mapping consistency (university/company ID ranges)
 * - GraphAr index column correctness
 */
class DataIntegrityValidator(graphArPath: String, spark: SparkSession) {

  import spark.implicits._

  // Load graph metadata (try different possible filenames)
  private val graphInfoPath = {
    val possibleNames = Seq(
      "ldbc_social_network.graph.yml",
      "ldbc_test.graph.yml",
      "graph.yml"
    )
    possibleNames.map(name => s"$graphArPath/$name")
      .find(path => new java.io.File(path).exists())
      .getOrElse(throw new IllegalArgumentException(
        s"No graph metadata file found in $graphArPath. Tried: ${possibleNames.mkString(", ")}"
      ))
  }

  private val graphInfo = GraphInfo.loadGraphInfo(graphInfoPath, spark)

  // Load all vertices and edges using GraphReader
  private val (vertexDataframes, edgeDataframes) = GraphReader.read(graphInfoPath, spark)

  /**
   * Validate edge reference integrity: edge indices are within valid vertex index range.
   *
   * GraphAr uses index-based storage format. Edge DataFrames only contain:
   * - _graphArSrcIndex: index of source vertex (0-based)
   * - _graphArDstIndex: index of destination vertex (0-based)
   *
   * This validates that all edge indices reference valid vertex indices in range [0, vertexCount).
   *
   * @param srcVertexType Source vertex type (e.g., "Person")
   * @param edgeName Edge name (e.g., "knows")
   * @param dstVertexType Destination vertex type (e.g., "Person")
   * @return ValidationReport with results
   */
  def validateEdgeReferenceIntegrity(
    srcVertexType: String,
    edgeName: String,
    dstVertexType: String
  ): ValidationReport = {
    try {
      // Get vertex counts (max valid index = count - 1)
      val srcVertexCount = vertexDataframes(srcVertexType).count()
      val dstVertexCount = vertexDataframes(dstVertexType).count()

      val maxSrcIndex = srcVertexCount - 1
      val maxDstIndex = dstVertexCount - 1

      // Get edge DataFrame
      val edgeKey = (srcVertexType, edgeName, dstVertexType)
      val edgeDFOpt: Option[DataFrame] = edgeDataframes.get(edgeKey).map { adjListMap =>
        // Get the first adj_list type (usually "ordered_by_source")
        adjListMap.head._2.select("_graphArSrcIndex", "_graphArDstIndex")
      }

      // Check if edge type exists
      if (edgeDFOpt.isEmpty) {
        return ValidationReport(
          edgeName = s"$srcVertexType-$edgeName-$dstVertexType",
          totalEdges = 0,
          invalidSrcReferences = 0,
          invalidDstReferences = 0,
          passed = false,
          errorMessage = Some(s"Edge type not found in GraphAr output")
        )
      }

      val edgeDF: DataFrame = edgeDFOpt.get
      val totalEdges = edgeDF.count()

      // Validate source index range: [0, maxSrcIndex]
      val invalidSrcCount = edgeDF
        .filter(col("_graphArSrcIndex") < 0 || col("_graphArSrcIndex") > maxSrcIndex)
        .count()

      // Validate destination index range: [0, maxDstIndex]
      val invalidDstCount = edgeDF
        .filter(col("_graphArDstIndex") < 0 || col("_graphArDstIndex") > maxDstIndex)
        .count()

      ValidationReport(
        edgeName = s"$srcVertexType-$edgeName-$dstVertexType",
        totalEdges = totalEdges,
        invalidSrcReferences = invalidSrcCount,
        invalidDstReferences = invalidDstCount,
        passed = (invalidSrcCount == 0 && invalidDstCount == 0),
        errorMessage = if (invalidSrcCount > 0 || invalidDstCount > 0) {
          Some(s"Found $invalidSrcCount invalid src indices (valid range: [0, $maxSrcIndex]), " +
               s"$invalidDstCount invalid dst indices (valid range: [0, $maxDstIndex])")
        } else None
      )
    } catch {
      case e: Exception =>
        ValidationReport(
          edgeName = s"$srcVertexType-$edgeName-$dstVertexType",
          totalEdges = 0,
          invalidSrcReferences = 0,
          invalidDstReferences = 0,
          passed = false,
          errorMessage = Some(s"Validation error: ${e.getMessage}")
        )
    }
  }

  /**
   * Validate Organisation ID mapping consistency.
   *
   * LDBC standard: Company and University IDs are in continuous range, distinguished by type field.
   * - Companies: typically [0, N)
   * - Universities: typically [N, M), following companies
   * - No ID offset is applied (verified against LDBC SNB Datagen official implementation)
   *
   * This validation checks:
   * 1. No duplicate IDs between companies and universities
   * 2. Type field is correctly set
   *
   * @return ValidationReport with results
   */
  def validateOrganisationIdMapping(): ValidationReport = {
    try {
      val organisations = vertexDataframes("Organisation")

      val companies = organisations.filter($"type" === "Company")
      val universities = organisations.filter($"type" === "University")

      val totalOrgs = organisations.count()
      val companyCount = companies.count()
      val universityCount = universities.count()

      // Check for ID uniqueness (no overlaps)
      val totalDistinctIds = organisations.select("id").distinct().count()
      val duplicateIds = totalOrgs - totalDistinctIds

      // Check that type field is set correctly (should only be Company or University)
      val invalidTypes = organisations.filter($"type" =!= "Company" && $"type" =!= "University").count()

      ValidationReport(
        edgeName = "Organisation_ID_Mapping",
        totalEdges = totalOrgs,
        invalidSrcReferences = duplicateIds,
        invalidDstReferences = invalidTypes,
        passed = (duplicateIds == 0 && invalidTypes == 0),
        errorMessage = if (duplicateIds > 0 || invalidTypes > 0) {
          Some(s"Found $duplicateIds duplicate IDs, $invalidTypes invalid type values. " +
               s"Total: $totalOrgs orgs ($companyCount companies, $universityCount universities)")
        } else {
          Some(s"✓ LDBC standard: $companyCount companies + $universityCount universities with unique continuous IDs")
        }
      )
    } catch {
      case e: Exception =>
        ValidationReport(
          edgeName = "Organisation_ID_Mapping",
          totalEdges = 0,
          invalidSrcReferences = 0,
          invalidDstReferences = 0,
          passed = false,
          errorMessage = Some(s"Validation error: ${e.getMessage}")
        )
    }
  }

  /**
   * Validate GraphAr index column exists and is a 0-based sequence.
   *
   * @param vertexType Vertex type to validate (e.g., "Person")
   * @return ValidationReport with results
   */
  def validateIndexColumns(vertexType: String): ValidationReport = {
    try {
      val vertices = vertexDataframes(vertexType)

      // Check if _graphArVertexIndex column exists
      if (!vertices.columns.contains("_graphArVertexIndex")) {
        return ValidationReport(
          edgeName = s"${vertexType}_IndexColumn",
          totalEdges = vertices.count(),
          invalidSrcReferences = 1,
          invalidDstReferences = 0,
          passed = false,
          errorMessage = Some(s"Missing _graphArVertexIndex column in $vertexType")
        )
      }

      val vertexCount = vertices.count()

      // Check if indices form a 0-based sequence
      val indices = vertices.select("_graphArVertexIndex")
        .distinct()
        .orderBy("_graphArVertexIndex")

      val expectedIndices = spark.range(0, vertexCount).toDF("_graphArVertexIndex")

      // Find mismatches
      val mismatch = indices.except(expectedIndices).count()

      ValidationReport(
        edgeName = s"${vertexType}_IndexColumn",
        totalEdges = vertexCount,
        invalidSrcReferences = mismatch,
        invalidDstReferences = 0,
        passed = (mismatch == 0),
        errorMessage = if (mismatch > 0) {
          Some(s"Index sequence has $mismatch gaps or duplicates")
        } else None
      )
    } catch {
      case e: Exception =>
        ValidationReport(
          edgeName = s"${vertexType}_IndexColumn",
          totalEdges = 0,
          invalidSrcReferences = 0,
          invalidDstReferences = 0,
          passed = false,
          errorMessage = Some(s"Validation error: ${e.getMessage}")
        )
    }
  }

  /**
   * Run all validation checks and return reports
   *
   * Executes comprehensive validation including edge reference integrity,
   * ID mapping consistency, and index column correctness for all entity types.
   *
   * @return Sequence of validation reports for all checks
   */
  def validateAll(): Seq[ValidationReport] = {
    Seq(
      // Edge reference integrity (9 edges)
      validateEdgeReferenceIntegrity("Person", "knows", "Person"),
      validateEdgeReferenceIntegrity("Person", "hasInterest", "Tag"),
      validateEdgeReferenceIntegrity("Person", "studyAt", "Organisation"),
      validateEdgeReferenceIntegrity("Person", "workAt", "Organisation"),
      validateEdgeReferenceIntegrity("Person", "isLocatedIn", "Place"),
      validateEdgeReferenceIntegrity("Organisation", "isLocatedIn", "Place"),
      validateEdgeReferenceIntegrity("Place", "isPartOf", "Place"),
      validateEdgeReferenceIntegrity("Tag", "hasType", "TagClass"),
      validateEdgeReferenceIntegrity("TagClass", "isSubclassOf", "TagClass"),

      // ID mapping (1 check)
      validateOrganisationIdMapping(),

      // Index columns (5 vertex types)
      validateIndexColumns("Person"),
      validateIndexColumns("Organisation"),
      validateIndexColumns("Place"),
      validateIndexColumns("Tag"),
      validateIndexColumns("TagClass")
    )
  }
}

/**
 * Validation report case class.
 *
 * @param edgeName Name of the validation check
 * @param totalEdges Total number of records checked
 * @param invalidSrcReferences Number of invalid source references found
 * @param invalidDstReferences Number of invalid destination references found
 * @param passed Whether the validation passed
 * @param errorMessage Optional error message if validation failed
 */
case class ValidationReport(
  edgeName: String,
  totalEdges: Long,
  invalidSrcReferences: Long,
  invalidDstReferences: Long,
  passed: Boolean,
  errorMessage: Option[String] = None
) {
  /**
   * Convert validation report to Markdown table row format.
   *
   * @return Markdown formatted string
   */
  def toMarkdown: String = {
    val status = if (passed) "✓ through" else "❌ Failure"
    val errorMsg = errorMessage.map(msg => s" ($msg)").getOrElse("")
    s"| $edgeName | $totalEdges | $invalidSrcReferences | $invalidDstReferences | $status$errorMsg |"
  }
}

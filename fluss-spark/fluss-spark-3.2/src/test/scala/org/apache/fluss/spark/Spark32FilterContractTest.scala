/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.spark

import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.{Schema, TableDescriptor, TableInfo, TablePath}
import org.apache.fluss.spark.adapter.SparkScanBuilderAdapter
import org.apache.fluss.types.DataTypes

import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThanOrEqual, IsNull, StringContains}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

/** Verifies the legacy Spark filter acceptance and residual contract. */
class Spark32FilterContractTest extends AnyFunSuite {

  test("accepted ARROW filters still require Spark row filtering") {
    val builder = newBuilder()
    val equal = EqualTo("name", "two")
    val range = GreaterThanOrEqual("id", 2)
    val filters = Array[Filter](equal, range, StringContains("name", "w"), IsNull("name"))
    assertThat(builder.pushFilters(filters).toSeq).isEqualTo(filters.toSeq)
    assertThat(builder.pushedFilters().toSeq).isEqualTo(Seq(equal, range))
    builder.pushFilters(Array.empty[Filter])
    assertThat(builder.pushedFilters().isEmpty).isTrue
  }

  test("primary-key filters remain Spark residuals") {
    val builder = newBuilder(primaryKey = true)
    val filters = Array[Filter](EqualTo("name", "two"))
    assertThat(builder.pushFilters(filters).toSeq).isEqualTo(filters.toSeq)
    assertThat(builder.pushedFilters().isEmpty).isTrue
  }

  test("non-ARROW log filters remain Spark residuals") {
    val builder = newBuilder("indexed")
    val filters = Array[Filter](EqualTo("id", 2))
    assertThat(builder.pushFilters(filters).toSeq).isEqualTo(filters.toSeq)
    assertThat(builder.pushedFilters().isEmpty).isTrue
  }

  test("unsupported literals are not advertised as pushed filters") {
    val builder = newBuilder()
    val filters = Array[Filter](
      EqualTo("dt", java.sql.Date.valueOf("2026-01-02")),
      EqualTo("amount", new java.math.BigDecimal("1.25")),
      EqualTo("id", "not an integer"),
      EqualTo("name", Integer.valueOf(2)),
      EqualTo("missing", 2),
      EqualTo("id", null)
    )
    assertThat(builder.pushFilters(filters).toSeq).isEqualTo(filters.toSeq)
    assertThat(builder.pushedFilters().isEmpty).isTrue
  }

  private def newBuilder(
      logFormat: String = "arrow",
      primaryKey: Boolean = false): SupportsPushDownFilters = {
    val tablePath = TablePath.of("fluss", "filters")
    val schemaBuilder = Schema
      .newBuilder()
      .column("id", DataTypes.INT().copy(false))
      .column("name", DataTypes.STRING())
      .column("dt", DataTypes.DATE())
      .column("amount", DataTypes.DECIMAL(10, 2))
    if (primaryKey) {
      schemaBuilder.primaryKey("id")
    }
    val descriptor = TableDescriptor
      .builder()
      .schema(schemaBuilder.build())
      .distributedBy(1)
      .property("table.log.format", logFormat)
      .build()
    val tableInfo = TableInfo.of(tablePath, 1L, 1, descriptor, "/tmp/filters", 0L, 0L)
    SparkScanBuilderAdapter
      .newScanBuilder(
        tablePath,
        tableInfo,
        new CaseInsensitiveStringMap(Collections.emptyMap[String, String]()),
        new Configuration())
      .asInstanceOf[SupportsPushDownFilters]
  }
}

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

package org.apache.fluss.spark.adapter

import org.apache.fluss.config.{Configuration => FlussConfiguration}
import org.apache.fluss.metadata.{LogFormat, TableInfo, TablePath}
import org.apache.fluss.predicate.{Predicate => FlussPredicate, PredicateBuilder}
import org.apache.fluss.row.BinaryString
import org.apache.fluss.spark.SparkConversions
import org.apache.fluss.spark.read.{AppendPlanner, FlussAppendBatch, FlussAppendMicroBatchStream, FlussScanBuilder, FlussUpsertBatch, FlussUpsertMicroBatchStream, UpsertPlanner}
import org.apache.fluss.types.DataTypeRoot

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** Spark 3.2 scan-builder entry point using Spark's legacy filter contract. */
object SparkScanBuilderAdapter {

  def newScanBuilder(
      tablePath: TablePath,
      tableInfo: TableInfo,
      options: CaseInsensitiveStringMap,
      flussConfig: FlussConfiguration): ScanBuilder = {
    new Spark32ScanBuilder(tablePath, tableInfo, options, flussConfig)
  }

  private class Spark32ScanBuilder(
      tablePath: TablePath,
      tableInfo: TableInfo,
      options: CaseInsensitiveStringMap,
      flussConfig: FlussConfiguration)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

    private var requiredSchema: Option[StructType] = None
    private var pushedPredicate: Option[FlussPredicate] = None
    private var acceptedFilters: Array[Filter] = Array.empty[Filter]

    override def pruneColumns(schema: StructType): Unit = {
      requiredSchema = Some(schema)
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val canPush = !tableInfo.hasPrimaryKey &&
        tableInfo.getTableConfig.getLogFormat == LogFormat.ARROW &&
        !tableInfo.getTableConfig.isDataLakeEnabled
      val converted = if (canPush) {
        filters.flatMap(filter => toFlussPredicate(tableInfo, filter).map((filter, _))).toSeq
      } else {
        Seq.empty[(Filter, FlussPredicate)]
      }
      acceptedFilters = converted.map(_._1).toArray
      pushedPredicate = converted.map(_._2) match {
        case Seq() => None
        case Seq(single) => Some(single)
        case predicates => Some(PredicateBuilder.and(predicates.asJava))
      }
      // Fluss skips record batches, not individual rows. Spark must reapply even accepted filters.
      // Keeping residuals also preserves filtering when the scan becomes a micro-batch source.
      filters
    }

    override def pushedFilters(): Array[Filter] = acceptedFilters

    override def build(): Scan = {
      new Spark32Scan(tablePath, tableInfo, requiredSchema, pushedPredicate, options, flussConfig)
    }
  }

  private class Spark32Scan(
      tablePath: TablePath,
      tableInfo: TableInfo,
      requiredSchema: Option[StructType],
      pushedPredicate: Option[FlussPredicate],
      options: CaseInsensitiveStringMap,
      flussConfig: FlussConfiguration)
    extends Scan {

    override def readSchema(): StructType = {
      requiredSchema.getOrElse(SparkConversions.toSparkDataType(tableInfo.getRowType))
    }

    override def description(): String = {
      s"FlussScan: [$tablePath], Type: [Spark32]"
    }

    override def toBatch(): org.apache.spark.sql.connector.read.Batch = {
      val projection = FlussScanBuilder.projectionOf(tableInfo, Some(readSchema()))
      if (tableInfo.getPrimaryKeys.isEmpty) {
        val planner =
          new AppendPlanner(
            tablePath,
            tableInfo,
            None,
            pushedPredicate,
            projection,
            options,
            flussConfig)
        new FlussAppendBatch(
          tablePath,
          tableInfo,
          readSchema(),
          pushedPredicate,
          None,
          options,
          flussConfig,
          planner)
      } else {
        val planner =
          new UpsertPlanner(
            tablePath,
            tableInfo,
            None,
            pushedPredicate,
            projection,
            options,
            flussConfig)
        new FlussUpsertBatch(
          tablePath,
          tableInfo,
          readSchema(),
          pushedPredicate,
          None,
          options,
          flussConfig,
          planner)
      }
    }

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      if (tableInfo.getPrimaryKeys.isEmpty) {
        new FlussAppendMicroBatchStream(
          tablePath,
          tableInfo,
          readSchema(),
          options,
          flussConfig,
          checkpointLocation)
      } else {
        new FlussUpsertMicroBatchStream(
          tablePath,
          tableInfo,
          readSchema(),
          options,
          flussConfig,
          checkpointLocation)
      }
    }
  }

  private def toFlussPredicate(tableInfo: TableInfo, filter: Filter): Option[FlussPredicate] = {
    val builder = new PredicateBuilder(tableInfo.getRowType)
    val converted = filter match {
      case equal: EqualTo => Some((equal.attribute, equal.value, builder.equal _))
      case greater: GreaterThan => Some((greater.attribute, greater.value, builder.greaterThan _))
      case greater: GreaterThanOrEqual =>
        Some((greater.attribute, greater.value, builder.greaterOrEqual _))
      case less: LessThan => Some((less.attribute, less.value, builder.lessThan _))
      case less: LessThanOrEqual => Some((less.attribute, less.value, builder.lessOrEqual _))
      case _ => None
    }
    converted.flatMap {
      case (fieldName, value, operator) =>
        val index = builder.indexOf(fieldName)
        if (index < 0 || value == null) {
          None
        } else {
          toFlussLiteral(tableInfo.getRowType.getTypeAt(index).getTypeRoot, value)
            .map(operator(index, _))
        }
    }
  }

  // Legacy filters carry Java values rather than Catalyst literals. Reject any type whose
  // conversion is not defined here; retaining Spark's residual cannot undo an unsafe batch skip.
  private def toFlussLiteral(root: DataTypeRoot, value: Any): Option[Object] = {
    (root, value) match {
      case (DataTypeRoot.STRING | DataTypeRoot.CHAR, text: String) =>
        Some(BinaryString.fromString(text))
      case (DataTypeRoot.BOOLEAN, boolean: java.lang.Boolean) => Some(boolean)
      case (DataTypeRoot.TINYINT, number: java.lang.Byte) => Some(number)
      case (DataTypeRoot.SMALLINT, number: java.lang.Short) => Some(number)
      case (DataTypeRoot.INTEGER, number: java.lang.Integer) => Some(number)
      case (DataTypeRoot.BIGINT, number: java.lang.Long) => Some(number)
      case _ => None
    }
  }
}

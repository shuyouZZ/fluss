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

import org.apache.spark.sql.catalyst.expressions.{Expression, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.DataType

/** Adapts SQL facilities to Spark 3.2, which has no timestamp-without-time-zone type. */
object SparkSqlAdapter {

  /** Fails explicitly because Spark 3.2 cannot represent timestamps without time zones. */
  def timestampWithoutTimeZone: DataType =
    throw new UnsupportedOperationException(
      "Spark 3.2 cannot represent Fluss TIMESTAMP without time zone. " +
        "Use TIMESTAMP_LTZ or Spark 3.4 or newer.")

  /** Whether the supplied type represents a timestamp without a time zone in this runtime. */
  def isTimestampWithoutTimeZone(dataType: DataType): Boolean = false

  /** Expands runtime-replaceable expressions before analysis-time evaluation of TVF bounds. */
  def replaceRuntimeExpressions(expression: Expression): Expression =
    expression.transformUp { case replaceable: RuntimeReplaceable => replaceable.child }

  /** Constructs a parse error using this runtime's source-position and exception signatures. */
  def parseException(
      command: Option[String],
      message: String,
      line: Option[Int],
      position: Option[Int]): ParseException = {
    val origin = Origin(line, position)
    new ParseException(command, message, origin, origin)
  }
}

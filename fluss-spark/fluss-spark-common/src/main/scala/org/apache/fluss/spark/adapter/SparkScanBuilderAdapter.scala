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
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.spark.read.{FlussAppendScanBuilder, FlussUpsertScanBuilder}

import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Spark-version seam for constructing a Fluss scan builder.
 *
 * <p>The default implementation targets the current Spark V2 predicate contract. Version-specific
 * connector artifacts replace this class with an implementation compiled against their Spark API.
 */
object SparkScanBuilderAdapter {

  def newScanBuilder(
      tablePath: TablePath,
      tableInfo: TableInfo,
      options: CaseInsensitiveStringMap,
      flussConfig: FlussConfiguration): ScanBuilder = {
    if (tableInfo.getPrimaryKeys.isEmpty) {
      new FlussAppendScanBuilder(tablePath, tableInfo, options, flussConfig)
    } else {
      new FlussUpsertScanBuilder(tablePath, tableInfo, options, flussConfig)
    }
  }
}

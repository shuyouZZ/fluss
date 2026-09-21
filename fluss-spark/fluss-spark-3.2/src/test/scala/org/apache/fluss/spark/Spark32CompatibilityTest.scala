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

import org.apache.fluss.metadata.{Schema, TableDescriptor}
import org.apache.fluss.types.DataTypes

import org.apache.spark.SparkException
import org.assertj.core.api.Assertions.assertThat

/** Catalog and SQL regressions for the Spark release compatibility seam. */
class Spark32CompatibilityTest extends FlussSparkTestBase {

  test("Spark 3.2 rejects dropping a nonempty namespace and honors CASCADE") {
    val namespace = "spark32_compat_namespace"
    sql(s"CREATE NAMESPACE $DEFAULT_CATALOG.$namespace")
    try {
      sql(s"CREATE TABLE $DEFAULT_CATALOG.$namespace.items (id INT)")
      val error = intercept[SparkException] {
        sql(s"DROP NAMESPACE $DEFAULT_CATALOG.$namespace")
      }
      assertThat(error.getMessage).contains("Cannot drop a non-empty namespace")
    } finally {
      sql(s"DROP NAMESPACE $DEFAULT_CATALOG.$namespace CASCADE")
    }
    assertThat(admin.listDatabases().get().contains(namespace)).isFalse
  }

  test("Spark 3.2 reports unsupported local timestamps when loading an existing Fluss table") {
    val path = createTablePath("spark32_local_timestamp")
    createFlussTable(
      path,
      TableDescriptor
        .builder()
        .schema(Schema.newBuilder().column("event_time", DataTypes.TIMESTAMP(6)).build())
        .distributedBy(1)
        .build())
    try {
      val error = intercept[UnsupportedOperationException] {
        sql(s"SELECT * FROM $DEFAULT_DATABASE.spark32_local_timestamp").collect()
      }
      assertThat(error.getMessage)
        .contains("Spark 3.2 cannot represent Fluss TIMESTAMP without time zone", "TIMESTAMP_LTZ")
    } finally {
      admin.dropTable(path, true).get()
    }
  }
}

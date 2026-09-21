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

import org.apache.fluss.exception.FlussRuntimeException
import org.apache.fluss.spark.write.{FlussAppendWrite, FlussUpsertWrite}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.PhysicalWriteInfo
import org.apache.spark.unsafe.types.UTF8String
import org.assertj.core.api.Assertions.assertThat

/** Exercises Spark's writer lifecycle against the packaged connector and a real Fluss cluster. */
class Spark32StreamingWriteTest extends FlussSparkTestBase {

  Seq(false, true).foreach {
    primaryKey =>
      test(
        s"streaming writer commit flushes and abort closes without rollback (primary key: $primaryKey)") {
        val tableName =
          if (primaryKey) "spark32_pk_writer_lifecycle" else "spark32_log_writer_lifecycle"
        withTable(tableName) {
          val primaryKeyProperty = if (primaryKey) ", 'primary.key' = 'id'" else ""
          sql(s"""CREATE TABLE $tableName (id INT, name STRING)
                 |TBLPROPERTIES ('bucket.num' = '1'$primaryKeyProperty)""".stripMargin)
          val tablePath = createTablePath(tableName)
          flussServer.waitUntilTableReady(admin.getTableInfo(tablePath).get().getTableId)
          val schema = spark.table(tableName).schema
          val write = if (primaryKey) {
            FlussUpsertWrite(tablePath, schema, flussServer.getClientConfig).toStreaming
          } else {
            FlussAppendWrite(tablePath, schema, flussServer.getClientConfig).toStreaming
          }
          val factory = write.createStreamingWriterFactory(new PhysicalWriteInfo {
            override def numPartitions(): Int = 1
          })
          val first = factory.createWriter(0, 0L, 0L)
          try {
            first.write(InternalRow(1, UTF8String.fromString("one")))
            val message = first.commit()
            assertThat(message).isNotNull
            // A task commit flushes its data before the driver's epoch callback.
            checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, "one")))
            write.commit(0L, Array(message))
            first.abort()
            val closed = intercept[FlussRuntimeException] {
              first.write(InternalRow(99, UTF8String.fromString("must not be written")))
            }
            assertThat(closed.getMessage).contains("Writer state: closed")
            // Fluss epoch abort does not roll back data already acknowledged by the server.
            write.abort(0L, Array(message))
            checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, "one")))
          } finally {
            first.close()
          }
          val next = factory.createWriter(0, 1L, 1L)
          try {
            next.write(InternalRow(2, UTF8String.fromString("two")))
            write.commit(1L, Array(next.commit()))
          } finally {
            next.close()
          }
          checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1, "one"), Row(2, "two")))
        }
      }
  }
}

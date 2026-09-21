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

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger

import java.io.File

class Spark32MicroBatchTest extends FlussSparkTestBase {

  test("Spark 3.2 micro-batch log reads resume from the checkpoint without replay") {
    checkCheckpointRecovery("spark32_log_stream", "'bucket.num' = '1'")
  }

  test("Spark 3.2 micro-batch primary-key reads resume from the checkpoint without replay") {
    checkCheckpointRecovery("spark32_pk_stream", "'bucket.num' = '1', 'primary.key' = 'id'")
  }

  private def checkCheckpointRecovery(tableName: String, properties: String): Unit = {
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id INT, name STRING) TBLPROPERTIES ($properties)")
      sql(s"INSERT INTO $tableName VALUES (1, 'one'), (2, 'two')")

      withTempDir {
        dir =>
          val checkpoint = new File(dir, "checkpoint").getAbsolutePath
          val output = new File(dir, "output").getAbsolutePath

          def consumeAvailable(): Unit = {
            val query = spark.readStream
              .option("scan.startup.mode", "earliest")
              .table(tableName)
              .writeStream
              .format("parquet")
              .option("checkpointLocation", checkpoint)
              .trigger(Trigger.ProcessingTime("100 milliseconds"))
              .start(output)
            try {
              query.processAllAvailable()
            } finally {
              query.stop()
            }
          }

          consumeAvailable()
          checkAnswer(spark.read.parquet(output), Seq(Row(1, "one"), Row(2, "two")))

          // New data arrives while the query is stopped. Reuse both the source checkpoint and sink.
          sql(s"INSERT INTO $tableName VALUES (3, 'three'), (4, 'four')")
          consumeAvailable()
          checkAnswer(
            spark.read.parquet(output),
            Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"), Row(4, "four")))

          // A second restart with no new input must not replay any previously committed rows.
          consumeAvailable()
          checkAnswer(
            spark.read.parquet(output),
            Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"), Row(4, "four")))
      }
    }
  }
}

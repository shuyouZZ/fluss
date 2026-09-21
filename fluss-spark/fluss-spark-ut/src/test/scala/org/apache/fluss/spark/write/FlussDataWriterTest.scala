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

package org.apache.fluss.spark.write

import org.apache.fluss.client.table.writer.TableWriter
import org.apache.fluss.config.Configuration
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.row.SparkAsFlussRow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.scalatest.funsuite.AnyFunSuite

import java.io.IOException
import java.util.concurrent.CompletableFuture

/** Tests the Spark data writer's acknowledgement and asynchronous failure contract. */
class FlussDataWriterTest extends AnyFunSuite {

  test("commit propagates an asynchronous write failure with its original cause") {
    val failure = new IllegalStateException("server rejected the write")
    val acknowledgement = new CompletableFuture[Void]()
    val writer = dataWriter(
      acknowledgement,
      () => {
        acknowledgement.completeExceptionally(failure)
        ()
      })

    writer.write(InternalRow.empty)

    assertThatThrownBy(() => writer.commit())
      .isInstanceOf(classOf[IOException])
      .hasCause(failure)
  }

  test("commit flushes successful writes before returning a commit message") {
    val acknowledgement = new CompletableFuture[Void]()
    val writer = dataWriter(
      acknowledgement,
      () => {
        acknowledgement.complete(null)
        ()
      })

    writer.write(InternalRow.empty)
    assertThat(acknowledgement.isDone).isFalse

    assertThat(writer.commit()).isEqualTo(FlussWriterCommitMessage())
    assertThat(acknowledgement.isDone).isTrue
  }

  private def dataWriter(
      acknowledgement: CompletableFuture[Void],
      flushAction: () => Unit): FlussDataWriter[Void] = {
    new FlussDataWriter[Void](
      TablePath.of("test", "table"),
      new StructType(),
      new Configuration()) {
      override val writer: TableWriter = new TableWriter {
        override def flush(): Unit = flushAction()
      }

      override def writeRow(record: SparkAsFlussRow): CompletableFuture[Void] = acknowledgement
    }
  }
}

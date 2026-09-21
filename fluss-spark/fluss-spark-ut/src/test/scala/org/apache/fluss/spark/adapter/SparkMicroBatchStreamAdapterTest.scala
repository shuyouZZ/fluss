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

import org.apache.fluss.spark.adapter.SparkMicroBatchStreamAdapterTest.{TestingMicroBatchStream, TestOffset}

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{Offset, ReadLimit}
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

class SparkMicroBatchStreamAdapterTest extends AnyFunSuite {

  test("ordinary admission-control reads follow the current end offset") {
    val stream = new TestingMicroBatchStream(TestOffset(10))
    assertThat(stream.latestOffset(TestOffset(0), stream.getDefaultReadLimit).json())
      .isEqualTo("10")
    stream.endOffset = TestOffset(20)
    assertThat(stream.latestOffset(TestOffset(10), ReadLimit.allAvailable()).json()).isEqualTo("20")
  }

  test("AvailableNow freezes its end offset until the trigger is prepared again") {
    val stream = new TestingMicroBatchStream(TestOffset(10))
    stream.prepareForTriggerAvailableNow()
    stream.endOffset = TestOffset(20)
    assertThat(stream.latestOffset(TestOffset(0), ReadLimit.allAvailable()).json()).isEqualTo("10")
    stream.prepareForTriggerAvailableNow()
    stream.endOffset = TestOffset(30)
    assertThat(stream.latestOffset(TestOffset(10), ReadLimit.allAvailable()).json()).isEqualTo("20")
  }

  test("bounded read limits are rejected even after preparing AvailableNow") {
    val stream = new TestingMicroBatchStream(TestOffset(10))
    stream.prepareForTriggerAvailableNow()
    val error = intercept[UnsupportedOperationException] {
      stream.latestOffset(TestOffset(0), ReadLimit.maxRows(1))
    }
    assertThat(error.getMessage).contains("Only ReadAllAvailable is supported")
  }

  test("the legacy callback directs callers to admission control") {
    val stream = new TestingMicroBatchStream(TestOffset(10))
    val error = intercept[UnsupportedOperationException](stream.latestOffset())
    assertThat(error.getMessage).contains("latestOffset(Offset, ReadLimit) should be called")
  }
}

/** Shared lifecycle fixture for the default and Spark 3.2 adapter contracts. */
object SparkMicroBatchStreamAdapterTest {

  case class TestOffset(value: Long) extends Offset {
    override def json(): String = value.toString
  }

  class TestingMicroBatchStream(var endOffset: Offset) extends SparkMicroBatchStreamAdapter {
    override protected def currentEndOffset(): Offset = endOffset
    override def initialOffset(): Offset = TestOffset(0)
    override def deserializeOffset(json: String): Offset = TestOffset(json.toLong)
    override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] =
      Array.empty
    override def createReaderFactory(): PartitionReaderFactory = {
      throw new UnsupportedOperationException("Lifecycle fixture does not read rows")
    }
    override def commit(end: Offset): Unit = {}
    override def stop(): Unit = {}
  }
}

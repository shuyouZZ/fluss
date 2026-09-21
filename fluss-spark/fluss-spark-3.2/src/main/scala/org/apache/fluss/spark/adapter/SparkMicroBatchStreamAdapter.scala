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

import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}

/**
 * Adapts shared micro-batch reads to Spark 3.2's legacy latestOffset callback.
 *
 * Spark 3.2 has no AvailableNow trigger API. Use ProcessingTime or Once instead; the connector
 * deliberately does not reference the newer SupportsTriggerAvailableNow interface.
 */
abstract class SparkMicroBatchStreamAdapter extends MicroBatchStream {

  /** Returns the current end offset without applying trigger-specific bounds. */
  protected def currentEndOffset(): Offset

  override def latestOffset(): Offset = currentEndOffset()
}

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

import org.apache.spark.sql.connector.catalog.SupportsNamespaces

/** Adapts catalog namespace operations whose contracts differ between Spark releases. */
trait SparkNamespaceAdapter extends SupportsNamespaces {

  /** Drops the database after the adapter has interpreted this runtime's cascade contract. */
  protected def dropFlussNamespace(namespace: Array[String], cascade: Boolean): Boolean

  /** Preserves the default catalog's existing non-root namespace listing. */
  protected def namespaceChildren(databaseName: String): Array[Array[String]] =
    Array(Array(databaseName))

  /** Delegates deletion using the namespace contract of this Spark runtime. */
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    dropFlussNamespace(namespace, cascade)
}

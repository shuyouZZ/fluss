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

import org.apache.fluss.client.ConnectionFactory
import org.apache.fluss.spark.adapter.{SparkMicroBatchStreamAdapter, SparkNamespaceAdapter, SparkScanBuilderAdapter, SparkSqlAdapter}

import org.assertj.core.api.Assertions.assertThat
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets
import java.util.jar.JarFile

import scala.collection.JavaConverters._

/** Checks the deployable artifact rather than an unshaded compilation directory. */
class Spark32ArtifactTest extends AnyFunSuite {

  test("Spark 3.2 artifact contains shared behavior and adapters without Spark runtime classes") {
    val location =
      new File(classOf[SparkCatalog].getProtectionDomain.getCodeSource.getLocation.toURI)
    val artifactPath = Option(System.getProperty("spark32.artifact.path")).getOrElse {
      throw new IllegalStateException("Run the packaged connector acceptance tests with mvn verify")
    }
    val expectedArtifact = new File(artifactPath).getCanonicalFile
    assertThat(location.getCanonicalFile).isEqualTo(expectedArtifact)
    Seq(
      classOf[SparkCatalog],
      classOf[ConnectionFactory],
      classOf[SparkMicroBatchStreamAdapter],
      classOf[SparkNamespaceAdapter],
      SparkScanBuilderAdapter.getClass,
      SparkSqlAdapter.getClass
    ).foreach {
      implementation =>
        val origin = new File(implementation.getProtectionDomain.getCodeSource.getLocation.toURI)
        assertThat(origin.getCanonicalFile).isEqualTo(expectedArtifact)
    }
    assertThat(location.getName).startsWith("fluss-spark-3.2_")
    assertThat(location.getName).endsWith(".jar")
    val jar = new JarFile(location)
    try {
      val names = jar.entries().asScala.map(_.getName).toSeq
      assertThat[String](names.toArray).contains(
        "org/apache/fluss/spark/SparkCatalog.class",
        "org/apache/fluss/spark/read/FlussMicroBatchStream.class",
        "org/apache/fluss/spark/adapter/SparkScanBuilderAdapter$.class",
        "org/apache/fluss/client/ConnectionFactory.class"
      )
      assertThat(names.distinct.size).isEqualTo(names.size)
      val sparkClasses =
        names.filter(n => n.startsWith("org/apache/spark/") && n.endsWith(".class"))
      // These are connector-owned package shims; no Spark implementation belongs in this JAR.
      assertThat[String](sparkClasses.filterNot {
        name =>
          name.startsWith("org/apache/spark/sql/CatalogV2UtilShim") ||
          name.startsWith("org/apache/spark/sql/FlussIdentityTransform") ||
          name.startsWith("org/apache/spark/sql/catalyst/parser/Fluss") ||
          name.startsWith("org/apache/spark/sql/catalyst/parser/UpperCaseCharStream") ||
          name.startsWith("org/apache/spark/sql/catalyst/parser/Origin") ||
          name.startsWith("org/apache/spark/sql/catalyst/parser/CurrentOrigin")
      }.toArray).isEmpty()
      assertThat(names.exists(_.startsWith("scala/"))).isFalse()

      val unavailableApis = Seq(
        "org/apache/spark/sql/connector/expressions/filter/Predicate",
        "org/apache/spark/sql/connector/read/SupportsPushDownV2Filters",
        "org/apache/spark/sql/connector/read/SupportsPushDownLimit",
        "org/apache/spark/sql/connector/read/streaming/SupportsTriggerAvailableNow",
        "org/apache/spark/sql/types/TimestampNTZType"
      )
      // Default-only common helpers can mention newer APIs; only the selected adapters must
      // have a Spark 3.2 linkage surface. Shared behavior is exercised by the reused suites.
      names
        .filter(n => n.startsWith("org/apache/fluss/spark/adapter/") && n.endsWith(".class"))
        .foreach {
          name =>
            val stream = jar.getInputStream(jar.getJarEntry(name))
            val bytes =
              try {
                val output = new ByteArrayOutputStream()
                org.apache.fluss.utils.IOUtils.copyBytes(stream, output, false)
                output.toByteArray
              } finally {
                stream.close()
              }
            val constantPool = new String(bytes, StandardCharsets.ISO_8859_1)
            unavailableApis.foreach {
              api =>
                val assertion = assertThat(constantPool.contains(api))
                assertion.withFailMessage(s"$name links to unavailable Spark API $api")
                assertion.isFalse()
            }
        }
    } finally {
      jar.close()
    }
    assertThat(SparkScanBuilderAdapter.getClass.getProtectionDomain.getCodeSource.getLocation)
      .isEqualTo(classOf[SparkCatalog].getProtectionDomain.getCodeSource.getLocation)
  }
}

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

package org.apache.fluss.spark;

import org.junit.jupiter.api.Test;
import org.scalatest.tools.Runner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs acceptance suites with the shaded connector selected by the integration-test classpath. */
class Spark32ArtifactITCase {

    @Test
    void testPackagedConnector() throws IOException {
        Path reports = Paths.get(System.getProperty("spark32.artifact.reports"));
        Files.createDirectories(reports);
        assertThat(
                        Runner.run(
                                new String[] {
                                    "-s",
                                    "org.apache.fluss.spark.Spark32ArtifactTest",
                                    "-s",
                                    "org.apache.fluss.spark.Spark32BatchQueryTest",
                                    "-s",
                                    "org.apache.fluss.spark.Spark32MicroBatchTest",
                                    "-s",
                                    "org.apache.fluss.spark.SparkStreamingTest",
                                    "-s",
                                    "org.apache.fluss.spark.Spark32StreamingWriteTest",
                                    "-o",
                                    "-u",
                                    reports.toString()
                                }))
                .as("Packaged connector acceptance suites")
                .isTrue();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for bundled Iceberg regional configuration. */
class IcebergRegionConfigTest {

    @TempDir private Path directory;

    @Test
    void testRegionSelectionAndFallback() throws Exception {
        writeHiveSite("cn", "cn-value");
        try (URLClassLoader loader = resourceLoader()) {
            Configuration conf = new Configuration(false);
            IcebergRegionConfig.load(conf, loader, "worker.us01");
            assertThat(conf.get("hive.client.key.private")).isEqualTo("cn-value");
        }

        writeHiveSite("us", "us-value");
        try (URLClassLoader loader = resourceLoader()) {
            Configuration conf = new Configuration(false);
            IcebergRegionConfig.load(conf, loader, "worker.us01");
            assertThat(conf.get("hive.client.key.private")).isEqualTo("us-value");
            conf = new Configuration(false);
            IcebergRegionConfig.load(conf, loader, null);
            assertThat(conf.get("hive.client.key.private")).isEqualTo("cn-value");
        }
    }

    @Test
    void testConfigurationEntryPointAndExplicitOverride() throws Exception {
        writeHiveSite("cn", "bundled-value");
        writeHiveSite("us", "bundled-value");
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = resourceLoader()) {
            Thread.currentThread().setContextClassLoader(loader);
            Configuration conf =
                    (Configuration)
                            IcebergConfiguration.from(new org.apache.fluss.config.Configuration())
                                    .get();
            assertThat(conf.get("hive.client.key.private")).isEqualTo("bundled-value");

            org.apache.fluss.config.Configuration explicit =
                    new org.apache.fluss.config.Configuration();
            explicit.setString("iceberg.hadoop.hive.client.key.private", "explicit-value");
            conf = (Configuration) IcebergConfiguration.from(explicit).get();
            assertThat(conf.get("hive.client.key.private")).isEqualTo("explicit-value");
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testMissingResourcesPreserveConfiguration() throws Exception {
        try (URLClassLoader loader = resourceLoader()) {
            Configuration conf = new Configuration(false);
            conf.set("existing", "value");
            IcebergRegionConfig.load(conf, loader, "worker.cn");
            assertThat(conf.get("existing")).isEqualTo("value");
            assertThat(conf.get("hive.client.key.private")).isNull();
        }
    }

    @Test
    void testIncompleteCredentialsHaveSanitizedFailure() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("ic.x7u.k", "sensitive-value");
        try (URLClassLoader loader = resourceLoader()) {
            assertThatThrownBy(
                            () ->
                                    IcebergRegionConfig.initializeSuperUser(
                                            TestingHadoopUtil.class, properties, loader))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Cannot initialize bundled Iceberg super-user credentials")
                    .hasNoCause();
            assertThat(TestingHadoopUtil.hiveSuperUser).isNull();
            assertThat(TestingHadoopUtil.hiveSuperPwd).isNull();
        }
    }

    @Test
    void testSuperUserInitializationPreservesExistingCredentials() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("ic.x7u.k", "user");
        properties.setProperty("ic.x9p.v", "password");
        try (URLClassLoader loader = resourceLoader()) {
            IcebergRegionConfig.initializeSuperUser(TestingHadoopUtil.class, properties, loader);
            assertThat(TestingHadoopUtil.hiveSuperUser).isEqualTo("user");
            assertThat(TestingHadoopUtil.hiveSuperPwd).isEqualTo("password");

            properties.setProperty("ic.x7u.k", "");
            properties.remove("ic.x9p.v");
            IcebergRegionConfig.initializeSuperUser(TestingHadoopUtil.class, properties, loader);
            assertThat(TestingHadoopUtil.hiveSuperUser).isEqualTo("user");
            assertThat(TestingHadoopUtil.hiveSuperPwd).isEqualTo("password");
        } finally {
            TestingHadoopUtil.hiveSuperUser = null;
            TestingHadoopUtil.hiveSuperPwd = null;
        }
    }

    private URLClassLoader resourceLoader() throws Exception {
        return new URLClassLoader(
                new URL[] {directory.toUri().toURL()}, IcebergRegionConfig.class.getClassLoader()) {
            @Override
            public URL getResource(String name) {
                return name.startsWith("conf/") ? findResource(name) : super.getResource(name);
            }
        };
    }

    private void writeHiveSite(String region, String value) throws Exception {
        Path path = directory.resolve("conf/" + region + "/hive-site.xml");
        Files.createDirectories(path.getParent());
        String xml =
                "<configuration><property><name>hive.client.key.private</name><value>"
                        + value
                        + "</value></property></configuration>";
        Files.write(path, xml.getBytes(StandardCharsets.UTF_8));
    }

    private static final class TestingHadoopUtil {
        private static String hiveSuperUser;
        private static String hiveSuperPwd;
    }
}

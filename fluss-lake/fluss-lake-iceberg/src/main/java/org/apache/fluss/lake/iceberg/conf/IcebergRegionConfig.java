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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Properties;

/** Loads the configuration bundled with internal Iceberg distributions. */
final class IcebergRegionConfig {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRegionConfig.class);

    private IcebergRegionConfig() {}

    static void load(Configuration hadoopConf) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = IcebergRegionConfig.class.getClassLoader();
        }
        load(hadoopConf, classLoader, System.getenv("HOSTNAME"));
    }

    static void load(Configuration hadoopConf, ClassLoader classLoader, String hostname) {
        URL hiveSite = resource(classLoader, hostname, "hive-site.xml");
        if (hiveSite != null) {
            hadoopConf.addResource(hiveSite);
            LOG.info("Loaded bundled Iceberg regional Hive configuration");
        }

        URL propertiesFile = resource(classLoader, hostname, "config.properties");
        if (propertiesFile == null) {
            return;
        }
        Properties properties = new Properties();
        try (InputStream input = propertiesFile.openStream()) {
            properties.load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load bundled Iceberg regional properties");
        }

        String user = properties.getProperty("ic.x7u.k");
        String password = properties.getProperty("ic.x9p.v");
        if (user == null && password == null) {
            return;
        }
        try {
            Class<?> hadoopUtil =
                    Class.forName("org.apache.iceberg.util.HadoopUtil", true, classLoader);
            initializeSuperUser(hadoopUtil, properties, classLoader);
        } catch (ClassNotFoundException e) {
            // Upstream Iceberg does not have the internal super-user extension.
            LOG.debug("Iceberg distribution has no internal Hadoop credential helper");
        }
    }

    private static URL resource(ClassLoader classLoader, String hostname, String name) {
        String region = hostname != null && hostname.endsWith(".us01") ? "us" : "cn";
        URL resource = classLoader.getResource("conf/" + region + "/" + name);
        if (resource == null && "us".equals(region)) {
            resource = classLoader.getResource("conf/cn/" + name);
        }
        return resource;
    }

    private static String decode(String value, ClassLoader classLoader)
            throws ReflectiveOperationException {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Empty bundled Iceberg credential");
        }
        Class<?> crypto;
        try {
            crypto =
                    Class.forName(
                            "org.apache.iceberg.flink.util.CredentialCryptoUtil",
                            true,
                            classLoader);
        } catch (ClassNotFoundException e) {
            return value;
        }
        Method decrypt = crypto.getMethod("decrypt", String.class);
        String decoded = (String) decrypt.invoke(null, value);
        return decoded == null ? value : decoded;
    }

    static void initializeSuperUser(
            Class<?> hadoopUtil, Properties properties, ClassLoader classLoader) {
        synchronized (hadoopUtil) {
            try {
                Field userField = hadoopUtil.getDeclaredField("hiveSuperUser");
                Field passwordField = hadoopUtil.getDeclaredField("hiveSuperPwd");
                userField.setAccessible(true);
                passwordField.setAccessible(true);
                if (userField.get(null) != null || passwordField.get(null) != null) {
                    return;
                }
                String user = decode(properties.getProperty("ic.x7u.k"), classLoader);
                String password = decode(properties.getProperty("ic.x9p.v"), classLoader);
                // Initialize the helper's existing cache before any catalog operations start.
                passwordField.set(null, password);
                userField.set(null, user);
            } catch (ReflectiveOperationException | RuntimeException e) {
                // Reflection and decryption errors can include credentials in their messages.
                throw new IllegalStateException(
                        "Cannot initialize bundled Iceberg super-user credentials");
            }
        }
    }
}

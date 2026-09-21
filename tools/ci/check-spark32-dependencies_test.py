################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""Run with python3 tools/ci/check-spark32-dependencies_test.py."""

import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


class Spark32DependenciesTest(unittest.TestCase):
    def setUp(self):
        self.script = Path(__file__).with_name("check-spark32-dependencies.py")
        pom = ET.parse(self.script.resolve().parents[2] / "pom.xml")
        self.scala_version = pom.findtext(
            "m:properties/m:scala212.version",
            namespaces={"m": "http://maven.apache.org/POM/4.0.0"},
        )
        self.dependencies = "\n".join(
            ["The following files have been resolved:"]
            + [
                "   org.apache.spark:" + artifact + "_2.12:jar:" + suffix
                for artifact in ("spark-core", "spark-sql", "spark-catalyst")
                for suffix in ("3.2.4:provided", "tests:3.2.4:test")
            ]
            + [
                "   org.scala-lang:" + artifact + ":jar:" + self.scala_version + ":compile"
                for artifact in ("scala-library", "scala-reflect", "scala-compiler")
            ]
            + ["   org.scala-lang.modules:scala-xml_2.12:jar:1.2.0:compile"]
        )

    def run_checker(self, dependencies):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "dependencies.txt"
            report.write_text(dependencies)
            return subprocess.run(
                [sys.executable, str(self.script), str(report)],
                capture_output=True,
                text=True,
                check=False,
            )

    def assert_rejected(self, dependencies, message):
        result = self.run_checker(dependencies)
        self.assertNotEqual(0, result.returncode)
        self.assertIn(message, result.stderr)

    def test_valid_closure_with_and_without_maven_module_annotations(self):
        for suffix in ("", "\x1b[36m -- module example\x1b[0;1m [auto]\x1b[m"):
            with self.subTest(suffix=suffix):
                result = self.run_checker(self.dependencies.replace(":compile", ":compile" + suffix))
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn("Verified Apache Spark 3.2.4 / Scala " + self.scala_version, result.stdout)

    def test_mixed_spark_versions(self):
        self.assert_rejected(
            self.dependencies.replace("spark-core_2.12:jar:3.2.4:", "spark-core_2.12:jar:3.5.6:"),
            "Unexpected Spark dependency",
        )

    def test_mixed_scala_versions(self):
        self.assert_rejected(
            self.dependencies.replace("scala-reflect:jar:" + self.scala_version, "scala-reflect:jar:2.13.16"),
            "Unexpected Scala dependency",
        )

    def test_wrong_scala_module_binary_version(self):
        self.assert_rejected(
            self.dependencies.replace("scala-xml_2.12:", "scala-xml_2.13:"),
            "Unexpected Scala module",
        )

    def test_missing_spark_main_jar(self):
        self.assert_rejected(
            "\n".join(line for line in self.dependencies.splitlines() if "spark-sql_2.12:jar:3.2.4:" not in line),
            "Missing required Spark/Scala dependencies",
        )

    def test_empty_report(self):
        self.assert_rejected("", "Missing required Spark/Scala dependencies")

    def test_malformed_coordinate(self):
        self.assert_rejected(self.dependencies + "\norg.apache.spark:broken", "Unexpected dependency coordinate")


if __name__ == "__main__":
    unittest.main()

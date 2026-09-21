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
"""Check the resolved test closure emitted by Maven dependency:list."""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def check_dependencies(dependency_file):
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(Path(__file__).resolve().parents[2] / "pom.xml")
    scala_version = pom.findtext("m:properties/m:scala212.version", namespaces=namespace)
    if not scala_version or not scala_version.startswith("2.12."):
        raise ValueError("The repository must define its Scala 2.12 version")

    resolved = set()
    text = re.sub(r"\x1b\[[0-9;]*m", "", Path(dependency_file).read_text())
    for line in text.splitlines():
        coordinate = line.strip().split(" ", 1)[0]
        if not coordinate.startswith(("org.apache.spark:", "org.scala-lang")):
            continue
        parts = coordinate.split(":")
        if len(parts) not in (5, 6):
            raise ValueError("Unexpected dependency coordinate: " + coordinate)
        group, artifact = parts[:2]
        version = parts[-2]
        classifier = parts[3] if len(parts) == 6 else ""
        if group == "org.apache.spark":
            if version != "3.2.4" or not artifact.endswith("_2.12"):
                raise ValueError("Unexpected Spark dependency: " + coordinate)
        elif group == "org.scala-lang":
            if version != scala_version:
                raise ValueError("Unexpected Scala dependency: " + coordinate)
        elif group == "org.scala-lang.modules" and not artifact.endswith("_2.12"):
            raise ValueError("Unexpected Scala module: " + coordinate)
        resolved.add((group, artifact, classifier))

    required = {
        ("org.apache.spark", artifact + "_2.12", classifier)
        for artifact in ("spark-core", "spark-sql", "spark-catalyst")
        for classifier in ("", "tests")
    }
    required.update(
        ("org.scala-lang", artifact, "")
        for artifact in ("scala-library", "scala-reflect", "scala-compiler")
    )
    missing = required - resolved
    if missing:
        raise ValueError("Missing required Spark/Scala dependencies: " + str(sorted(missing)))
    print("Verified Apache Spark 3.2.4 / Scala " + scala_version + " test dependencies")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: check-spark32-dependencies.py <dependency-list.txt>")
    try:
        check_dependencies(sys.argv[1])
    except ValueError as error:
        sys.exit(str(error))

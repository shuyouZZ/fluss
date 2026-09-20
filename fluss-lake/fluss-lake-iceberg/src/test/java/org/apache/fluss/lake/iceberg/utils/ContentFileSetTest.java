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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContentFileSet}. */
class ContentFileSetTest {

    @Test
    void testDeduplicatesFilesWithTheSamePath() {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        DataFile firstFile = createDataFile(spec, 100L);
        DataFile duplicatePathFile = createDataFile(spec, 200L);

        ContentFileSet<DataFile> files = ContentFileSet.create();

        assertThat(files.add(firstFile)).isTrue();
        assertThat(files.add(duplicatePathFile)).isFalse();
        assertThat(files).containsExactly(firstFile);
    }

    private DataFile createDataFile(PartitionSpec spec, long recordCount) {
        return DataFiles.builder(spec)
                .withPath("/warehouse/data/file.parquet")
                .withFileSizeInBytes(4096L)
                .withRecordCount(recordCount)
                .withFormat(FileFormat.PARQUET)
                .build();
    }
}

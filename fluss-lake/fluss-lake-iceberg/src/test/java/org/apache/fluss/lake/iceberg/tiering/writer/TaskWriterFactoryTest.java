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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that tiering writers honor Iceberg table properties. */
class TaskWriterFactoryTest {

    @TempDir private File tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testParquetCompressionProperties(boolean primaryKey) throws Exception {
        Schema schema =
                new Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                                Types.NestedField.optional(2, "value", Types.StringType.get())),
                        primaryKey ? Collections.singleton(1) : Collections.emptySet());
        Map<String, String> properties = new HashMap<>();
        properties.put(TableProperties.FORMAT_VERSION, "2");
        properties.put(TableProperties.PARQUET_COMPRESSION, "zstd");
        properties.put(TableProperties.PARQUET_COMPRESSION_LEVEL, "3");
        Table table =
                new HadoopTables(new Configuration())
                        .create(
                                schema,
                                PartitionSpec.unpartitioned(),
                                properties,
                                new File(tempDir, "table").toURI().toString());

        WriteResult result;
        try (TaskWriter<Record> writer = TaskWriterFactory.createTaskWriter(table, null, 0)) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("id", 1);
            record.setField("value", "tiering compression properties");
            writer.write(record);
            if (primaryKey) {
                record = record.copy();
                record.setField("id", 2);
                ((GenericRecordDeltaWriter) writer).delete(record);
            }
            result = writer.complete();
        }

        assertThat(result.dataFiles()).hasSize(1);
        assertZstdCompression(result.dataFiles()[0].path().toString());
        if (primaryKey) {
            assertThat(result.deleteFiles()).hasSize(1);
            assertZstdCompression(result.deleteFiles()[0].path().toString());
        }
    }

    private static void assertZstdCompression(String file) throws IOException {
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
            assertThat(reader.getFooter().getBlocks()).isNotEmpty();
            reader.getFooter()
                    .getBlocks()
                    .forEach(
                            block ->
                                    assertThat(block.getColumns())
                                            .allSatisfy(
                                                    column ->
                                                            assertThat(column.getCodec())
                                                                    .isEqualTo(
                                                                            CompressionCodecName
                                                                                    .ZSTD)));
        }
    }
}

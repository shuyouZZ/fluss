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

import org.apache.spark.sql.Row

/**
 * Tests legacy filter and LIMIT semantics without the shared read suites' Spark 3.4+ V2 predicate
 * and pushed-limit assertions. Catalog, writes and row conversion use the shared test suites.
 */
class Spark32BatchQueryTest extends FlussSparkTestBase {

  Seq(false, true).foreach {
    primaryKey =>
      test(s"Spark 3.2 projection, filters and LIMIT preserve rows (primary key: $primaryKey)") {
        withTable("spark32_queries") {
          val properties = if (primaryKey) {
            "TBLPROPERTIES ('primary.key' = 'id', 'bucket.num' = '1')"
          } else {
            "TBLPROPERTIES ('bucket.num' = '1')"
          }
          sql(s"""CREATE TABLE $DEFAULT_DATABASE.spark32_queries
                 |(id INT, name STRING, amount INT) $properties""".stripMargin)
          sql(s"""INSERT INTO $DEFAULT_DATABASE.spark32_queries VALUES
                 |(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), (4, 'four', 40)""".stripMargin)
          if (primaryKey) {
            sql(s"INSERT INTO $DEFAULT_DATABASE.spark32_queries VALUES (2, 'two', 22)")
          }
          checkAnswer(
            sql(s"SELECT name FROM $DEFAULT_DATABASE.spark32_queries ORDER BY id"),
            Row("one") :: Row("two") :: Row("three") :: Row("four") :: Nil)
          checkAnswer(
            sql(s"SELECT name FROM $DEFAULT_DATABASE.spark32_queries WHERE id >= 2 AND id < 4"),
            Row("two") :: Row("three") :: Nil)
          checkAnswer(
            sql(s"""SELECT name FROM $DEFAULT_DATABASE.spark32_queries
                   |WHERE id >= 2 AND id % 2 = 0 ORDER BY id LIMIT 1""".stripMargin),
            Row("two") :: Nil
          )
          checkAnswer(
            sql(s"SELECT id FROM $DEFAULT_DATABASE.spark32_queries WHERE name = 'three'"),
            Row(3) :: Nil)
          checkAnswer(
            sql(s"SELECT count(*) FROM $DEFAULT_DATABASE.spark32_queries"),
            Row(4L) :: Nil)
          checkAnswer(
            sql(s"SELECT count(*) FROM $DEFAULT_DATABASE.spark32_queries WHERE id > 2"),
            Row(2L) :: Nil)
          checkAnswer(
            sql(s"SELECT amount FROM $DEFAULT_DATABASE.spark32_queries WHERE id = 2"),
            Row(if (primaryKey) 22 else 20) :: Nil)
          checkAnswer(
            sql(s"SELECT * FROM $DEFAULT_DATABASE.spark32_queries LIMIT 0"),
            Seq.empty[Row])
        }
      }
  }

  test("Spark 3.2 evaluates non-ARROW filters in Spark") {
    withTable("spark32_indexed") {
      sql(s"""CREATE TABLE $DEFAULT_DATABASE.spark32_indexed (id INT, name STRING)
             |TBLPROPERTIES ('table.log.format' = 'indexed', 'bucket.num' = '1')""".stripMargin)
      sql(s"INSERT INTO $DEFAULT_DATABASE.spark32_indexed VALUES (1, 'one'), (2, 'two')")
      checkAnswer(
        sql(s"SELECT name FROM $DEFAULT_DATABASE.spark32_indexed WHERE id = 2"),
        Row("two") :: Nil)
    }
  }

  test("Spark 3.2 preserves temporal and decimal values with residual filtering") {
    withSQLConf("spark.sql.session.timeZone" -> "UTC") {
      withTable("spark32_types") {
        sql(s"""CREATE TABLE $DEFAULT_DATABASE.spark32_types
               |(id INT, dt DATE, ts TIMESTAMP, amount DECIMAL(10, 2))""".stripMargin)
        sql(s"""INSERT INTO $DEFAULT_DATABASE.spark32_types VALUES
               |(1, DATE '2026-01-01', TIMESTAMP '2026-01-01 12:00:00', 1.25),
               |(2, DATE '2026-01-02', TIMESTAMP '2026-01-02 12:00:00', 2.50)""".stripMargin)
        checkAnswer(
          sql(s"""SELECT id, CAST(dt AS STRING), CAST(ts AS STRING), CAST(amount AS STRING)
                 |FROM $DEFAULT_DATABASE.spark32_types
                 |WHERE dt = DATE '2026-01-02' AND amount > 2.00
                 |AND ts >= TIMESTAMP '2026-01-02 00:00:00'""".stripMargin),
          Row(2, "2026-01-02", "2026-01-02 12:00:00", "2.50") :: Nil
        )
      }
    }
  }
}

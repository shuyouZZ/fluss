# Spark 3.2 适配与主分支能力对比

## 对比范围

- 对比基准：本地 `main`，提交 `14f9aaed6ff8239f4f84bafce6b0d05736629942`，核对日期 2026-09-21。
  这是本次工作开始时的本地主分支，不代表实时远端最新版。
- 主分支 Spark 模块：3.4.3、3.5.7；本次新增内部目标：**Apache Spark 3.2.4 / Scala 2.12**。
- Scala 继承根 POM 的 2.12.18，构建和本地验收使用 Java 11；没有给 3.2 profile 改写 Scala/JVM target。
  本轮没有另跑 Spark 发行包自带 Scala 2.12.15 的运行矩阵，不将 2.12.18 的结果扩大到该组合。
- 比较仅覆盖 Spark 适配。当前分支基座中已有的 `08796d270` Flink 1.18/Iceberg profile 不属于本次 Spark 修改。
- “支持”指下列指定运行时和已验收场景；“未验收”不等于已经证明不可用。
  Dream Spark fork、其他 3.2 补丁版本、其他 JDK/部署组合不能自动套用本结论。

## 实现边界

复用 `fluss-spark-common` 二进制和共享业务逻辑，3.2 编译并替换四个小入口：

| 入口 | 3.2 处理内容 | 继续复用的逻辑 |
| --- | --- | --- |
| `SparkScanBuilderAdapter` | legacy `Filter`、列裁剪、声明可用扫描能力 | 批规划器、读分区、记录读取 |
| `SparkMicroBatchStreamAdapter` | 旧版 `latestOffset()` 回调 | checkpoint、offset、分区规划 |
| `SparkSqlAdapter` | Catalyst/解析异常/Origin/时间类型 API 差异 | SQL parser、CALL、TVF、类型转换 |
| `SparkNamespaceAdapter` | namespace 删除签名和子 namespace 枚举 | Fluss catalog/database 操作 |

没有复制整套 parser、catalog、读写实现；没有新增独立 3.2 UT 模块。
3.4/3.5 版本模块未修改，common 只保留必要入口；流式写继续直接复用 common。
本轮另外修复共享 `FlussDataWriter` 的异步错误状态初值：由 `null` 改为 `None`，
避免首次异步写入失败被回调中的空指针异常吞掉。该修复由共享 writer 契约测试覆盖。

## 能力对比

| 能力 | 主分支 3.4/3.5 | Spark 3.2.4 结果与边界 |
| --- | --- | --- |
| Fluss catalog、namespace、建表/删表/属性 | 支持 | 支持已测场景；删除非空 namespace 的 RESTRICT/CASCADE 由 3.2 入口适配 |
| 分区 DDL：列出、增加、删除 | 支持 | 支持，共享 Catalog 测试覆盖 |
| 日志表/主键表批读写 | 支持 | 支持；主键写入保留 upsert 行为 |
| 列投影 | 支持 | 支持，通过 legacy scan 的 required-columns 接口 |
| SQL WHERE 结果正确性 | 支持 | 支持；未下推的条件仍由 Spark 计算 |
| 过滤下推 | V2 Predicate | 有限 legacy Filter 下推，详见后表 |
| 分区谓词裁剪 | 默认扫描路径具有相关实现 | 3.2 批规划器当前没有传入分区谓词，不承诺同等扫描效率 |
| SQL LIMIT | 支持，下推可用 | 结果支持；不做源端 LIMIT 下推 |
| micro-batch 读取及 checkpoint 重启 | 支持 | 支持日志表、主键表，包含分区表共享测试及最终 JAR 恢复验收 |
| Structured Streaming 写入 | 支持 | 支持日志 append、主键 upsert；共享流写/重启测试在最终 JAR 上执行 |
| writer commit/abort | 共享 Fluss 生命周期 | 保留 flush、异常传播、资源关闭；epoch abort 不回滚已写数据 |
| CALL | 支持 | 保留共享解析和 `get_cluster_configs` 行为测试 |
| 时间范围 TVF | 支持 | 支持已测日志表/主键表、窗口参数及边界场景 |
| 常规类型转换 | 共享类型实现 | 复用转换测试；本地 TIMESTAMP 例外见下文 |
| `TIMESTAMP_LTZ` | 支持 | 支持；勿与 Fluss 不带时区的本地 TIMESTAMP 混淆 |
| Fluss 本地 `TIMESTAMP` / Spark `TimestampNTZType` | 3.4/3.5 可表达 | **不支持**；加载相关表给出明确异常，建议使用 TIMESTAMP_LTZ 或较新 Spark |
| ProcessingTime micro-batch | 支持 | 支持，恢复验收使用此触发方式 |
| AvailableNow | 新版 runtime 与默认 adapter 支持 | **不支持**，3.2 没有该 Trigger/API；本次不模拟该能力 |
| Continuous Processing | 未由当前 connector 实现 | 未新增支持 |
| `INSERT OVERWRITE`、SQL UPDATE/DELETE/MERGE | 不应由“主键 upsert”推导为支持 | 本次不新增支持；INSERT 到主键表不等于这些 SQL 操作 |
| Lake/Paimon/Iceberg 联合读取 | 主分支有独立 lake 模块/测试 | **未在 3.2 发布范围内验收**；不能由复用 common 推导为支持 |
| Scala 2.13 | 主分支有单独构建/CI 配置 | **不支持本内部 3.2 制品线**，仅 Scala 2.12 |
| 公开发布 / Maven Central / 官方支持矩阵 | 现有 3.4/3.5 线 | 3.2 为内部 profile 和资格检查，不改变公开矩阵、不执行发布 |

## 过滤和性能限制

3.2 只有同时满足 **非主键表、ARROW 日志格式、未启用 lake** 时才尝试记录批次过滤：

| 条件 | 3.2 下推 |
| --- | --- |
| `=`, `>`, `>=`, `<`, `<=` | 仅安全匹配的 STRING/CHAR、BOOLEAN、整数类型字面量 |
| null、不匹配/未知字面量、其他过滤表达式 | 保留 Spark 侧计算 |
| DATE、DECIMAL、TIMESTAMP、FLOAT、DOUBLE 谓词 | 当前不转换为 Fluss 下推条件；不代表相应数据都不可读写 |
| 主键表、非 ARROW 日志格式、lake 表 | 不进行此类 legacy 条件下推 |

即使接受下推，**全部过滤条件仍返回 Spark 作 residual 计算**。
原因是 Fluss 的该优化跳过记录批次，并非逐行精确过滤；不可去掉 Spark 的最终判断。
没有 V2 Predicate、源端 LIMIT 和分区谓词裁剪的性能保证，不宣称与 3.4/3.5 性能等价。

## 流式写与提交语义

本地官方 Spark JAR 的接口和 `MicroBatchWrite` 字节码核对结果：

| runtime | `StreamingWrite.useCommitCoordinator()` | Spark 实际策略 |
| --- | --- | --- |
| 3.2.4 | 接口没有此方法 | `MicroBatchWrite` 使用 `BatchWrite` 默认值，开启协调 |
| 3.4.3 | 接口同样没有此方法 | 同 3.2，开启协调 |
| 3.5.7 | 接口提供此方法 | 转调 Fluss 覆盖，返回 false |

共同实现中额外存在一个返回 false 的普通方法，不会让 3.2 调用缺失接口。
Fluss 的字节码没有对旧 Spark 缺失方法的调用，因此不需要人为增加空 adapter。
新增 adapter 也无法通过 3.2 不存在的接口关闭其协调器。

任务 `commit()` 会 flush 并检查异步错误；任务 `abort()` 关闭资源。
驱动端 epoch `commit/abort` 保持原有 no-op，writer factory 不把 epochId 用作 Fluss 去重键。
因此 **不能宣称事务回滚或端到端 exactly-once**。正常 checkpoint 重启已测试，
但失败后重放可能使日志表出现重复；主键 upsert 也不等于事务级 exactly-once。
这些是共享写入模型的边界，不是本次 3.2 新增的事务保障。

## 构建、测试与 CI

从仓库根目录使用 JDK 11；首次构建包含上游及共享 test JAR：

```bash
mvn -Pinternal-spark-3.2 -pl fluss-spark/fluss-spark-3.2 -am clean install -DskipTests
mvn -Pinternal-spark-3.2 -pl fluss-spark/fluss-spark-3.2 clean verify
```

内部 Maven 坐标为 `org.apache.fluss:fluss-spark-3.2_2.12:1.0-SNAPSHOT`，
构建产物为 `fluss-spark/fluss-spark-3.2/target/fluss-spark-3.2_2.12-1.0-SNAPSHOT.jar`。
使用匹配的 Apache Spark 3.2.4 / Scala 2.12 环境加载此 connector；不要同时加载
3.4/3.5 connector 或独立的 `fluss-spark-common`。示例会话配置如下，bootstrap 地址需替换：

```text
--jars <path>/fluss-spark-3.2_2.12-1.0-SNAPSHOT.jar
--conf spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.SparkCatalog
--conf spark.sql.catalog.fluss_catalog.bootstrap.servers=<host:port>
--conf spark.sql.defaultCatalog=fluss_catalog
--conf spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions
```

- `test`：普通共享/版本契约测试，不要求已有最终 JAR。
- `package`：组装只包含所需 Fluss payload 的 shaded JAR。
- `integration-test`：JUnit 桥接运行最终 JAR 的制品、批读写、micro-batch 恢复、共享流式读写及 writer 生命周期测试。
- `verify`：前述检查全部成功才通过。验收 classpath 用最终 JAR 替换 `target/classes`，
  排除独立 common/client JAR，并断言类来源；使用 classpath 避免 JPMS 同包测试隔离。
- 加 `-am` 会构建/测试上游；profile 本身不会自动排除其他版本模块，所以需配合 `-pl`。

独立工作流 [.github/workflows/internal-spark32.yaml](../../.github/workflows/internal-spark32.yaml)
支持手动触发及相关路径 PR/push 触发，不修改原有公开矩阵。
它构建上游、检查实际 test 依赖闭包，再执行 `clean verify`，上传依赖清单和测试报告。
[依赖检查器](../../tools/ci/check-spark32-dependencies.py) 要求 Spark JAR 全部为 3.2.4/_2.12，
Scala 核心版本与根 POM 一致，拒绝缺失/混用依赖与空报告。

```bash
python3 tools/ci/check-spark32-dependencies_test.py
```

本地验证与远端 Actions 是不同证据：工作流可以完成代码和本地命令验收，
但没有远端运行链接时，不把它描述为已在 GitHub Actions 成功发布或部署。

## 验证记录与 ticket

09、10 已完成实现和本地验收。最终同一次 `clean verify` 通过 **182/182** 个行为用例
（167 普通 + 15 最终 JAR）；3.4/3.5 writer 回归各 2/2，依赖检查器 7/7。
执行命令和完整验证结果记入 [implementation-notes.md](implementation-notes.md)。
Ticket 01–04 为被替代的历史方案；05–08 为先前完成范围，10 为流式写资格验证，
09 为持续构建/依赖/制品发布门禁。本次按 10 → 09 的依赖顺序验收。

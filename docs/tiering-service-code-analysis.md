# Tiering Service 代码分析

本文基于当前 checkout 的实现，说明 Fluss 到 Lake 的 Tiering Service 工作流程。这是一份代码路径分析，不是生产运维手册；运行时版本、存储凭据及部署侧的 classloader 行为仍应在目标环境验证。

## 1. 目标与边界

Tiering Service 是一个长期运行的 Flink 作业，使 Fluss 表在数据湖中的表示持续追赶 Fluss 数据。它不是由 Flink checkpoint 驱动的 sink：

- 读取 Fluss 表快照和日志；
- 向已配置的数据湖格式写入文件；
- 提交生成的数据湖快照；
- 将数据湖快照 id 和每个 bucket 的日志 offset 回写到 Fluss。

持久化进度的权威来源是 Fluss `LakeSnapshot` 元数据，而不是 Flink SourceReader 的 checkpoint state。因此，作业会强制使用 Flink 全局故障转移。参见
`fluss-flink-tiering/.../FlussLakeTiering.java` 与
`fluss-flink-common/.../tiering/committer/FlussTableLakeSnapshotCommitter.java`。

## 2. 端到端流程

```text
CLI 参数 / 配置提供器
        |
        v
FlussLakeTiering
        |
        v
LakeTieringJobBuilder
        |
        +-- TieringSource（Enumerator + Reader）
        |       |
        |       +-- Fluss Admin：选择表并生成 bucket split
        |       +-- Fluss LogScanner：读取快照 / WAL
        |       +-- LakeWriter：生成每个 bucket 的写入结果
        |
        v
TieringCommitOperator（parallelism 1）
        |
        +-- Iceberg/Paimon/... LakeCommitter：提交 lake snapshot
        +-- Fluss Coordinator：记录 snapshot id 和 bucket offset
        |
        v
Enumerator heartbeat：上报完成 / 失败 epoch，并请求下一轮工作
```

`TieringCommitOperator` 被刻意设置为单并发阶段。它必须聚合一张表的所有 bucket 结果，才能形成一个一致的数据湖提交。

## 3. 启动与配置

`FlussLakeTiering` 解析命令行参数，解析 `${provider:...}` 形式的配置值，并按前缀拆分配置：

| 输入 | 去向 |
| --- | --- |
| `--fluss.*` | Fluss client 配置；必须包含 `bootstrap.servers` |
| `--datalake.format` | 数据湖实现选择器 |
| `--datalake.<format>.*` | 传给所选数据湖实现 |
| `--lake.tiering.*` | Tiering Service 专用配置 |

它创建的 `StreamExecutionEnvironment` 使用全局故障转移和指数退避重启。代码中的原因很明确：区域级恢复可能只重启 committer；此时 committer 会丢失已收集的每 bucket 结果，导致该轮无法完成提交。

构建作业图前，会通过 `ServiceLoader` 加载可选的 `LakeTieringDecoratorPlugin`。内部部署因此能在不修改通用入口的情况下，添加环境专属的 Flink 设置。

## 4. 作业图构建与读取偏好

`LakeTieringJobBuilder` 依次执行：

1. 加载配置的 `LakeStoragePlugin`，并创建对应的 `LakeTieringFactory`。
2. 若用户没有明确配置，设置 `client.scanner.log.read-preference=remote-first`。
3. 构建无界的 `TieringSource`。
4. 将 source 以 parallelism 和 max parallelism 均为 1 的 `TieringCommitOperatorFactory` 相连。
5. 以 `DiscardingSink` 结束作业图；真正的输出是 committer 的副作用，而非下游记录。

`remote-first` 表示 Fluss server 优先读取 remote log segment，以减轻 TabletServer 本地存储读取压力。它并不是只读远端：当远端 manifest/segment 对请求 offset 尚未就绪时，server 会回退到本地日志。最新写入的数据通常会走此回退路径。

相关代码：

- `fluss-flink-common/.../tiering/LakeTieringJobBuilder.java`
- `fluss-client/.../table/scanner/log/LogFetcher.java`
- `fluss-server/.../replica/ReplicaManager.java`

## 5. Enumerator：选择表并创建一轮 Tiering

`TieringSourceEnumerator` 通过 lake-tiering heartbeat 向 Fluss Coordinator 注册。它启动后立即轮询，之后按 `tiering.poll.table.interval` 轮询（默认 30 秒）。Coordinator 的响应决定应处理的表和对应 epoch。

对选中的表，`TieringSplitGenerator` 会读取：

- 最新 Fluss `LakeSnapshot`，包括已提交的数据湖 snapshot id 和每 bucket offset；
- 当前 Fluss log end offset；
- 对主键表，读取最新 KV snapshot 元数据；
- 对分区表，读取分区和 bucket 拓扑。

随后生成一个或多个 `TieringSplit`。主键表的一个 bucket 可能需要先处理 snapshot 部分，再从 snapshot offset 开始读取 WAL，直到当前 end offset；非主键表可以直接下沉日志。之前的 LakeSnapshot 使新一轮成为增量处理，而不是从头回放。

split 在分配前会被打散以减少倾斜。source 可以将它们分配给多个 reader，但同一张表的所有结果最终都会汇聚到一个 committer。

## 6. Freshness 是一轮处理的截止时间，不是固定提交间隔

`table.datalake.freshness` 的文档定义为数据湖内容相对 Fluss 更新允许的最大滞后时间。Enumerator 对每一个已生成的表处理轮次，会将它具体用作定时器：

```text
一轮开始
  -> 所有 split 先完成：立即提交
  -> freshness 定时器先触发：标记表为强制结束
                              -> source 完成当前可用日志记录
                              -> 提交当前进度
```

因此，如 `30s` 的值并不表示“每 30 秒精确创建一个 Iceberg snapshot”。所有 bucket split 提前完成时，提交可能早于 30 秒；实际墙钟时间也可能更长，因为强制收尾、文件完成、catalog commit 和结果上报都需要额外时间。

freshness 是类似 SLO 的目标，也是一个公平性保护：慢表被强制结束，以免无限阻塞其他表。它本身不能证明最终 Iceberg snapshot 会在该时限内可见。

## 7. Reader 与 Writer 行为

`TieringSourceReader` 持有一个 `TieringSplitReader`。该 split reader：

1. 优先处理空 split；即使未写数据，也会产生进度结果。
2. 在需要时读取 snapshot 数据。
3. 通过 `LogScanner` 读取 WAL，使用 Arrow record batch 或行记录路径。
4. 每个 table bucket 使用一个 `LakeWriter`。
5. 完成 writer 后，发出 `TableBucketWriteResult`，其中包含写入结果、结束 offset、最大下沉时间戳、bucket 标识和本轮预期结果数。

Flink Web UI 中的 **Records Received/Sent** 计数，统计的是算子间的这些中间 `TableBucketWriteResult` 消息，而不是原始 Fluss 行数。例如，source 没有上游 Flink edge，因此可以显示 received records 为零；其下游 committer 收到的是该 source 发出的结果消息。

## 8. 提交屏障与协议

每收到一个 `TableBucketWriteResult`，`TieringCommitOperator` 都会按 table id 暂存它。只有已收集的不同 bucket 结果数量等于本轮预期结果数量时，才执行提交。同一 bucket 的重复结果会让本轮失败，而不会被静默接受。

提交协议顺序如下：

1. 保留所有 bucket 的 log end offset，包括没有产生数据但 offset 已推进的 bucket。
2. 从非空 writer result 构造数据湖格式专属的 committable。
3. 检查 Fluss 是否遗漏此前已提交的数据湖 snapshot；不安全时对齐或中止。
4. 要求 Fluss Coordinator 准备一个持久化 offsets 文件路径。
5. 使用该 offsets 文件路径作为 snapshot property，提交数据湖 snapshot。
6. 向 Fluss 提交数据湖 snapshot id、tiered/readable offset 与 tiered timestamp。
7. 向 Source Coordinator 发送成功事件；后者会在下一次 heartbeat 上报已完成 epoch。

当未产生 data file、但 offset 已推进时，可以提交空的数据湖 snapshot。否则同一个空 WAL 范围会被持续重新生成。

数据湖提交失败时，operator 会发送 `FailedTieringEvent`；Enumerator 将失败 epoch 上报 Fluss，后续轮次再尝试处理。如果一致性检查发现数据湖已领先于 Fluss 已知 snapshot，代码会在可能时重建遗漏状态，并中止不安全的新 committable。

## 9. Iceberg 专属写入与提交语义

`IcebergLakeWriter` 根据 Iceberg identifier field 选择 writer：

- 没有 identifier field：append-only task writer；
- 有 identifier field：delta task writer。

对带 identifier key 的表，delta 路径会生成 data file 和 delete file。Iceberg committer 只在没有 delete file 时使用 `AppendFiles`；否则使用 `RowDelta`，同时加入 data file 和 delete file。这解释了 metadata 中 `write.merge.mode`、`write.delete.mode`、`write.update.mode` 均为 `merge-on-read`，以及存在 equality delete 和 position delete file 的现象。

Iceberg 提交完成后，service 会将所得 snapshot id 记录到 Fluss。恢复时比较进度使用 Iceberg sequence number，而不是无序的 snapshot id。

## 10. 指标与运维解释

下列指标具有不同含义：

| 指标 | 含义 |
| --- | --- |
| Source records sent / committer records received | 每 bucket 的 Tiering 结果消息，不是源数据行 |
| Iceberg `added-records` | 该 snapshot data file 中写入的物理记录数 |
| Iceberg equality / position delete | 查询读取时需要应用的 merge-on-read 工作量 |
| Iceberg snapshot timestamp 间隔 | 已观察到的提交节奏，不是精确 freshness 度量 |
| Fluss LakeSnapshot offset/timestamp | 持久化进度，也是衡量数据湖滞后的更可靠依据 |

对高频 upsert 的 Iceberg 表，持续增加的 delete file 和大量小 data file 会扩大读取放大。Tiering Service 只会对 legacy schema 表运行内部 compaction；对于 clean-schema Iceberg 表，即使开启自动压缩也会记录警告，并要求外部 Iceberg compaction。

## 11. 恢复模型与重要限制

- Reader state 被刻意视为无状态；source 恢复时会查询 Fluss 中最新持久化 LakeSnapshot，并按记录的 offset 重建 split。
- Flink 作业使用全局故障转移，因为 committer 在内存中聚合一轮结果。
- Flink 作业成功提交不代表运行时 JAR 一定与集群的 Flink/Iceberg API 匹配；必须使用实际部署的 Flink build 验证 Tiering JAR。
- Maven dependency-convergence gate 校验 Maven 依赖图，但不能单独证明手工解包或 shade 合并后的 JAR 内容正确。
- `table.datalake.freshness` 不会清理旧 Iceberg snapshot、delete file 或 orphan file；仍需要独立的 Iceberg 维护策略。

## 12. 主要代码映射

| 职责 | 主要实现 |
| --- | --- |
| CLI 与 Flink 环境 | `fluss-flink-tiering/.../FlussLakeTiering.java` |
| 作业图与 remote-first 默认值 | `fluss-flink-common/.../tiering/LakeTieringJobBuilder.java` |
| Source 生命周期 | `fluss-flink-common/.../tiering/source/TieringSource.java` |
| Coordinator heartbeat 与 freshness 定时器 | `fluss-flink-common/.../tiering/source/enumerator/TieringSourceEnumerator.java` |
| 增量 split 生成 | `fluss-flink-common/.../tiering/source/split/TieringSplitGenerator.java` |
| Snapshot/WAL 扫描和 writer | `fluss-flink-common/.../tiering/source/TieringSplitReader.java` |
| 轮次聚合与两阶段协议 | `fluss-flink-common/.../tiering/committer/TieringCommitOperator.java` |
| Fluss LakeSnapshot 持久化 | `fluss-flink-common/.../tiering/committer/FlussTableLakeSnapshotCommitter.java` |
| Iceberg 文件写入器 | `fluss-lake-iceberg/.../tiering/IcebergLakeWriter.java` |
| Iceberg RowDelta/append 提交 | `fluss-lake-iceberg/.../tiering/IcebergLakeCommitter.java` |

## 13. 运行时日志实例：2026-09-17 TaskManager tiering

以下结论来自 `taskmanager.log (1).user`，时间范围为 2026-09-17 14:44:39 至
14:55:21。它覆盖表 `flink_on_k8s_test.prod_driver_status`（`tableId=0`）的
TieringSourceReader 和 Iceberg commit 日志；Coordinator 心跳和 Fluss
`LakeSnapshot` 回写的日志不在该文件中。

### 13.1 已观察到的实际链路

```text
10 个 bucket 的 TieringLogSplit
        -> RemoteLogDownloader 读取各 bucket 的 bounded WAL 区间
        -> Iceberg catalog 加载当前 metadata
        -> LakeWriter 完成并发出 TableBucketWriteResult
        -> TieringCommitOperator 汇集本轮结果
        -> Hive Metastore lock 校验
        -> Iceberg RowDelta snapshot 提交
        -> 下一轮从已提交的 bucket offset 继续
```

首个 split 例如 `tiering-log-split-0-5`，读取区间为
`[29959629, 30341041)`。日志中每个 split 的
`Subscribe to read log ...` 都由对应的 `Finish tier bucket ...` 收尾，且
RemoteLogDownloader 和 LogFetcher 随后正常关闭。

### 13.2 提交结果

在该 10 分 42 秒窗口内，Iceberg metadata 从版本 `00305` 连续推进至
`00317`，即成功提交了 12 个新 snapshot（sequence number `306` 至 `317`）。
日志没有 `FailedTieringEvent`、`Fail to commit tiering write result` 或 `ERROR`。

首轮成功提交 metadata `00306` / snapshot `6724478477066792805`：

- `addedDataFiles=8`；
- `addedRecords=1,412,595`；
- `addedEqualityDeleteFiles=8`、`addedPositionalDeleteFiles=8`。

末轮成功提交 metadata `00317` / snapshot `6241132464744129628`：

- `addedDataFiles=10`；
- `addedRecords=142,000`；
- `addedEqualityDeleteFiles=10`、`addedPositionalDeleteFiles=10`。

因此该实例证明了 source-side 扫描、LakeWriter 完成和 Iceberg 提交均已发生。
delete file 与 data file 同时增加也与主键/upsert 表的 Iceberg RowDelta 路径一致。

### 13.3 Freshness 到期时的受控部分提交

日志中有 6 次 `Received reach max duration for table 0`。这是 freshness 定时器
先于全部 bucket 完成时的受控路径，并非任务失败。

首轮在 14:45:09 到期时：

- bucket 3 原 stop offset 为 `30630597`，实际只推进到 `30373137`，随后被强制完成；
- bucket 6 和 9 尚未开始，标记 `skipCurrentRound`，以 empty split 结束；
- 该轮依然提交了 metadata `00306`，但只生成 8 个 data file。

后续日志显示其续跑行为：bucket 3 从 `30373137` 继续；bucket 6 仍从
`30084961` 开始；bucket 9 仍从 `30028830` 开始。也就是说，强制完成的
bucket 只提交已读取的 offset，跳过的 bucket 不会被虚假标记为已 tier。该机制
把已完成部分持久化，同时让未处理范围在下一轮重新生成 split，避免数据丢失。

### 13.4 非阻断告警

- `No metrics reporter configured`：没有配置 Fluss metrics exporter。
- `Tokens update task not started`：初始 security token 已发送；日志只表明 token
  没有声明自动续期时间。
- `hive.load.partition.update.cols.enable does not exist`：HiveConf 的未知配置项告警。

这些告警之后出现了多次成功的 Hive Metastore lock 校验和 Iceberg snapshot
提交，因此在该运行窗口内没有表现为 tiering 阻断问题。

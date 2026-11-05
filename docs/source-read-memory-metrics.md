<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source 读取缓冲与 Arrow 分配指标

这组指标用于区分读取数据量、Source 保留的数据量、Arrow allocator 分配记账量和 Flink 日志记录交接队列。它们不等于进程 RSS，也不应直接相加作为总内存。

## 指标定义

| 逻辑指标名 | 类型 / 单位 | 范围和含义 |
|---|---|---|
| `sourceFetchReceivedBytesTotal` | Counter / B | 累计接纳到本地 Fetch buffer 的编码 payload 字节；不含 RPC/TLS header、被忽略的过期响应或远端文件下载字节；重放/重试重新接纳的数据会再次计数 |
| `sourceFetchRetainedEncodedBytes` | Gauge / B | 已接纳且尚未 drain/discard 的本地编码 payload 长度；包括正在处理及排在远端 pending 后面的本地响应；从队列 poll 不会扣减 |
| `sourceBufferedFetches` | Gauge / 个 | 尚未 drain 的 CompletedFetch 数量，包括当前处理的响应；不是 Arrow RecordBatch 数或 RPC 在途数 |
| `sourceArrowAllocatedBytes` | Gauge / B | 当前 scanner 所有 local/remote、shaded/unshaded Arrow read allocator 的分配事件记账净值 |
| `sourceArrowPeakAllocatedBytes` | Gauge / B | 上述聚合在用记账量的生命周期高水位；在分配事件上更新，非各 allocator 独立峰值之和 |
| `sourceArrowAllocatedBytesTotal` | Counter / B | 累计成功的 Arrow 分配事件字节，可用于计算分配速率 |
| `sourceArrowReleasedBytesTotal` | Counter / B | 累计 Arrow 释放事件字节；释放不保证底层 native pool 立即归还操作系统 |
| `sourceQueuedRecords` | Gauge / 条 | 已物化日志 handover 包中尚待交接或丢弃的记录数；包括尚未放入队列和正在消费的包；不统计惰性 KV snapshot 或控制包 |
| `sourceQueuedBatches` | Gauge / 个 | 尚未 recycle 的非空日志 handover 包数；不是 Arrow RecordBatch 数；已交接所有记录但尚未 recycle 时可为 1 |

Fetch 三项位于 Fluss `client.scanner` 指标组。Arrow 四项也在该 scanner 组；Flink Source 通过已有 `FlinkMetricRegistry` 桥接注册到 Source operator 的 `fluss` 子组。两个 queued 指标直接位于 Flink SourceReader metric group。最终 OpenTSDB/Prometheus 名称取决于内部 Reporter 的 scope 展平、变量和白名单配置，不应直接把表中逻辑名假定为 Grafana metric 名。

## 内存与生命周期边界

- Fetch payload 是逻辑编码长度。多个响应共享一个 ByteBuf、retained slice 或容量取整时，它不等于底层实际分配容量。
- 远端日志 segment 是文件，文件长度不计入 `sourceFetchRetainedEncodedBytes`；远端 Arrow 解码分配仍由 Arrow listener 计量。
- Arrow 当前值与累计值是 allocator 分配/释放记账，包括该 allocator 负责的解码与 schema 投影分配，不包含 JVM 对象、整个 Netty pool 或 ChunkedFactory 空闲 chunk 的保留容量。
- Arrow 峰值在 scanner 生命周期内保留，scanner/任务重建后重新开始。当前分配归零不意味着 RSS 必然下降。
- `sourceQueuedRecords` 减少代表记录完成交接，不代表原 List 的对象引用已经释放；底层记录引用可能保留到 handover 包 recycle。
- Fetch drain 的终止通知按对象身份幂等处理。取消订阅和关闭释放所有持有项，包括已 poll 的响应；关闭尝试即使有某个资源失败也继续处理其余项，并传播关闭异常。关闭后到达的数据不再入队。
- 关闭后的计数归零不是“所有 native 内存释放成功”的证明；资源关闭异常仍需检查日志和 allocator/RSS 指标。
- queued 计数在 Reader.close 的 finally 清理；晚到的交接/recycle 不会造成负数。snapshot 路径的零值仅表示该指标未覆盖其惰性迭代器。

## 采集与验收

1. 替换旧 Client/Connector jar 并重启测试任务，避免同时加载新旧版本。无需变更表 schema、持久化格式或 RPC 协议。
2. 在 Flink Source operator 的 Metrics 列表或 REST metrics 端点搜索 `sourceFetch`、`sourceArrow`、`sourceQueued`，先确认注册成功。
3. 确认内部 Metrics Reporter 导出这些 Counter/Gauge，并保留任务、application、TM、operator/subtask 等定位维度。只修改 Grafana panel 不会生成数据。
4. Gauge 绘制当前值；BytesTotal counter 按数据源的 counter 规则计算速率，正确处理 scanner/任务重启；不要对 Gauge 取 rate。
5. 对比同样 64 列完整输入、相同吞吐、相同 TM/JVM/Fetch 配置下的全列和少列读取。采集启动阶段与共同稳态区间，不在同一 JVM 高水位历史上顺序切换后直接比较。
6. 将当前分配量、分配 B/s、Fetch 保留字节、queued 记录数与 Heap/Direct、GC、CPU、容器 RSS/Working Set 并排观察。不同指标不能简单相加。

聚合时，当前字节数可以按同一时间戳对独立 scanner/TM 求和。各 TM 峰值之和通常不是作业在同一时刻的峰值；作业峰值应先按时间对齐并汇总当前值，再求最大值。

Java 对象按 Source 调用栈的分配速率仍建议通过 JFR/allocation profiler 采样；本次没有把整个 Fetch 或 mailbox 线程的分配量错误标成 Source 独占分配量。

## 本次验证与测试包

使用 JDK11 和系统 Maven，Common、Client、Flink-common 共 84 个定向用例通过；内部 Flink 1.18 指标上报集成用例另通过 1 个。一个投影集成方法因项目已有 `@MultiVersionTest` 门禁跳过，没有计入通过数。格式、Checkstyle 与 `git diff --check` 通过。

测试包位于 `fluss-flink/fluss-flink-1.18/target/fluss-flink-1.18-1.0-SNAPSHOT.jar`，使用已有 `internal-flink118-iceberg16` profile 构建，包含当前工作区原有改动。完整命令、红绿测试证据与校验值见 `.scratch/source-read-memory-metrics/evidence.md`。

## 代码位置

- `fluss-client/.../log/LogFetchBuffer.java`：本地编码 payload 和 CompletedFetch 的持有责任。
- `fluss-client/.../log/CompletedFetch.java`：drain 生命周期通知。
- `fluss-client/.../metrics/ScannerMetricGroup.java`：Counter/Gauge 注册与聚合 Arrow AllocationListener。
- `fluss-common/.../record/LogRecordReadContext.java`：local/remote 读取 allocator、unshaded API listener 桥接。
- `fluss-flink-common/.../FlinkRecordsWithSplitIds.java`：逐条交接和幂等 recycle。
- `fluss-flink-common/.../FlinkSourceReaderMetrics.java`：handover 计数及终止清理。

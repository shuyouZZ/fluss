<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file to You under
 the Apache License, Version 2.0 (the "License"); you may not use this file
 except in compliance with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 License for the specific language governing permissions and limitations
 under the License.
-->

# Fluss 生产上线准备

本文定义 Fluss 接入生产前必须具备的能力、配置基线和验收证据。它覆盖 Fluss、Flink、
Iceberg 及其依赖系统之间的运行边界；不替代各组件的安装手册。

本文使用以下术语：

- **业务作业**：读取或写入业务数据的 Flink 作业。
- **Tiering 作业**：由 `fluss-flink-tiering` 启动的、将启用湖存储的 Fluss 表下沉到
  Lakehouse 的常驻 Flink System Job。
- **远端存储**：Fluss 用于 KV 快照及日志分层的文件系统路径，与 Iceberg Warehouse 不是同一概念。
- **验收证据**：配置快照、作业与集群状态、压测或故障演练记录；没有证据的项目不能标记为完成。

## 1. 上线范围与验收原则

### 1.1 先确定的边界

每个生产集群上线前必须记录以下内容：

| 项目 | 必须明确的内容 |
| --- | --- |
| 工作负载 | 日志表或主键表、写入峰值和平均吞吐、读写并发、最大消息大小、分区策略。 |
| 数据目标 | 热数据可读时长、远端存储保留期、Iceberg 历史数据保留期、允许的数据新鲜度。 |
| 可用性 | 可容忍的节点、rack 或可用区故障数量；对应的副本数与最小同步副本策略。 |
| 恢复目标 | RPO、RTO、负责人、恢复所依赖的 ZooKeeper、远端存储、Iceberg Catalog 和 Flink 平台。 |
| 所有权 | Fluss 集群、表 DDL、Tiering 作业、Iceberg 表、密钥、告警和故障处置的唯一负责人。 |

### 1.2 验收规则

- 生产默认值不能沿用发行包示例。当前示例 `server.yaml` 的 `data.dir`、
  `remote.data.dir`、bucket 数和副本数均是本地开发值。
- 所有可恢复性结论必须通过一次实际演练验证，不能只依据副本数或配置推断。
- 任何例外必须写明风险、到期日期和批准人；没有例外记录即表示满足本章全部门槛。
- 本文中的“必须”是上线门槛；“建议”是实现门槛后用于提升容量、成本或运维质量的做法。

## 2. 目标架构与职责边界

### 2.1 推荐拓扑

```text
业务 Flink 作业 ── Fluss connector ──> Fluss 表
                                        │
                                        ├──> TabletServer 本地数据
                                        ├──> 远端存储：KV 快照 / 分层日志
                                        └──> Tiering System Job ──> Iceberg Catalog + Warehouse

Flink、Spark、Trino 等查询引擎 ────────> Fluss 或 Iceberg
```

| 组件 | 生产职责 | 不应承担的职责 |
| --- | --- | --- |
| ZooKeeper | Coordinator 选主、集群协调和元数据服务依赖。 | 保存业务数据或替代备份。 |
| CoordinatorServer | 表、节点、副本、分层和集群控制面。 | 承载业务数据副本。 |
| TabletServer | 承载 Log/KV tablet、复制和本地读写。 | 共享临时目录或依赖易失本地盘完成持久化。 |
| 业务 Flink 作业 | 业务转换、写入 Fluss，或业务定义的外部 Sink。 | 管理 Fluss 原生 Tiering 状态。 |
| Tiering 作业 | 将启用 lakehouse 的 Fluss 表持续写入受管 Iceberg 表。 | 与业务作业并发写同一 Iceberg 表。 |
| Iceberg Catalog/Warehouse | 表元数据和历史数据文件的持久化。 | 代替 Fluss 热数据路径或由多个所有者修改受管表。 |

### 2.2 Iceberg 接入模式：必须二选一

**模式 A：Fluss 原生 Tiering。** 适用于需要 Fluss 热数据与 Iceberg 历史数据联读、
或需要由 Fluss 管理下沉位点的表。表开启 `table.datalake.enabled` 后，由独立 Tiering
作业负责下沉。

对 Iceberg，当前代码只允许使用 Fluss 的数据库和表名作为默认映射；
`table.datalake.database-name` 与 `table.datalake.table-name` 的自定义映射目前仅支持
Paimon。因此 `pricing_test.pricing_request` 不能通过原生 Iceberg Tiering 映射为任意的
`flink_test.test_explain_table_pricing_request`。

**模式 B：业务 Flink Iceberg Sink。** 适用于目标 Iceberg 表名、schema、分区、写入策略
或变换逻辑由业务定义的场景。该作业必须拥有其目标 Iceberg 表，并以自己的 checkpoint、
重启和告警策略运行。

同一 Iceberg 表只能选择一个写入所有者。原生 Tiering 表不得再被业务 Sink、批处理或维护
任务直接写入；schema 变更由 Fluss 表 DDL 发起并经受管链路同步。

### 2.3 已确认的实现边界

- `LakeTieringJobBuilder` 创建 Tiering source 和 committer；committer 固定单并行度，
  因此扩容前要压测提交吞吐，而非只提高 source 并行度。
- `FlussLakeTiering` 将 failover 策略设为 `full`，并设置指数退避重启；其注释明确说明
  Tiering 位点保存在 Fluss Server 的 LakeSnapshot，而非 Flink checkpoint state。
- 业务 Flink connector 的 source/sink 与 Tiering job 是不同的模块和作业生命周期，
  平台不应将 Tiering 嵌入用户 SQL 作业。

## 3. 高可用、复制与故障域

### 3.1 控制面

- 使用独立的 ZooKeeper ensemble；生产基线为 3 个节点，跨故障域部署并纳入备份、监控和
  升级流程。
- 至少部署一个 active 和一个 standby CoordinatorServer，全部指向同一个 ZooKeeper
  地址和集群根路径。Coordinator 故障切换期间，管理操作可能短暂不可用；上线演练需验证
  客户端重连和控制面恢复时间。
- `zookeeper.address`、listener 地址、advertised listener 和 DNS/VIP 必须由平台统一
  管理；客户端 `bootstrap.servers` 至少包含可持续发现的入口，而非单一易失节点。

### 3.2 副本与写入持久性

| 项目 | 基线 | 验收要求 |
| --- | --- | --- |
| 可用 TabletServer | 不少于表的副本数。 | 建表前确认 live TabletServer 数量；副本数大于节点数会被拒绝。 |
| 日志副本 | 关键日志表建议 3 副本。 | 每个副本分布在独立故障域；容量模型按所有副本计算。 |
| 确认策略 | 关键写入使用 `client.writer.acks = all (-1)`。 | 与 `log.replica.min-in-sync-replicas-number = 2` 配套压测。 |
| 默认值 | 显式设置集群默认 bucket 与副本数。 | 不允许依赖示例配置中的 `1`。 |

副本数只决定副本数量，不自动保证写入在足够多副本持久化。`acks = all` 与
`log.replica.min-in-sync-replicas-number` 共同决定写入成功所需的确认数；代码示例给出的
典型组合是 3 副本、最小同步副本 2。

### 3.3 `tablet-server.rack`

`tablet-server.rack` 是每台 TabletServer 的静态故障域标签。优先编码可用区；若没有跨
可用区拓扑，再编码物理机架。例如：

```yaml
# 每台 TabletServer 使用各自的 server.yaml
tablet-server.id: 0
tablet-server.rack: cn-hz-az-a
```

- 标签必须来自 CMDB、Kubernetes node label 或机房拓扑，不可使用临时 hostname、Pod 名称
  或人为伪造的 rack。
- 生产推荐 `3 个 rack/AZ × 每 rack 至少 2 台 TabletServer`。每个 rack 的节点数尽量相等，
  否则副本和磁盘负载会向节点更少的 rack 倾斜。
- 3 副本但只有 2 个 rack 时，最多只能跨 2 个 rack 分布，仍有两个副本处于同一 rack；
  这不是完整的单 rack 故障隔离。
- 修改 rack 标签、扩容或下线节点后，需要在低峰执行 rebalance。推荐目标顺序：

```sql
CALL sys.rebalance('RACK_AWARE,REPLICA_DISTRIBUTION,LEADER_DISTRIBUTION');
CALL sys.list_rebalance();
```

`RACK_AWARE` 必须排在首位。rebalance 会复制数据并消耗网络、磁盘和副本带宽；开始前应
确认没有其他 rebalance、预留容量，并设置停止条件。

### 3.4 版本风险

当前工作区的 snapshot 代码在所有 live TabletServer 都报告 rack 时才启用 rack-aware
assignment；滚动配置期间会回退为 rack-unaware assignment。部署中的 0.9.1 发行制品应以
其对应文档和测试结果为准：全量配置每台 TabletServer，并在生产变更前验证混合配置期间的
实际行为。不能把 snapshot 行为直接当作 0.9.1 的上线承诺。

## 4. 存储、容量与数据生命周期

### 4.1 存储职责与最低要求

| 层 | 内容 | 上线要求 |
| --- | --- | --- |
| `data.dir` / `data.dirs` | TabletServer 本地日志、KV、checkpoint 和节点本地文件。 | 使用持久化、高 IOPS、独占磁盘；禁止 `/tmp`、容器临时层和无性能承诺的共享盘。 |
| `remote.data.dir` / `remote.data.dirs` | KV 快照与日志分层文件。 | 使用 HDFS 或对象存储等具备持久性、权限控制和恢复能力的远端文件系统。 |
| Iceberg Warehouse | Iceberg 数据文件与元数据文件。 | Catalog、FileIO 和 Warehouse 的权限、HA、备份、生命周期策略独立验收。 |

新集群优先配置 `remote.data.dirs`；代码将新建表或分区分配到其中一个路径，默认策略为
round-robin。若使用 `WEIGHTED_ROUND_ROBIN`，权重数量必须与路径数量一致，且总和大于零。

### 4.2 容量模型

容量评估至少覆盖以下公式，并以压测测得的压缩比和写放大替换估算值：

```text
本地热数据 = 峰值写入字节/秒 × 本地保留秒数 × 副本数 × 压缩后比例 × 安全余量
远端数据   = 平均写入字节/秒 × 远端保留秒数 × 压缩后比例 × 安全余量
```

- 本地和远端安全余量应覆盖 rebalance、故障副本重建、compaction、写入尖峰与扩容窗口；
  阈值由实际磁盘与网络基准确定，不在本文固定。
- `bucket.num` 决定表的并行分桶。它应由吞吐、key 分布、TabletServer 数和 Flink 并行度
  共同决定；bucket 过少会热点，过多会增加小文件、元数据、复制和调度成本。
- 分区表要将分区数、预创建数和保留数纳入容量及元数据规模估算，避免无界分区。

### 4.3 生命周期

- `table.log.ttl` 控制日志段的最长保留期，默认值为 7 天；`0ms` 关闭 TTL 删除。
- 远端日志分层时，`table.log.local-ttl` 控制本地副本删除时机；过期本地段仅会在成功复制到
  远端后删除，且其值不得大于 `table.log.ttl`。
- 启用 Iceberg Tiering 的表，应将 `table.datalake.freshness` 作为业务可见的新鲜度 SLO；
  默认是 3 分钟，延长该值可降低下沉成本但扩大历史数据滞后。
- Iceberg 的快照过期和文件 compaction 必须有明确所有者和保留策略。自动 snapshot 过期或
  compaction 默认关闭，开启前先验证查询、回溯与恢复需求。

## 5. 运行时兼容性与制品管理

### 5.1 版本矩阵

每个环境必须维护不可变的版本矩阵，至少包含：

| 类别 | 必填项 |
| --- | --- |
| Fluss | Server、Flink connector、filesystem plugin、Iceberg lake plugin、Tiering JAR 的同一发行版本和构建标识。 |
| Flink | 运行时版本、Scala 版本、JDK 版本、JobManager/TaskManager 镜像版本、SQL Gateway 或平台 executor 版本。 |
| Iceberg | Iceberg API/runtime、Catalog 实现、FileIO、Hadoop/Hive 依赖及其冲突解决结果。 |
| 存储 | HDFS client、对象存储 SDK、Kerberos/STS 机制及目标集群版本。 |

当前仓库的 `internal-flink118-iceberg16` profile 绑定了 Flink 1.18.2-145 和
`dream-1.6.1-135`。用户当前 SQL 使用的是 `dream-1.6.1-138-test`；二者不能仅因主版本
相同而视为兼容。上线前必须产出与实际平台完全一致的 Fluss、Tiering 与 Iceberg 依赖树，
并在目标 Flink 集群做提交、重启、读写和下沉验证。

### 5.2 Classpath 规则

- Fluss Server 发行包通过 `plugins/iceberg/` 加载 Iceberg lake plugin；生产包应验证该
  plugin provider 存在，而不是只验证 JAR 文件名。
- 原生 Tiering 作业是独立 JAR 作业。Fluss connector、匹配的 filesystem plugin、
  `fluss-lake-iceberg`、Iceberg Catalog/FileIO 和 Hadoop 依赖必须在每个 JobManager 与
  TaskManager 的一致运行时 classpath 中。
- 不要将 `ADD JAR` 用作生产 Tiering 依赖管理。它只改变当前 SQL 会话，不能保证每个
  TaskManager、作业重启和平台隔离 classloader 的一致性。
- 制品发布必须包含 checksum、依赖树、镜像 digest 和回滚到上一已验证版本的路径。

### 5.3 验收

在预发环境运行一次最小闭环：创建 Fluss 表、写入、读取、故障重启、Tiering、Iceberg 查询
和 Union Read。任何 `ClassNotFoundException`、`NoSuchMethodError`、ServiceLoader provider
缺失或 Hadoop/FileIO 初始化错误都应被视为版本矩阵不通过，而非线上临时补 JAR。

## 6. Flink 与 Iceberg 接入模式

### 6.1 业务 Flink 作业

- 每个作业使用服务账号连接 Fluss，`bootstrap.servers` 使用平台维护的稳定入口。
- 并行度、checkpoint、restart、状态后端和资源限额由业务作业所有者配置；它们不由 Fluss
  Tiering 作业继承。
- DDL 必须显式指定影响容量和可用性的表选项，至少包括 bucket、主键/分区策略、日志保留和
  lakehouse 开关。`CREATE TABLE IF NOT EXISTS` 不会修改既有表配置；`ALTER TABLE` 仅可
  修改 Fluss 支持动态变更的选项，变更必须有记录。
- 业务侧 Iceberg Sink 需要验证其 checkpoint 与 Iceberg commit 语义，且不得写入由 Fluss
  原生 Tiering 管理的表。

### 6.2 原生 Tiering 配置与提交

在 Fluss 集群配置中定义 Iceberg Catalog；以下以 Hive Catalog 为例，真实地址和凭据必须由
平台配置中心或密钥引用提供：

```yaml
datalake.enabled: true
datalake.format: iceberg
datalake.iceberg.type: hive
datalake.iceberg.uri: thrift://<hms-host>:<port>
datalake.iceberg.warehouse: hdfs://<nameservice>/<warehouse>
```

新表应在创建时预先固定 lake format，再启用下沉：

```sql
CREATE TABLE pricing_request (
  -- columns omitted
) WITH (
  'bucket.num' = '10',
  'table.datalake.format' = 'iceberg',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '3 min'
);
```

`table.datalake.format` 不在当前代码的可动态修改列表中。既有表仅可在其已预设正确
lake format（或创建时继承正确的集群 `datalake.format`）的前提下，通过下列方式启用：

```sql
ALTER TABLE pricing_request SET (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '3 min'
);
```

若既有表没有可验证的 Iceberg format，先在预发验证迁移或重建方案；不要直接把 format
动态切换到 Iceberg。

由内部平台单独提交常驻 System Job：

```text
主 JAR:    fluss-flink-tiering-<verified-version>.jar
主类:      org.apache.fluss.flink.tiering.FlussLakeTieringEntrypoint
关键参数:  --fluss.bootstrap.servers <bootstrap>
           --datalake.format iceberg
           --datalake.iceberg.type <catalog-type>
           --datalake.iceberg.<catalog/fileio option> <value>
```

- Tiering 作业应拥有独立名称、资源队列、自动重启、日志、告警和发布记录。
- 初始部署使用每个 `Fluss 集群 × Iceberg 配置` 一条 Tiering 作业；扩容优先提高经过压测的
  source 并行度。运行多个 Tiering 作业前，必须验证平台版本上的并发协调、Iceberg commit
  吞吐和故障恢复。
- Fluss 集群配置与 Tiering 参数应从同一配置源渲染，防止 Catalog、Warehouse 或安全配置不一致。

### 6.3 Iceberg 表治理

- 明确 Catalog、Warehouse、namespace、表所有者、schema 变更人和文件维护人。
- 对原生 Tiering 表，按 Fluss 表进行 schema 演进；禁止外部引擎直接改写其 schema 或提交
  数据文件。
- 外部查询引擎访问 Iceberg 时使用只读身份，并验证它的 Catalog/FileIO 参数与 Tiering 配置
  完全一致。

## 7. 安全与权限

### 7.1 Fluss 访问控制

- 默认 `authorizer.enabled` 为 `false`；生产必须显式启用，并使用内置或经审核的 authorizer。
- 为平台管理员、业务 writer、业务 reader、Tiering 作业和运维工具创建独立 principal。
  权限按 cluster、database、table 和操作类型最小化授权；不要让业务账号成为 super user。
- 若启用 SASL/PLAIN，客户端与服务端参数、listener 映射和 JAAS/凭据来源必须一起验收。认证
  不等于传输加密；网络隔离或传输加密要求由企业网络与合规方案确认。

### 7.2 凭据与密钥

- 禁止在 SQL、JAR 参数、镜像层、Git、Flink 日志或告警标签中放置 HDFS、Iceberg Catalog、
  对象存储或 SASL 明文密码。
- 使用 Fluss Config Provider 的 `env`、`directory` 或 `file` 引用，限制可读取路径；平台负责
  将 Secret 以环境变量或挂载文件注入。
- HDFS/Kerberos、对象存储角色、Hive Metastore/Iceberg REST 账号的权限分别验证：Fluss Server、
  Tiering Job、业务 Flink Job 和只读查询引擎不能共享超权限账号。

### 7.3 审计与网络

- 记录 DDL、ACL、配置、服务账号和 Iceberg 元数据变更的操作者与时间。
- 将 client listener、内部 server listener、ZooKeeper、Catalog 和存储端点置于明确的网络
  边界；仅开放实际需要的方向与端口。
- 密钥轮换必须先在预发验证，并包含 Fluss、Flink、Tiering、Catalog 与远端存储的无中断或
  可恢复步骤。

## 8. 可观测性、告警与服务目标

本节与 [Fluss 指标体系与内部观测接入](metrics-system.md) 配套使用；本节定义上线门槛，
不重复完整指标字典。

### 8.1 必需的观测面

| 面 | 必须采集 | 最小告警 |
| --- | --- | --- |
| Fluss 控制面 | Coordinator 存活、active coordinator、事件队列、表和 bucket 数。 | Coordinator 缺失、控制面不可用。 |
| 副本与节点 | active TabletServer、`underReplicated`、`underMinIsr`、`offlineBucketCount`、leader/replica 分布。 | 任一关键表副本不足；最小同步副本不足立即升级。 |
| 容量与性能 | `diskUsageRatio`、本地/远端大小、写入/复制吞吐、flush 和 RocksDB stall。 | 容量趋势越界、持续背压、复制延迟异常。 |
| Flink | Job/TaskManager 存活、失败、重启、checkpoint、反压与延迟。 | 业务或 Tiering 作业失败、连续重启、checkpoint 失败。 |
| Tiering | `tierLag`、`freshness`、`tieredTimestamp`、`failuresTotal`、pending/running table。 | 超出表级新鲜度 SLO、下沉失败增加。 |
| 外部依赖 | ZooKeeper、HDFS/对象存储、Iceberg Catalog、Warehouse API/元数据延迟与错误。 | 依赖不可用、权限失败、提交失败。 |

### 8.2 SLO 与告警原则

- 对每张关键表定义写入可用性、读取可用性、数据新鲜度和恢复时间目标；`tierLag` 不能使用
  集群统一的固定阈值替代业务 SLO。
- 告警应带 `cluster_id`、环境、database、table、server_id 和 rack 等有限标签。禁止使用
  request ID、driver ID 等无界业务字段作为标签。
- Dashboard 分为 Fluss 存储、业务 Flink、Tiering 和 Iceberg 外部依赖四层；关联而不混淆。
- 上线前必须触发并验证一次节点下线、副本不足、Tiering 失败和磁盘阈值告警。

## 9. 变更、扩缩容与故障演练

### 9.1 变更流程

每次生产变更均应包含：变更前健康检查、容量预留、回滚判据、变更窗口、负责人和观察窗口。

| 变更 | 必须步骤 |
| --- | --- |
| 新增 TabletServer | 注入唯一 server ID 和真实 rack；验证加入集群；低峰 rebalance；确认 leader/replica 均衡。 |
| 下线或维护 TabletServer | 先标记 `TEMPORARY_OFFLINE` 或 `PERMANENT_OFFLINE`，完成 rebalance 后再停止；禁止直接删除承载副本的节点。 |
| 修改 rack | 作为拓扑变更执行：全量标签校验、受控重启、rack-aware rebalance、验证每个 bucket 的副本分布。 |
| 升级 Fluss/Flink/Iceberg | 按版本矩阵预发验证；先升级兼容的依赖和客户端，再做 schema 或格式变更；保留回滚制品与数据兼容性证据。 |
| 修改 Tiering 配置 | 先验证 Catalog/warehouse 连通与权限；将 server 配置和 Tiering job 参数一起发布；观察新鲜度、失败数和 Iceberg commit。 |

Fluss 同一时刻只允许一个 active rebalance。取消 rebalance 不会撤销已完成的数据迁移，因而
停止条件和容量保护必须在开始前定义。

### 9.2 上线前演练

至少完成以下演练，并为每项记录数据范围、开始/恢复时间、告警和验证查询：

1. 单 TabletServer 故障：写入与读取按副本和 ISR 策略表现符合预期；副本恢复完成。
2. 单 rack/可用区故障：验证副本实际跨 rack，确认剩余容量足以承载负载。
3. Coordinator 故障切换：客户端、管理操作和监控恢复符合 RTO。
4. ZooKeeper 连接异常：确认控制面告警、恢复路径和数据面影响边界。
5. 远端存储不可用：确认本地保护窗口、告警、写入/读取降级与恢复程序。
6. Tiering 作业失败与重启：确认 LakeSnapshot 位点恢复、Iceberg 无重复或缺失提交，并恢复到
   新鲜度 SLO。
7. Iceberg Catalog 或 Warehouse 权限失败：确认快速失败、告警、密钥回滚与待下沉数据恢复。

### 9.3 备份与恢复

- ZooKeeper 元数据、远端存储和 Iceberg Catalog 元数据均要按所属系统的恢复机制备份，且在
  隔离环境实际恢复。
- 不要把本地 TabletServer 磁盘副本视为唯一备份。备份保留期必须覆盖发现数据问题和恢复演练
  所需的时间窗。
- 恢复 Runbook 应明确依赖恢复顺序、权限、配置版本、Flink/Tiering 作业重提步骤和数据校验
  查询。

## 10. 上线检查表

| 门槛 | 验收证据 | 状态 | 负责人 |
| --- | --- | --- | --- |
| 工作负载、RPO/RTO、数据保留和 Iceberg 所有权已确认 | 架构评审记录 | 待填写 | 待填写 |
| ZooKeeper、Coordinator 与 TabletServer 拓扑满足目标故障域 | 集群清单、rack 分布、故障演练 | 待填写 | 待填写 |
| 副本、ISR、acks 和关键表默认值已压测 | 写入/故障压测报告 | 待填写 | 待填写 |
| 本地盘、远端存储和 Warehouse 的容量及生命周期已评估 | 容量模型、配额和保留策略 | 待填写 | 待填写 |
| Flink 1.18、Fluss、Iceberg、Hadoop 与插件制品完全锁定 | 依赖树、镜像 digest、checksum | 待填写 | 待填写 |
| 原生 Tiering 或业务 Sink 的模式已选择，且目标 Iceberg 表只有一个写入者 | 表所有权清单、作业定义 | 待填写 | 待填写 |
| Tiering 作业已由平台独立托管并完成失败恢复验证 | 作业配置、重启演练、Iceberg 校验 | 待填写 | 待填写 |
| ACL、存储权限、密钥注入和轮换流程已验证 | 最小权限与轮换测试记录 | 待填写 | 待填写 |
| Fluss、Flink、Tiering 和外部依赖的告警已触发验证 | Dashboard、告警测试与值班 Runbook | 待填写 | 待填写 |
| 扩缩容、rebalance、回滚和恢复演练已完成 | 演练报告与恢复查询结果 | 待填写 | 待填写 |

## 11. 已知限制与待确认项

| 项目 | 状态 | 处理要求 |
| --- | --- | --- |
| 内部 Iceberg 制品版本 | **待确认**：仓库 profile 是 `dream-1.6.1-135`，现有 SQL 使用 `dream-1.6.1-138-test`。 | 生成并验证与生产平台一致的完整依赖树；未完成前不得上线原生 Tiering。 |
| 原生 Iceberg 目标表重命名 | **已确认不支持任意映射**：当前代码仅对 Paimon 支持 datalake database/table name override。 | 如需不同名称或不同 schema，使用独立业务 Iceberg Sink。 |
| 0.9.1 rack 滚动变更行为 | **待确认**：当前 checkout 是 `1.0-SNAPSHOT`，不可代替部署的 0.9.1 事实。 | 用部署制品在预发验证所有节点已配置、混合配置和 rebalance 行为。 |
| 内部数据平台提交能力 | **待确认**：平台是否支持独立 System Job、运行时依赖注入、Secret 引用、自动重启和资源隔离。 | 以本文件第 6.2 节作业契约进行平台联调。 |

## 12. 证据索引

以下为本文的仓库代码事实来源；部署行为仍需以实际发行制品和预发验证为准。

- 默认 bucket、副本、远端目录、日志保留和 Lakehouse 配置：
  `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`。
- 远端目录与服务端配置校验：
  `fluss-common/src/main/java/org/apache/fluss/config/FlussConfigUtils.java`。
- rack 感知分配与副本数校验：
  `fluss-server/src/main/java/org/apache/fluss/server/utils/TableAssignmentUtils.java`、
  `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java`。
- Tiering 作业入口、重启策略和 LakeSnapshot 位点边界：
  `fluss-flink/fluss-flink-tiering/src/main/java/org/apache/fluss/flink/tiering/FlussLakeTiering.java`、
  `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/LakeTieringJobBuilder.java`。
- 内部 Flink 1.18 / Iceberg 1.6 profile 与 Iceberg plugin 包装：`pom.xml`、
  `fluss-dist-internal/pom.xml`、`fluss-dist-internal/src/main/assemblies/bin.xml`。
- Fluss 指标、报警与内部接入：`docs/metrics-system.md`。

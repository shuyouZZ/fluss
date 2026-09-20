# Fluss + Flink 1.18 + Iceberg 1.6 环境与任务运行手册

本手册对应本目录 `spec.md` 已锁定的内部矩阵：选定 Fluss 基线、内部 Flink
`1.18.2-SNAPSHOT` 和 Iceberg `1.6.1`。三者是一个发布单元：不得将上游
Flink 1.20 / Iceberg 1.10.x 的连接器、Tiering JAR 或依赖混入该环境。

## 运行拓扑

```text
业务 Flink 1.18 作业 ── Fluss connector ──> Fluss Coordinator/TabletServer
                                                 │
独立 Flink 1.18 Tiering 集群 ─ Tiering job ──────┘
                                                 │
                                    Iceberg Catalog + FileIO + Warehouse
```

Fluss 负责低延迟的热数据；Tiering Service 是独立的、无状态的 Flink 作业，持续把
已启用的 Fluss 表写入 Iceberg。业务作业和 Tiering 作业必须分开部署，避免业务作业的
依赖污染 Tiering 的 Iceberg classpath。

## 前置环境

- Linux/macOS、Java 17（最低 Java 11；本项目文档推荐 Java 17+）。所有 Fluss
  服务进程和两个 Flink 集群使用同一主版本 Java。
- ZooKeeper（本分支的 Fluss 部署仍需要），一个 CoordinatorServer，至少一个
  TabletServer。
- 内部 Flink `1.18.2-SNAPSHOT` standalone / Kubernetes 集群各一套：业务集群与
  Tiering 集群分离。
- Iceberg 1.6.1 的 Catalog（内部实现、JDBC、Hive、REST 等之一）和 FileIO；两者必须
  对 Fluss 服务进程与 Tiering TaskManager 都可见。
- 可写 warehouse（S3/OSS/HDFS 等）及 Fluss remote storage。生产环境的访问密钥由
  secret manager / workload identity 注入，不写进 `server.yaml` 或作业参数。

## JAR 分层与放置位置

| 进程 | 必须存在的 JAR | 放置位置 |
| --- | --- | --- |
| CoordinatorServer、TabletServer | 内部 `fluss-lake-iceberg` 1.6.1 适配器闭包；内部 Catalog、FileIO、存储和认证实现；Hadoop client（如 Catalog/FileIO 需要） | `${FLUSS_HOME}/plugins/iceberg/` |
| 业务 Flink 1.18 | **仅一个**内部 `fluss-flink-1.18` connector；如读 union/lake，则加入同一 Iceberg/Catalog/FileIO 闭包；Fluss remote storage 对应 filesystem JAR | `${FLINK_HOME}/lib/` |
| Tiering Flink 1.18 | **仅一个**内部 `fluss-flink-1.18` connector、内部 `fluss-lake-iceberg`、内部 `fluss-flink-tiering` 作业；与服务端完全相同的 Catalog/FileIO/Hadoop/存储依赖 | connector/adapter 置 `${FLINK_HOME}/lib/`；tiering JAR 由 `flink run` 提交 |

不要在 Tiering 集群默认加入 `iceberg-flink-runtime`。只有需要在这套专用集群执行原生
Iceberg SQL 时，才将其作为独立兼容性决策；否则它容易与 Tiering 所持有的 Iceberg 1.6.1
产生重复类或 ServiceLoader 冲突。

启动前在每台机器检查：依赖树中没有 Iceberg 1.10.x 和 Flink 1.20.x；每种
`fluss-*` connector/adapter 只有一个版本；`META-INF/services` 中 Iceberg Catalog、
FileIO 与 Fluss lake plugin 的 provider 资源未被覆盖。

### 内部 Catalog、FileIO、存储与认证的实际装配方式

“内部实现”不是 Fluss 自带的一项开关，而是平台要提供的一组 Iceberg 运行时类。每个内部
Catalog 必须实现 Iceberg `org.apache.iceberg.catalog.Catalog`，每个内部 FileIO 必须实现
`org.apache.iceberg.io.FileIO`，并且都以本矩阵的 `dream-1.6.1-135` Iceberg API 编译。
Fluss 的 `IcebergCatalogUtils` 通过 `CatalogUtil.buildIcebergCatalog(...)` 反射加载
`catalog-impl`，因此 JAR 的 FQCN、以及它的全部运行时依赖，必须由同一个 plugin
classloader 可见。

当前工作区的内部 profile 将 Iceberg 锁为 `dream-1.6.1-135`、Flink 锁为
`1.18.2-145`。`fluss-dist-internal` 目前只复制 `fluss-lake-iceberg` JAR 并校验其
`LakeStoragePlugin` provider；它**没有**打包上述 Iceberg 核心 JAR 或内部实现的依赖闭包。
因此，发布包必须由平台的制品流程补上以下两种之一，不能只复制 adapter JAR：

1. 一个经过依赖收敛验证的 `internal-iceberg-runtime` fat JAR；或
2. 一个 plugin 目录，包含所有下列普通 JAR，且每个类只出现一次。

最小 plugin 目录清单如下（`<internal-*>` 是平台真实制品名，不能照抄为字面值）：

```text
${FLUSS_HOME}/plugins/iceberg/
  fluss-lake-iceberg-<internal-fluss-version>.jar
  iceberg-api-dream-1.6.1-135.jar
  iceberg-common-dream-1.6.1-135.jar
  iceberg-core-dream-1.6.1-135.jar
  iceberg-data-dream-1.6.1-135.jar
  iceberg-parquet-dream-1.6.1-135.jar
  iceberg-orc-dream-1.6.1-135.jar
  iceberg-bundled-guava-dream-1.6.1-135.jar
  <internal-catalog>-<version>.jar
  <internal-fileio>-<version>.jar
  <internal-auth-provider>-<version>.jar
  <jdbc-driver-or-object-store-sdk-or-hadoop-client dependencies>.jar
```

不要同时部署 fat JAR 与其已包含的 Iceberg/AWS/Hadoop 子 JAR，也不要把 Iceberg 1.10.x
或 `iceberg-flink-runtime` 放进该目录。若 Catalog 是 JDBC，还需 JDBC driver；若 FileIO
调用 Hadoop/HDFS/S3A，则带入唯一一套经过认证的 Hadoop client（或以
`HADOOP_CLASSPATH` 提供）；若 FileIO 自己封装对象存储 SDK，则只部署该封装所需的 SDK。

Flink Tiering 集群的 `${FLINK_HOME}/lib` 需拥有同一组 runtime JAR（以及 Flink 1.18
connector）。业务 Flink 只有在执行 union/lake read 时才需要同一 Iceberg/Catalog/FileIO
闭包；纯 Fluss 热数据写入不需要它。

配置只引用类和属性，不携带 JAR 路径：

```yaml
# Coordinator 和每个 TabletServer；FQCN 必须来自 plugin 目录中的内部 JAR。
datalake.enabled: true
datalake.format: iceberg
datalake.iceberg.catalog-impl: com.company.platform.iceberg.InternalCatalog
datalake.iceberg.io-impl: com.company.platform.iceberg.InternalFileIO
datalake.iceberg.name: fluss_catalog
datalake.iceberg.warehouse: s3://<bucket>/iceberg

# 以下键由 Iceberg 原样交给内部实现；名称由内部 SPI 定义。
datalake.iceberg.catalog.endpoint: https://<catalog-endpoint>
datalake.iceberg.auth.provider: com.company.platform.auth.WorkloadIdentityProvider
datalake.iceberg.auth.audience: <catalog-audience>
datalake.iceberg.storage.endpoint: https://<object-store-endpoint>
```

Tiering 提交时逐项改成相同的 `--datalake.iceberg.*` 参数。认证优先让
`WorkloadIdentityProvider` / token-provider 从挂载的 short-lived token、环境或 JVM
identity 获取凭据；禁止把 access key、数据库密码或 OAuth secret 编进 JAR、`server.yaml`
或 `flink run` 命令。需要向 Flink 暴露 Iceberg 文件的 union read 还需要 Flink 自身取得
对象存储读权限；Catalog credential vending 不会自动解决 Flink 直接读 Parquet 的权限。

发布前在三种运行面各做一次 classpath smoke check：加载 Catalog、实例化 FileIO、创建/读取
一个 Iceberg 表。对 Fluss 服务端和 Tiering 集群应额外验证 `LakeStoragePlugin` 与
`LakeTieringFactory` 的 ServiceLoader discovery；业务 Flink 验证 union read。

## Fluss 服务端配置

在 Coordinator 和每个 TabletServer 的 `conf/server.yaml` 放入完全相同的
`datalake.*` 配置（网络地址、`data.dir` 等节点参数除外）：

```yaml
# 基础集群
zookeeper.address: zk-1.example:2181
bind.listeners: FLUSS://<node-hostname>:9123
remote.data.dir: s3://<bucket>/fluss/remote

# Fluss remote storage 的实现和凭据配置，示例为 S3 兼容存储
s3.endpoint: https://<object-store-endpoint>
s3.region: <region>
s3.path-style-access: true

# 流式湖仓
datalake.enabled: true
datalake.format: iceberg
datalake.iceberg.catalog-impl: <internal.iceberg.CatalogImpl>
datalake.iceberg.name: fluss_catalog
datalake.iceberg.warehouse: s3://<bucket>/iceberg
datalake.iceberg.io-impl: <internal.iceberg.FileIOImpl>

# 将 Catalog/FileIO 的其余原生属性原样传入；例：
datalake.iceberg.uri: <catalog-uri>
datalake.iceberg.<credential-or-endpoint-key>: <secret-reference-or-injected-value>
```

Fluss 会去掉 `datalake.iceberg.` 前缀，并将其余属性交给 Iceberg Catalog/FileIO。因此
Catalog 名称、warehouse、实现类、endpoint 与认证方式必须和 Tiering 作业参数完全一致。

## 启动次序

1. 启动对象存储、Iceberg Catalog（和其元数据服务，如 PostgreSQL/HMS/REST）、ZooKeeper。
2. 校验 Fluss service-side plugin 的 JAR 和 Hadoop classpath，启动 Coordinator，再启动
   TabletServer；确认 `bootstrap.servers` 可连。
3. 在专用 Tiering Flink 1.18 集群的 JobManager/TaskManager 安装 JAR，随后重启该 Flink
   集群。
4. 启动业务 Flink 1.18 集群并安装 connector；用 SQL client 建 catalog 和表、提交写入任务。
5. 提交 Tiering 作业，最后验证 Iceberg snapshot、Fluss LakeSnapshot 和 union read。

## 业务 Flink SQL 任务

```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-1.example:9123'
  -- Catalog/FileIO 的凭据以运行时安全配置传入，不硬编码在 SQL 中
);
USE CATALOG fluss_catalog;

CREATE TABLE orders (
  order_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(18, 2),
  updated_at TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket.num' = '16',
  'bucket.key' = 'order_id',
  'table.datalake.format' = 'iceberg',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '1min',
  'table.datalake.auto-compaction' = 'false',
  'table.datalake.auto-expire-snapshot' = 'false'
);

-- 示例：将 CDC / Kafka / 其他 source table 持续写入 Fluss
INSERT INTO orders
SELECT order_id, user_id, amount, updated_at FROM source_orders;
```

`table.datalake.freshness` 是最大允许湖端滞后。新环境先用 1–3 分钟而不是 30 秒，等确认
文件数量、提交延迟和成本后再收紧。主键表的更新与删除由 Iceberg 的 MOR 语义表示。

## 提交 Tiering 任务

Tiering 任务参数必须逐项复制服务端的所有 `datalake.*` 逻辑配置；推荐由同一份部署配置
渲染两个目标，而不是手工维护两套。

```shell
${TIERING_FLINK_HOME}/bin/flink run \
  -Dparallelism.default=3 \
  -Dstate.checkpoints.dir=s3://<bucket>/flink-checkpoints/tiering \
  /opt/fluss/fluss-flink-tiering-<internal-version>.jar \
  --fluss.bootstrap.servers coordinator-1.example:9123 \
  --datalake.format iceberg \
  --datalake.iceberg.catalog-impl <internal.iceberg.CatalogImpl> \
  --datalake.iceberg.name fluss_catalog \
  --datalake.iceberg.warehouse s3://<bucket>/iceberg \
  --datalake.iceberg.io-impl <internal.iceberg.FileIOImpl> \
  --datalake.iceberg.uri <catalog-uri> \
  --lake.tiering.io.tmp.dirs /mnt/ssd/fluss-tiering-tmp
```

不要在命令行传递明文密钥。使用 Flink/Kubernetes secret 挂载、环境变量或内部凭据 provider，
再通过同一配置渲染器生成 Fluss 与 Tiering 两侧的引用。

## 验收与运行规则

```sql
-- 实时 + 历史的 union read
SELECT * FROM orders;

-- Iceberg-only，适合验证已落湖内容
SELECT * FROM orders$lake;
```

验收应依次确认：Tiering 作业持续 RUNNING；在 freshness 窗口后 Catalog 中出现 Iceberg
snapshot 和文件；Fluss 记录对应 LakeSnapshot/progress；`orders` 的 union read 含最新数据，
`orders$lake` 只含已 tier 的数据。故障演练至少覆盖 Iceberg commit 前、commit 后但 Fluss
确认前的重启；恢复后不得丢数或造成错误可见的重复数据。

Fluss 是 lake 表的 schema/metadata owner。不要由 Spark、Trino 或原生 Iceberg 作业修改
Fluss 管理的表结构；schema evolution 一律通过 Fluss `ALTER TABLE` 完成。外部引擎可以读，
写入需要另行定义 commit 并发与 schema ownership 协议。小文件压实和 snapshot 过期先作为
独立、受控的 Iceberg maintenance 工作流，确认兼容性后才开启自动维护。

## 证据来源

- 本仓库 `.scratch/fluss-flink118-iceberg16/spec.md`：内部锁定矩阵和专用 Tiering 部署约束。
- [Fluss: Deploying Streaming Lakehouse](../../website/docs/install-deploy/deploying-streaming-lakehouse.md)：配置、JAR 放置和提交方式。
- [Fluss: Iceberg format](../../website/docs/streaming-lakehouse/datalake-formats/iceberg.md)：Iceberg 配置前缀、Hadoop/classpath 与表映射。
- [Fluss: Tiering Service](../../website/docs/streaming-lakehouse/tiering-service.md)：独立 Flink 作业、exactly-once 语义、参数与扩缩容。
- [Apache Iceberg 1.6 configuration](https://iceberg.apache.org/docs/1.6.1/configuration/)：Catalog 与 FileIO 原生属性。

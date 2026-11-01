[中文](#zh) | [English](#en)

<a id="zh"></a>

# Airflow / Control-M 资源额度与串行步骤

日期：2026-11-01（Asia/Shanghai）。

## 问题与结论

一个 XML 定义了 10 个串行 step，应该申请多少资源？关键是调度器看见一个 Job，还是十个独立 Job。XML 文件数量、步骤数量不决定资源申请量。

Control-M Resource Pool（旧称 Quantitative Resource）和 Airflow Pool 都是逻辑额度。可以约定 1 单位等于一个并发作业，或使用负载权重。调度器不会因为填写数量就自动分配或限制相应 CPU、内存、数据库连接。实际 CPU / 内存由运行环境控制；在 Kubernetes 中通过容器 requests / limits 设置。不要把 Pool slot 与 Kubernetes ResourceQuota 混同。

## 两种映射

1. **一个 Control-M Job 启动一个 Java / Spring Batch 程序，程序内部有十个 step。** 若每个 Job 申请 EOD_BATCH 的 1 单位，就在该 Job 执行期间持有，程序结束后释放；不会每进入一个内部 step 就重新申请，也不因有十步而申请十份。迁移时可保留程序，通过一个 Airflow KubernetesPodOperator task 启动，设置 pool="eod_batch"、pool_slots=1。内部失败与重启仍由应用处理；Airflow 重试整个 task 会再次启动程序，必须确认幂等与 Spring Batch 重启参数。
2. **XML 中有十个独立的 Control-M Job，以依赖关系串联。** 迁移为一个 DAG 内十个 task，关系为 t1 >> t2 >> ... >> t10。每个 task 分别设置同一个 pool、pool_slots=1，逐个申请和释放。单次串行链最多有一个业务步骤执行，但多个 DAG run 或其他 DAG 可以共享资源池。前一步释放不保证后一步立刻获得额度，也不保证整条链独占资源。

## Airflow 设置与使用

在 Airflow 环境中创建资源池（示例值，不是当前部署的修改）：

```sh
airflow pools set eod_batch 5 "At most five batch tasks"
```

将以下参数加到实际 Operator：

```python
pool="eod_batch",
pool_slots=1,
```

这表示该池允许最多五个各占一份额度的 task 占用额度；其他调度限制或执行资源不足仍可能使实际并发更低。若只允许同一 DAG 同时运行一个批次，在 DAG 上另外设置 max_active_runs=1；这不限制其他 DAG，也不是整个流程的跨 DAG 锁。

如果使用 KubernetesPodOperator，实际容器资源独立配置，例如：

```python
from kubernetes.client import models as k8s

# Operator 参数片段；镜像、命令和 DAG 需由实际应用提供。
container_resources=k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
),
```

500m 表示 0.5 CPU 的 request；CPU limit 为 1 CPU，内存 limit 为 2Gi。示例需根据观测调整，不能由十个串行 step 推导。串行步骤不把 CPU / 内存需求简单相加，运行时长通常会累积。

若使用 deferrable=True 等待外部 Pod，且 Pool 用于限制仍在运行的外部 Batch 数量，需要在 Pool 设置启用 Include deferred tasks。否则 task 进入 deferred 状态后可能不占该池额度，即使 Pod 还在运行。Worker slot、Pool slot 和 Pod 资源是不同的控制对象。

## 相关文件、验证与限制

- [现有 POC DAG](../2026-10-27-airflow-kubernetes-spring-batch-migration/poc/charts/eod-poc/files/spring_batch_poc.py)：已经使用 eod_compute Pool、deferrable=True 和 container_resources；本文的 eod_batch 是教学示例，不替换现有设置。
- 用户已说明一个 Control-M Job 的十步分别触发不同 Spring Batch；尚未查看实际 XML 和程序，无法确认拆分条件或生产容量。
- 核对了官方文档和现有 POC 源码；未部署或运行本文示例，未修改现有集群。
- 使用前确认调度单元、资源池含义、跨运行并发和重试边界，再用负载测试确定容量。

## 补充：一个 Control-M Job 依次触发十个不同 Spring Batch

用户进一步说明：当前是一个 Control-M Job，十步分别触发不同 Spring Batch。若这些 Batch 可独立启动、等待完成并报告结果，建议新架构采用一个业务 DAG、十个串行 Airflow task，每个 task 用 KubernetesPodOperator 启动一个 Batch Pod。外层流程依赖交给 Airflow，每个 Batch 内部的 chunk、事务、Step 和检查点继续由 Spring Batch 管理。单个 Batch 内部 Step 不必逐个拆成 Airflow task。

```text
DAG（max_active_runs=1）
  batch_01 → batch_02 → ... → batch_10
      每个 task → 一个 Pod → 一个独立 Spring Batch Job
```

十个 task 不等于十个同时运行的 Pod；严格串行的单次 DAG run 通常仅有一个业务 Batch Pod 在执行（不包括 Airflow 服务、清理中的 Pod 或异常遗留 Pod）。按该模式使用：

1. 为十个 Batch 提供可单独调用的启动入口；镜像可以分别构建，也可以在相同依赖与发布周期下共享镜像并选择 Job。
2. 在一个 DAG 中串联十个 KubernetesPodOperator，保留默认 all_success 依赖，等待当前 Batch 真正完成再开始下一步。若仅调用异步触发 API，需继续查询最终执行状态，不能把请求被接受当成功。
3. 开始时每个 task 配置 pool="eod_compute", pool_slots=1。现有 POC 安装脚本创建该池容量为 2，并使用 --include-deferred；这里只核对了脚本，未检查当前集群状态。需要更多并发时按测量调整，不因十步就设置十个 slot。
4. 每个 task 独立指定 CPU / 内存 requests 和 limits。前一步结束后释放 Pool 额度，下一步重新竞争；max_active_runs=1 限制这个 DAG 的批次重叠，不锁住其他 DAG。如果业务要求整条链跨 DAG 独占，需要另行设计带失败恢复的业务锁。
5. 第六步失败时，默认依赖使后续步骤等待成功，重试预算耗尽后后续可成为 upstream_failed。自动重试或手工清理失败 task 后可重跑第六步，无需主动清理已成功的前五步；若后续已被标记 upstream_failed，手工恢复时应选择清理相应下游。该行为仍受依赖和重试配置影响。
6. Spring Batch 业务失败必须映射为进程非零退出码；设置执行超时，持久化 JobRepository、业务输出和日志。Pod 间通过数据库、对象存储或显式共享存储交换数据，不依赖前一个 Pod 的本地目录。
7. 统一传递 businessDate、tenant/domain（如适用）、逻辑批次标识。为同一逻辑执行的重试保持 identifying JobParameters 稳定；不要为了每次重试都能启动就加入新的 identifying 时间戳。根据实际 Spring Batch 版本和入口实现 restart / 已完成实例处理。Pod 被杀后若元数据仍为 STARTED，必须先确认旧执行已停止再按版本提供的恢复流程处理。跨系统副作用需应用级幂等，Airflow 重试不提供 exactly-once 保证。

拆分前检查共享 JVM、内存、事务上下文、本地文件、父 JobRepository 重启语义和补偿逻辑。若当前十步实际上强依赖这些上下文，先用一个 task 启动旧包装流程作为过渡，再逐步解耦；十个独立程序不应仅因过去包在一个 Control-M Job 中而永久保留为一个不可观察的 task。非常短的步骤也需权衡 JVM / Pod 启动开销。

本建议以用户描述为依据，未查看 XML 或十个 Batch 源码，未修改运行中的 DAG、镜像或集群。现有 POC 是一个 task 启动一个含内部三步的真实 Spring Batch，不是已实现的十个独立 Batch 流程。

补充参考：[Airflow Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)、[Spring Batch JobInstance 与重启语义](https://docs.spring.io/spring-batch/reference/domain.html)、[Spring Batch 启动与退出码](https://docs.spring.io/spring-batch/reference/job/running.html)。Spring 文档 stable 当前指向 6.x；现有应用的具体配置应匹配其实际版本。

## 短 Batch、异步等待、监控与清理：推荐修订

进一步确认：绝大多数调用同步，少数异步后通过数据库查询结果；业务无状态；部分任务不能重复；多数执行几秒或长短混合。因此不再默认将十个短 Batch 全部分配独立 Pod。建议按业务恢复边界合并连续短 Batch 为一个阶段 task / Pod，长任务、资源需求差异大的任务、必须单独重试的任务独立拆出。阶段数应通过测量决定；同一个 Pod 内执行多个 java -jar 仍有多次 JVM / Spring 启动，只有 Pod 调度开销被摊薄。TaskGroup 只组织 UI，不会合并 Pod 或复用 JVM。若 JVM 初始化仍占主要时间且吞吐要求高，再评估常驻 Batch runner 服务；它需要任务隔离、并发控制、幂等与生命周期管理，不是直接改成 kubectl exec 到常驻 Pod。

端到端耗时包含排队、Airflow 调度、Pod 创建和调度、镜像获取、JVM / Spring / agent 初始化、业务处理及结果确认。示例假设每步业务 3 秒，额外启动与调度 8 秒，十个串行独立 Pod 约需 110 秒，而业务总计只有 30 秒；这是算术示例，不是本集群实测。预置固定版本应用与 agent、不可变镜像、缓存 / 预拉取、足够节点容量和合理 CPU request 都值得测量。缓存仅对镜像已在目标节点且拉取策略适当时有效，删除 Pod 通常不立即删除节点镜像缓存。不要仅凭经验给出固定启动秒数。

合并阶段会扩大重试范围，必须持久化每个子 Batch 的状态和输出标识，只跳过已确认成功且业务上允许跳过的步骤。不能重复任务可设置 retries=0，作为停止自动重试的运维策略，但一个 allow_retry=false flag 不能防止手工重跑、网络超时后的重复提交或其他重复执行。为此需业务幂等键、原子唯一约束 / 状态转换、对外系统幂等支持及结果不明时的人工核对；本地 flag 或账本也不能独自保证跨外部系统 exactly-once。

异步调用应持久化外部 execution ID：提交一次后只查询该次执行，等待超时不应盲目再提交。可先在合并阶段中轮询数据库，或拆为提交 task + reschedule / deferrable 等待 task；后者可让提交 Pod 退出。分别设置运行名额与触发流量控制：提交 task 释放 Pool 后，等待 task 可能尚未获得 Pool，不能把二者简单当作外部运行期间的连续资源预留；如需严格外部并发控制，使用完整周期内持有的 deferrable task（Pool 计入 deferred），或外部执行系统的持久化配额。

推荐观测链路：

```text
阶段 task → Batch Pod（应用 + 已内置的 Java agent）
                 ├─ stdout/stderr → 常驻节点 Alloy → Loki → Grafana
                 ├─ 长运行 JVM / 应用指标 → Prometheus → Grafana
                 └─ 业务最终结果 → 持久化运行表；适用的汇总指标 → Pushgateway → Prometheus
```

Promtail 于 2026-03-02 EOL；新接入使用受维护的 Alloy 等采集器。日志通常用节点 DaemonSet 常驻采集，不必给每个 Batch Pod 启动日志 sidecar。受管环境若不能读取节点日志，需要另选受支持的采集方式。Java agent 不是日志采集器的替代品；具体是 JMX exporter、OpenTelemetry 还是其他 APM agent 尚未确认，导出协议和成本需按实际产品验证。

短任务可能在服务发现与 Prometheus scrape 前结束；即使保留已完成 Pod，退出的 JVM metrics endpoint 也不会继续服务。服务级 Batch 的末次结果、耗时、记录数和最后成功时间可按 Prometheus 指南推送到 Pushgateway，但它不保存每次执行事件，不自动清理旧序列；需要稳定 grouping key 和清理策略。不要用每次 run_id 作为无界 metrics label。逐次运行审计保存在数据库 / 日志；run_id、task_id、业务日期、execution ID 可作为结构化日志字段。长任务的运行中指标继续 scrape。若采用 OTel push，仍需验证退出前 flush、collector 接收和后端落地；强制终止时不能承诺最后一批 telemetry 一定送达。

业务结果持久化、日志尾部转发和指标导出经过验证后，成功 Pod 可以清理。开发期可短暂保留失败 Pod 收集退出码、OOMKilled、事件与描述信息，之后统一清理，监控存储按独立保留期保存。不要仅以“Pod 已结束”作为“日志已全部入库”的证明。直接由 KubernetesPodOperator 创建的裸 Pod 不适用 Job 的 ttlSecondsAfterFinished；延迟清理应由相应清理机制实现，或在采用 Kubernetes Job 时使用 Job TTL。Loki 中业务日志也不等同于 Airflow task 的完整调度日志，Airflow task log 需独立持久化或接入日志平台。

验证计划：选择短、长、不可重复和异步 Batch，测量冷 / 热镜像下 Pod 启动、JVM / agent 初始化及业务耗时；比较逐步 Pod、分阶段 Pod；测试失败恢复、重复提交和结果不明；删除 Pod 后检查尾部日志、最终指标与每次执行审计。此处仅更新设计，未部署监控组件、改动业务代码或测得性能。

新增官方资料：[Promtail EOL](https://grafana.com/docs/loki/latest/send-data/promtail/)、[Kubernetes 日志架构](https://kubernetes.io/docs/concepts/cluster-administration/logging/)、[Prometheus Batch 指标](https://prometheus.io/docs/practices/instrumentation/)、[Pushgateway 适用范围](https://prometheus.io/docs/practices/pushing/)、[Kubernetes 镜像](https://kubernetes.io/docs/concepts/containers/images/)。

## 官方资料

- [BMC Resource Pools](https://documents.bmc.com/supportu/9.0.21.000/en-US/Documentation/Resource_Pools.htm)
- [Airflow Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Kubernetes 容器资源管理](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

---

<a id="en"></a>

# Airflow / Control-M resource units and sequential steps

Date: 2026-11-01 (Asia/Shanghai).

## Problem and conclusion

How many resource units should a workflow with ten sequential steps declared in XML request? The key is whether the scheduler sees one Job or ten independent Jobs. Neither the number of XML files nor the number of steps determines the resource quantity.

Control-M Resource Pools (formerly Quantitative Resources) and Airflow Pools represent logical capacity. One unit may mean one concurrent job, or a workload weight. Specifying a quantity does not automatically allocate or enforce CPU, memory, or database connections. The runtime controls actual CPU and memory; Kubernetes uses container requests / limits. Pool slots are distinct from Kubernetes ResourceQuota.

## Two mappings

1. **One Control-M Job launches a Java / Spring Batch application with ten internal steps.** If the Job requests one unit from EOD_BATCH, it holds that unit during execution and releases it when the Job ends. It does not reacquire resources for every internal step or request ten units because it has ten steps. Migration can preserve the application and launch it with one Airflow KubernetesPodOperator task using pool="eod_batch", pool_slots=1. Internal failures and restarts remain application concerns; retrying the Airflow task launches the application again, so verify idempotency and Spring Batch restart parameters.
2. **The XML contains ten independent Control-M Jobs connected by dependencies.** Map these to ten tasks in one DAG, connected as t1 >> t2 >> ... >> t10. Each task uses the same pool and pool_slots=1, acquiring and releasing separately. A single sequential chain executes at most one business step at a time, but other DAG runs or DAGs may share the pool. Releasing one step's capacity does not guarantee that its successor starts immediately or that the whole chain has exclusive access.

## Airflow configuration and usage

Create a pool in the Airflow environment (illustrative values; no current deployment is changed):

```sh
airflow pools set eod_batch 5 "At most five batch tasks"
```

Add these parameters to the actual Operator:

```python
pool="eod_batch",
pool_slots=1,
```

This pool allows up to five tasks that each occupy one unit to hold capacity. Other scheduler limits or insufficient execution resources may reduce actual concurrency. To allow only one active run of the same DAG, separately set max_active_runs=1 on the DAG. This does not constrain other DAGs or provide a cross-DAG lock covering the entire workflow.

For KubernetesPodOperator, configure actual container resources independently, for example:

```python
from kubernetes.client import models as k8s

# Operator argument fragment; supply the application's actual image, command and DAG.
container_resources=k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1", "memory": "2Gi"},
),
```

500m is a request for 0.5 CPU; the CPU limit is 1 CPU and the memory limit is 2Gi. Adjust these examples using measurements, not the count of ten sequential steps. CPU and memory requirements of sequential steps are not simply added together; execution durations usually accumulate.

When using deferrable=True while waiting for external Pods, enable Include deferred tasks on the pool if it must limit the number of externally running Batch applications. Otherwise a deferred task may stop occupying pool capacity while its Pod still runs. Worker slots, pool slots, and Pod resources control different things.

## Related files, verification and limitations

- [Existing POC DAG](../2026-10-27-airflow-kubernetes-spring-batch-migration/poc/charts/eod-poc/files/spring_batch_poc.py): already uses the eod_compute pool, deferrable=True and container_resources. This document's eod_batch is a teaching example, not a replacement for existing settings.
- The user clarified that ten steps in one Control-M Job trigger distinct Spring Batch jobs; the XML and programs have not been inspected, so split feasibility and production sizing remain unverified.
- Official documentation and existing POC source were checked. These examples were not deployed or executed, and the existing cluster was not modified.
- Before use, identify scheduling units, pool semantics, concurrency across runs, and retry boundaries; then size capacity through load testing.

## Follow-up: one Control-M Job triggers ten distinct Spring Batch jobs

The user clarified that one Control-M Job has ten steps, each triggering a different Spring Batch job. If these batches can be launched independently, awaited, and checked for their final result, the recommended new design is one business DAG containing ten sequential Airflow tasks, each launching one Batch Pod through KubernetesPodOperator. Airflow owns dependencies between batches; Spring Batch retains each batch's chunks, transactions, internal Steps and checkpoints. Internal Steps need not each become Airflow tasks.

```text
DAG (max_active_runs=1)
  batch_01 → batch_02 → ... → batch_10
      Each task → one Pod → one independent Spring Batch Job
```

Ten tasks do not mean ten concurrently running Pods. A strictly sequential single DAG run normally executes only one business Batch Pod at a time, excluding Airflow services, terminating Pods and abnormal leftovers. Apply this design as follows:

1. Provide a separately callable entry point for each Batch. Images may be separate, or shared with job selection when dependencies and release cycles are compatible.
2. Chain ten KubernetesPodOperator tasks in one DAG using the default all_success dependency. Wait for actual Batch completion before starting its successor. If the interface only submits an asynchronous API request, continue polling for the final result; acceptance is not completion.
3. Initially use pool="eod_compute", pool_slots=1 for each task. The existing POC setup script creates this pool with capacity 2 and --include-deferred. Only the script was inspected, not the current cluster state. Change capacity based on measurements, not the number of sequential steps.
4. Set CPU / memory requests and limits per task. One step releases pool capacity and the next competes for it again. max_active_runs=1 prevents overlapping runs of this DAG, not other DAGs. Workflow-wide exclusivity across DAGs requires a separate business lock with failure recovery.
5. If step six fails, default dependencies wait for success; after retries are exhausted downstream tasks can become upstream_failed. Automatic retries or manually clearing the failed task can rerun batch six without deliberately clearing the first five successful tasks. If downstream tasks already became upstream_failed, select the relevant downstream tasks for clearing during manual recovery. Behavior remains subject to dependency and retry configuration.
6. Map Spring Batch business failure to a nonzero process exit code. Set execution timeouts and persist JobRepository, business outputs and logs. Exchange data through databases, object storage or explicitly shared storage rather than the previous Pod's local directory.
7. Pass businessDate, tenant/domain where relevant, and a logical batch identifier consistently. Keep identifying JobParameters stable across retries of the same logical execution; do not add a fresh identifying timestamp merely to bypass restart checks. Implement restart and already-completed-instance handling for the actual Spring Batch version and entry point. If a killed Pod leaves STARTED metadata, confirm the old execution has stopped before using version-appropriate recovery procedures. Application-level idempotency is needed for external side effects; Airflow retries do not guarantee exactly-once execution.

Before splitting, inspect shared JVM state, memory, transaction context, local files, parent JobRepository restart semantics and compensation logic. If the existing steps depend tightly on these, first run the legacy wrapper through one task as a transition and decouple incrementally. Independent programs should not permanently remain one opaque task solely because they previously belonged to one Control-M Job. For very short steps, also weigh JVM / Pod startup overhead.

This recommendation relies on the user's description. The XML and ten Batch implementations were not inspected, and no running DAG, image or cluster was modified. The existing POC launches one real Spring Batch containing three internal steps through one task; it does not yet implement ten independent batches.

Additional references: [Airflow Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html), [Spring Batch JobInstance and restart semantics](https://docs.spring.io/spring-batch/reference/domain.html), [Spring Batch launching and exit codes](https://docs.spring.io/spring-batch/reference/job/running.html). Spring stable documentation currently points to 6.x; application configuration must match its actual version.

## Short batches, asynchronous waiting, observability and cleanup: revised recommendation

Further clarification: most calls are synchronous; a few are asynchronous with database status queries. Applications are stateless, some must not execute twice, and most run for seconds or have mixed durations. Consequently, ten independent Pods are no longer the default recommendation. Group consecutive short batches into stage tasks / Pods at business recovery boundaries. Separate long batches, substantially different resource requirements and batches requiring independent retries. Determine stage count through measurements. Running several java -jar commands in one Pod still starts multiple JVMs / Spring contexts; it only amortizes Pod scheduling overhead. TaskGroup organizes the UI but does not merge Pods or reuse JVMs. If JVM initialization still dominates and throughput requires it, evaluate a persistent Batch runner service with isolation, concurrency, idempotency and lifecycle management, rather than simply executing commands in an existing Pod through kubectl exec.

End-to-end duration includes queuing, Airflow scheduling, Pod creation and placement, image retrieval, JVM / Spring / agent initialization, business execution and result confirmation. Assuming 3 seconds of business execution and 8 seconds of startup / scheduling overhead per step, ten sequential independent Pods take about 110 seconds for 30 seconds of business work. This is arithmetic illustration, not a cluster benchmark. Measure fixed-version application and agent packaging, immutable images, image caching / pre-pulling, available node capacity and appropriate CPU requests. Caching only helps when the target node holds the image and the pull policy permits reuse; deleting a Pod normally does not immediately remove node image caches. Do not promise a fixed startup duration without measurement.

Grouping expands the retry boundary. Persist each child Batch's status and output identity, and skip only steps confirmed successful and safe to skip under business rules. Setting retries=0 can stop automatic retries for non-repeatable work, but an allow_retry=false flag cannot prevent manual reruns, duplicate submissions after network timeouts or other duplicates. Use business idempotency keys, atomic uniqueness / state transitions, external-system idempotency and manual reconciliation for ambiguous outcomes. A local flag or ledger alone cannot guarantee exactly-once effects across external systems.

For asynchronous calls, persist the external execution ID: submit once and query that execution; do not blindly resubmit after a waiting timeout. Initially poll within a grouped stage, or separate submission and a reschedule / deferrable waiting task so the submission Pod can exit. Separate running-capacity control from submission-rate control. After submission releases a Pool slot, its waiting task may not yet acquire one, so two such tasks do not provide continuous capacity reservation for the external execution. Strict external concurrency needs a deferrable task holding logical capacity throughout its lifecycle with deferred tasks included, or durable quotas enforced by the external execution system.

Recommended observability flow:

```text
Stage task → Batch Pod (application + packaged Java agent)
                 ├─ stdout/stderr → persistent node Alloy → Loki → Grafana
                 ├─ long-running JVM / application metrics → Prometheus → Grafana
                 └─ business final results → durable run table; suitable summary metrics → Pushgateway → Prometheus
```

Promtail reached EOL on 2026-03-02. Use a maintained collector such as Alloy for new integration. A persistent node DaemonSet normally collects logs without adding a logging sidecar to every Batch Pod. Managed environments that prohibit node log access require another supported collection method. A Java agent is not a replacement for a log collector. Whether the intended agent is a JMX exporter, OpenTelemetry or another APM agent remains unknown; validate protocols and cost for the actual product.

Short tasks may finish before service discovery and a Prometheus scrape. Keeping a completed Pod does not keep an exited JVM's metrics endpoint alive. Following Prometheus guidance, suitable service-level Batch summaries such as last outcome, duration, record count and last success time may use Pushgateway. It does not store every execution event or automatically remove stale series; define stable grouping keys and lifecycle cleanup. Avoid unbounded per-run IDs as metrics labels. Keep per-execution audit in a database / logs, using run_id, task_id, business date and execution ID as structured log fields. Continue scraping long-running jobs. OTel push still requires shutdown flush and collector / backend delivery verification; final telemetry cannot be guaranteed after forced termination.

Successful Pods can be removed after durable business results, tail-log delivery and metric export have been validated. During development, briefly retain failed Pods to inspect exit codes, OOMKilled, events and descriptions, then clean them up separately; monitoring storage has its own retention. Pod completion alone does not prove all logs reached Loki. Bare Pods created directly by KubernetesPodOperator cannot use a Job's ttlSecondsAfterFinished; delayed cleanup requires an appropriate cleanup mechanism, or Job TTL if Kubernetes Jobs are adopted. Business logs in Loki are not the complete Airflow scheduling/task log; persist or ingest Airflow task logs independently.

Validation plan: choose short, long, non-repeatable and asynchronous batches; measure cold / warm-image Pod startup, JVM / agent initialization and business time; compare individual and grouped Pods. Test failures, duplicate submissions and ambiguous outcomes. After deleting Pods, verify tail logs, final metrics and per-execution audit. This is a design update only: no monitoring components were deployed, business code changed or performance measured.

Additional official references: [Promtail EOL](https://grafana.com/docs/loki/latest/send-data/promtail/), [Kubernetes logging architecture](https://kubernetes.io/docs/concepts/cluster-administration/logging/), [Prometheus Batch instrumentation](https://prometheus.io/docs/practices/instrumentation/), [Pushgateway scope](https://prometheus.io/docs/practices/pushing/), [Kubernetes images](https://kubernetes.io/docs/concepts/containers/images/).

## Official references

- [BMC Resource Pools](https://documents.bmc.com/supportu/9.0.21.000/en-US/Documentation/Resource_Pools.htm)
- [Airflow Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Kubernetes container resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

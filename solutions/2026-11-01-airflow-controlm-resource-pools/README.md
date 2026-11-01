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

## Official references

- [BMC Resource Pools](https://documents.bmc.com/supportu/9.0.21.000/en-US/Documentation/Resource_Pools.htm)
- [Airflow Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Kubernetes container resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

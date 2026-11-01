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
- 尚未查看用户实际 XML，不能确认它属于上述哪种结构，也不能据此定出生产容量。
- 核对了官方文档和现有 POC 源码；未部署或运行本文示例，未修改现有集群。
- 使用前确认调度单元、资源池含义、跨运行并发和重试边界，再用负载测试确定容量。

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
- The user's actual XML has not been inspected, so neither its structure nor production sizing is established.
- Official documentation and existing POC source were checked. These examples were not deployed or executed, and the existing cluster was not modified.
- Before use, identify scheduling units, pool semantics, concurrency across runs, and retry boundaries; then size capacity through load testing.

## Official references

- [BMC Resource Pools](https://documents.bmc.com/supportu/9.0.21.000/en-US/Documentation/Resource_Pools.htm)
- [Airflow Pools](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [KubernetesPodOperator](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html)
- [Kubernetes container resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

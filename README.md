[中文](#zh) | [English](#en)

<a id="zh"></a>

# Cloud native batch

本项目用于保存每次讨论形成的完整解决方案及相关代码，方便通过 GitHub 在其他环境获取。

## 归档约定

所有 Markdown 文档在同一文件中同时包含完整英文版和中文版，并在每次更新时同步两种语言。

每个方案放在 `solutions/YYYY-MM-DD-topic/` 中，包含方案说明、使用步骤、相关文件和验证情况。同一主题的后续调整更新原有目录，通过 GitHub 提交历史保留版本。

## GitHub 同步状态

目标仓库：[charlven/cloud-native-batch](https://github.com/charlven/cloud-native-batch)，默认分支为 `main`。

每次完成方案后，将方案说明和相关代码同步到本仓库，并在聊天中提供对应链接。

## 在其他环境获取

首次获取：

```sh
git clone git@github.com:charlven/cloud-native-batch.git
```

后续在仓库目录内更新：

```sh
git pull --ff-only
```

也可以在 GitHub 仓库页面通过 **Code → Download ZIP** 下载。

## 方案索引

- [2026-10-27：Spring Batch / Control-M 向 Airflow / Kubernetes 迁移方案](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/README.md)：目标架构、运维支持、并发与容量、配置与状态、跨 domain 编排、镜像保障及 POC 验收计划。原文为架构设计评估；后续本地执行结果见下方 POC。
- [本地 Kubernetes / Airflow / Spring Batch POC](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/poc/README.md)：2026-11-01 已跑通真实 Java Batch，Airflow 正常执行与主动失败传播均验证通过；包含源码、DAG、Helm Chart、安装和验收脚本。S3/Python 模拟链路也已验证；Java 接入 S3 与生产容量验证待后续完成。

---

<a id="en"></a>

# Cloud native batch — English

This repository archives the complete solutions and supporting code developed during project discussions, so they can be retrieved from GitHub in other environments.

## Archive convention

Each solution lives under `solutions/YYYY-MM-DD-topic/`, including the explanation, usage instructions, related files, and verification status. Follow-up work on the same topic updates the existing directory; Git history preserves earlier revisions. All Markdown documents contain complete Chinese and English versions, updated together.

## GitHub synchronization

Destination: [charlven/cloud-native-batch](https://github.com/charlven/cloud-native-batch), default branch `main`.

Completed solution documents and supporting code are synchronized to this repository, with a link provided in the chat.

## Retrieve in another environment

Initial checkout:

```sh
git clone git@github.com:charlven/cloud-native-batch.git
```

Update from the repository directory:

```sh
git pull --ff-only
```

Alternatively, use **Code → Download ZIP** on GitHub.

## Solution index

- [2026-10-27: Spring Batch / Control-M migration to Airflow / Kubernetes](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/README.md#en): architecture, operational support, concurrency and capacity, configuration and state, domain orchestration, image availability, and POC acceptance criteria. The original document is a design assessment; subsequent local execution results are linked below.
- [Local Kubernetes / Airflow / Spring Batch POC](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/poc/README.md#en): the real Java Batch pipeline and intentional failure propagation were verified on 2026-11-01. Includes source, DAGs, Helm charts, setup and verification scripts. The S3/Python simulation has also passed; Java-to-S3 integration and production capacity validation remain outstanding.

# Cloud native batch

本项目用于保存每次讨论形成的完整解决方案及相关代码，方便通过 GitHub 在其他环境获取。

## 归档约定

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

- [2026-10-27：Spring Batch / Control-M 向 Airflow / Kubernetes 迁移方案](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/README.md)：目标架构、运维支持、并发与容量、配置与状态、跨 domain 编排、镜像保障及 POC 验收计划。当前为设计评估，尚未执行 POC。
- [本地 Kubernetes / Airflow POC：部署文件与当前进度](solutions/2026-10-27-airflow-kubernetes-spring-batch-migration/poc/README.md)：DAG、Helm Chart 和安装脚本初稿已归档；本机节点恢复、Airflow 安装及运行验证待继续。

[English](#en) | [中文](#zh)

<a id="en"></a>

# Project workflow

The user wants the solutions produced in this project saved to GitHub so they can retrieve them elsewhere.

- For each substantive solution, save a self-contained Markdown document under `solutions/YYYY-MM-DD-topic/README.md`, using Asia/Shanghai dates. Include the problem, final solution, usage steps, related files, and any verification or unresolved limitations. Save accompanying code and configuration in the same folder when appropriate.
- Every Markdown document published in this repository must include complete English and Chinese versions in the same file, with language navigation. Keep both versions aligned in every update, including commands, results, limitations, and links. This applies to READMEs, solution documents, progress/history records, and these project instructions. Do not replace one language with a short summary of the other.
- Update the root README solution index. Revise the existing solution folder for follow-up work on the same topic.
- Once the user identifies the destination GitHub repository, record it here and synchronize completed solution files to it as part of delivering the work. The user has requested this ongoing workflow; routine uploads within that scope do not need repeated confirmation.
- Check the current remote state before writing. Preserve unrelated changes; do not force-push. Respect branch protection and use a pull request if required.
- Upload final solution artifacts, not full chat transcripts, credentials, secrets, or unrelated local files.
- Verify the remote result and provide its GitHub link. If syncing fails, preserve the local files and state clearly that they have not been uploaded.

## Destination

Repository: https://github.com/charlven/cloud-native-batch
SSH URL: git@github.com:charlven/cloud-native-batch.git
Default branch: main
The local checkout uses the SSH URL above as origin. Synchronize using Git over SSH; the connected GitHub integration rejected content writes with HTTP 403 during setup. Fetch and inspect remote changes before pushing, preserve concurrent edits, and never force-push. This repository is public.

<a id="zh"></a>

# 项目工作流程（中文版）

用户希望将本项目中形成的解决方案保存到 GitHub，以便在其他环境获取。

- 每个实质性方案保存到 `solutions/YYYY-MM-DD-topic/README.md`，日期使用 Asia/Shanghai 时区。文档应自成一体，包含问题、最终方案、使用步骤、相关文件、验证情况及未解决的限制。适当时将配套代码和配置放在同一目录。
- 本仓库发布的每个 Markdown 文档必须在同一个文件中同时包含完整英文版和中文版，并提供语言跳转。每次更新必须同步两种语言的命令、结果、限制和链接。规则适用于 README、方案、进度/历史记录和本项目指令；不能以简短摘要代替其中一种语言的完整内容。
- 更新根目录 README 的方案索引。同一主题的后续工作更新原有方案目录。
- 用户确定目标 GitHub 仓库后，在此记录地址，并在交付时同步完成的方案文件。用户已授权此持续工作流程，范围内的常规上传无需重复确认。
- 写入前检查远端当前状态，保留无关修改，禁止强制推送。遵守分支保护，必要时使用 Pull Request。
- 上传最终方案产物，不上传完整聊天记录、凭据、密钥或无关本地文件。
- 验证远端结果并提供 GitHub 链接。如果同步失败，保留本地文件并明确说明尚未上传。

## 目标仓库

仓库：https://github.com/charlven/cloud-native-batch

SSH 地址：`git@github.com:charlven/cloud-native-batch.git`

默认分支：`main`

本地 checkout 的 origin 使用上述 SSH 地址。使用 Git over SSH 同步；设置期间 GitHub 连接器的内容写入返回 HTTP 403。推送前 fetch 并检查远端修改，保留并发编辑，禁止 force-push。本仓库为公开仓库。

# Project workflow

The user wants the solutions produced in this project saved to GitHub so they can retrieve them elsewhere.

- For each substantive solution, save a self-contained Markdown document under `solutions/YYYY-MM-DD-topic/README.md`, using Asia/Shanghai dates. Include the problem, final solution, usage steps, related files, and any verification or unresolved limitations. Save accompanying code and configuration in the same folder when appropriate.
- Update the root README solution index. Revise the existing solution folder for follow-up work on the same topic.
- Once the user identifies the destination GitHub repository, record it here and synchronize completed solution files to it as part of delivering the work. The user has requested this ongoing workflow; routine uploads within that scope do not need repeated confirmation.
- Check the current remote state before writing. Preserve unrelated changes; do not force-push. Respect branch protection and use a pull request if required.
- Upload final solution artifacts, not full chat transcripts, credentials, secrets, or unrelated local files.
- Verify the remote result and provide its GitHub link. If syncing fails, preserve the local files and state clearly that they have not been uploaded.

## Destination

Repository: https://github.com/charlven/cloud-native-batch
SSH URL: git@github.com:charlven/cloud-native-batch.git
Default branch: main
Use the connected GitHub tools to synchronize files. Local SSH is not required. Check remote files before creating or updating them, and preserve concurrent edits. This repository is public.

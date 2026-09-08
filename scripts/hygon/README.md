# HCU CI 验证与上游接入

本分支以官方代码为基础，移植 `origin/v0.3.13.post1_dev` 的 HCU/RPC、
SHCA 适配及 CI。没有合并整个厂商分支，也没有移植厂商的全局性能默认值、
版本命名、发布流水线或替换官方 Store benchmark。

## 流程和被测代码

- `ci_hygon.yaml`：可复用流程。配置检查 → standard/rpc/shca wheel 构建和
  CIUpload REPAIR → standard wheel 的跨机 TE 测试 → Store etcd/HA RDMA
  benchmark → HCU PR Gate。RPC、SHCA 当前仅构建，和原有 CI 覆盖范围一致。
- `ci_hygon_validation.yml`：仅供 `HYGON-AI/mooncake-das` 验证。向目标为
  `official_code_ci` 的同仓库 PR 添加 `run-hygon-ci` 标签时运行硬件测试。
  使用 `pull_request: labeled`，不要求将入口放入默认分支。
- `ci_hygon_pr.yml`：官方 PR 入口，仅在 `kvcache-ai/Mooncake` 且仓库变量
  `HCU_CI_ENABLED=true` 时启用，通过 `pull_request_target: labeled`
  响应目标为 `main` 的同仓库 PR 上的 `run-hygon-ci` 标签。
- 所有 checkout 使用加标签事件中 PR head 的同一个完整 SHA。
  PR 更新不会自动重跑，也不会改变已经启动任务的被测 SHA。
  完成后移除标签，下次需要验证新提交时重新添加。取消运行后若标签仍在，手动移除。
- 本仓库验证入口与官方入口使用的事件不同。本仓库测试通过并不代表官方的
  事件、runner 授权或审批已验证；官方接入时仍需验证这些环节。
- 官方已有 workflow 保留原触发规则。HCU 的配置检查和结果汇总使用
  `ubuntu-latest`；硬件构建和测试使用自托管机器。

## 1. 先配置仓库变量

位置：GitHub 仓库 Settings → Secrets and variables → Actions → Variables。
下列值应配置为 repository variables（或本仓库可访问的组织变量），
不要仅放在 environment variables 中，因为配置检查和 runner 调度需要提前读取。

| 变量 | 必需 | 配置方式 |
| --- | --- | --- |
| `BASE_COMPILE_IMAGE_UBUNTU_PY310` | 是 | 现有 Ubuntu/Python 3.10 编译镜像；构建和测试节点均能拉取 |
| `RESOURCE_SERVER_URL` | 是 | 现有资源服务器根 URL，包含 `/dtk-pkg/` 下的 DTK 包 |
| `PYPI_URL` | 是 | 现有 Python 源根 URL，CI 拼接 `/nightly/dtk2604/+simple/`；需提供 ciupload、构建与运行依赖 |
| `HCU_TEST_RUNNER_LABELS` | 是 | JSON 字符串数组，必须精确选中主测试节点。若沿用旧 runner，可用 `["self-hosted","Linux","X64","hcu","hcu-ci-pr","bw1100","nmz4"]`，先核实标签 |
| `HCU_TEST_TARGET_HOST` | 是 | **runner 本机**的服务主机名，解析到本机 RDMA/服务 IP；脚本会检查。不能指向第三台远程机器 |
| `HCU_TEST_INITIATOR_HOST` | 是 | 第二台测试机器的服务主机名 |
| `HCU_TEST_TARGET_FILTER` | 是 | 主节点 RDMA 网卡，例如旧脚本的 `mlx5_6`；按实际机器确认 |
| `HCU_BUILD_RUNNER_LABELS` | 否 | 构建 runner 标签 JSON；默认 `["self-hosted","Linux","X64","hcu","hcu-ci-pr"]` |
| `HCU_TEST_REMOTE_USER` | 否 | SSH 用户，默认 `github` |
| `HCU_TEST_INITIATOR_SSH_HOST` | 否 | 若管理网 SSH 地址不同于第二节点服务 IP，填管理网主机名/IP |
| `HCU_TEST_SSH_PORT` | 否 | 默认 `22` |
| `HCU_GPU_USAGE_THRESHOLD` | 否 | GPU 显存或计算利用率允许的最大百分比，默认 `10` |
| `HCU_GPU_WAIT_TIMEOUT` | 否 | 每个节点等待稳定空闲 GPU 的最长秒数，默认 `900` |
| `HCU_STORE_ETCD_URL` | 否 | 可访问的 etcd v3.6.1 linux-amd64 tarball；默认使用 GitHub release |
| `HCU_CI_ENABLED` | 官方接入时 | 官方环境准备完成后才设置 `true`；本仓库分支验证无需设置 |

旧 workflow 中的 `github-nmz1` / `github-nmz2` 不再硬编码。
特别注意：旧 job 的 `nmz4` runner 标签不代表目标一定就是 `github-nmz1`，
必须核对 runner 实际运行在哪台机器。

## 2. 确认机器和镜像

- 构建 runner Online，标签匹配；安装 Docker，具备 `/dev/kfd`、`/dev/dri`
  和 `/opt/hyhal`。镜像是可写的临时构建环境，不能预留冲突的 `/opt/dtk`。
- 测试 runner 就是主节点。两台测试节点均有 Docker、DTK 所需驱动、设备和
  `/opt/hyhal`，能访问镜像源、资源服务器和 Python 源。
- 主节点 runner 用户可以免交互 SSH/SCP 到第二节点，并已有经过确认的
  known_hosts。脚本使用 `StrictHostKeyChecking=yes`，不会自动信任主机密钥。
- 主节点需有 bash、ip、awk、cut、flock、getent、python3、timeout、ssh、scp。
  第二节点同样需要 Docker、bash、flock、python3、timeout；服务网互通。
- Store 测试每节点申请约 40GB segment，benchmark 另有约 40GiB 本地 buffer，
  还需留出程序和系统余量。使用专用测试机器，确认内存及 RDMA 注册资源。
- SHCA 构建沿用基础镜像。若其中没有
  `/usr/include/infiniband/shca_17b_types.h`，构建会从 `RESOURCE_SERVER_URL`
  下载 `mlxtoshca.sh` 和 `shca-tools_2.500.4.B068-Ubuntu22.04_amd64.deb`
  并在构建容器内安装。`MOONCAKE_DEPS_SHCA=1` 会避免通用依赖安装与
  SHCA 用户态 ibverbs 栈冲突。
- DTK 版本保持旧 CI：standard/shca 使用 26.04 rc4，rpc 使用 26.04.1 rc1。
  运行依赖安装仍使用官方 `dependencies.sh`；需允许其下载官方依赖和 Go 工具链。

## 3. 创建 hcu-ci 环境

Settings → Environments → New environment → `hcu-ci`。
检查分支策略允许 `official_code_ci`；验证 PR 时还需允许对应 PR merge ref。
需要人工准入时配置 required reviewers。所有硬件 job 都引用这个环境，
可能会在构建及后续测试阶段分别等待批准。

环境名称本身不会自动产生保护规则。公开 PR 会执行提交中的代码，
workflow 自身也可能被修改，因此官方接入需使用隔离、可清理的专用 runner，
并由管理员确认 runner 访问范围和 PR 执行策略；不能只依赖 YAML 中的环境声明。

## 4. 首次验证：提交、创建测试 PR、添加标签

在 `mooncake-das` 根目录执行，先确认当前分支和 diff：

```powershell
git branch --show-current
git diff --check
git status --short
git diff
```

确认只包含本次 HCU 改动后暂存、提交，然后推送：

```powershell
git add .github/workflows/ci_hygon.yaml .github/workflows/ci_hygon_validation.yml .github/workflows/ci_hygon_pr.yml scripts/hygon scripts/variant_build.sh scripts/build_wheel.sh dependencies.sh mooncake-common/common.cmake mooncake-common/include/rdma_lid.h mooncake-transfer-engine mooncake-store mooncake-wheel/pyproject.toml
git diff --cached --stat
git commit -m "[CI/Build] Add HCU validation on upstream code"
git push origin official_code_ci
```

这次 push 不会启动 HCU 硬件任务。然后从此分支创建一个临时测试分支，
修改本文的一处测试性说明并提交，推送该分支，在本仓库创建 PR：
base 为 `official_code_ci`，compare 为测试分支。确认入口和可复用 workflow
都已在此 PR 的代码中。创建仓库标签 `run-hygon-ci`，再给这个 PR 添加标签。
GitHub 若要求批准 Actions，先检查并批准；这与下面的环境审批是不同环节。

打开 Actions → `HCU branch validation` → 此次提交：

1. `Validate HCU configuration` 通过；缺少变量时按错误补齐。
2. 若出现环境审核，检查此次提交后批准。
3. standard、rpc、shca 均构建成功，产出三个 `hcu-wheel-*` artifact。
4. `TE-Cross-node test standard wheel` 成功，上传 TE 日志。
5. Store benchmark 成功，上传 `hcu-store-kv-benchmark-logs`。
6. `HCU PR Gate` 成功。仅构建成功或 job skipped 均不算完整通过。

环境修正后可在这次运行页面点击 Re-run all jobs，不需要制造源码修改。
若 workflow/脚本发生修改，提交并 push，再重新添加标签，才能验证新的代码。

## 5. 验证 PR 事件

在上述测试 PR 上记录以下结果：

1. 未加标签的 PR 和普通 push 不会执行 HCU 硬件任务。
2. 添加其他标签时 HCU job 跳过；添加 `run-hygon-ci` 才进入环境审核和测试。
3. 三种 wheel 构建、TE 和 Store 测试、HCU PR Gate 均成功，标签被移除。
4. 更新测试 PR 不自动启动 HCU；重新添加标签才验证新提交。
5. 构建 summary 的 SHA 等于加标签时的 PR head SHA。
6. 人工取消时确认双机容器和锁被释放；标签若未清除，手动移除后再运行。

验证完成后关闭测试 PR；无需把测试性说明修改合并回去。

## 常见定位

| 现象 | 排查方向 |
| --- | --- |
| 完全没有运行 | PR 目标分支、仓库名称、Actions 是否启用、是否添加了 run-hygon-ci 标签 |
| Waiting for a runner | Online 状态、标签数组、runner group 的仓库授权 |
| Waiting for review | `hcu-ci` environment 审核或分支准入 |
| SHCA 缺少头文件 | 查看资源服务器下载、`mlxtoshca.sh` 执行和安装后的文件检查 |
| 主节点地址检查失败 | 测试 runner 实际所在机器与 HCU_TEST_TARGET_HOST 不一致 |
| SSH 失败 | runner 用户的密钥、known_hosts、SSH 地址、网络及第二节点 Docker 权限 |
| 编译/链接失败 | 下载完整构建日志，确认是依赖安装、DTK API 还是移植兼容性问题 |
| Store 超时 | 看主/备 master、client、etcd 和 benchmark 日志，检查端口、内存和网卡过滤 |

## 上游交付边界

提交官方前排除 `ci_hygon_validation.yml` 这个本仓库验证入口，并整理本文中的
本地验证说明。保留可复用流程、官方 PR 入口和必要适配。官方管理员需配置
runner、变量、环境审核并开启 `HCU_CI_ENABLED`；必要检查需另外配置 branch
rules。该开关未开启时 HCU job 会跳过，不应宣称官方 HCU 已经接入成功。

本地静态检查不能证明 DTK 编译、wheel 动态链接或双机测试已经通过。
以实际 Actions 运行日志和最终 Gate 为准。

GitHub 参考：
[触发流程](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflows)、
[手动运行要求](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow)、
[环境审核](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow)。

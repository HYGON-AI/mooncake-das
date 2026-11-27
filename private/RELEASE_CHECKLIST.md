# 私有扩展发布检查清单 / Private-Extension Release Checklist

本清单同时适用于中文与英文发布评审。任何一项不满足都不得发布私有 `.so`。

This checklist applies to both Chinese and English release reviews. Do not ship
the private `.so` files unless every item passes.

## 1. 构建与 ABI / Build and ABI

- [ ] `cmake -S private -B private/build -DMOONCAKE_ENABLE_AHE=ON` 构建成功。
- [ ] `bash private/package_private_ext.sh` 在 `private/dist` 生成
      `libmooncake_dfe.so.1`；启用 AHE 时同时生成 `libmooncake_ahe.so.1` 与
      `mc_copy_kernel.co`，且三者同目录。
- [ ] 私有库 ABI 版本与公有 `mooncake_private_ext_abi.h` 一致
      （DFE v2、AHE v1）；不一致时公有侧必须安全降级。
- [ ] 符号收窄：`nm -D --defined-only private/dist/libmooncake_dfe.so.1`
      只暴露 `mooncake_dfe_extension_get`；AHE 同理只暴露
      `mooncake_ahe_extension_get`。
- [ ] `libmooncake_dfe.so.1` 不依赖设备运行时（`ldd` 无 HIP/CUDA）；
      `libmooncake_ahe.so.1` 仅依赖 HIP 运行时。

## 2. 部署布局 / Deployment Layout

- [ ] master 只需要 `libmooncake_dfe.so.1`；client 需要两个 provider。
- [ ] 两种解析方式均验证过：
      1. `MOONCAKE_EXT_DIR` 目录（按固定文件名查找）；
      2. 默认 `/opt/mooncake/plugins`。
- [ ] `MOONCAKE_ENABLE_EXT=0` 可整体关闭两个 provider。
- [ ] `mc_copy_kernel.co` 与 `libmooncake_ahe.so.1` 同目录。

## 3. 无 .so 降级 / Fallback Without Providers

- [ ] 启动日志出现 `dfe_provider=absent`，进程正常提供服务。
- [ ] 读、写、bucket 分配、mapped 分配全部走公有基线，功能正确、仅性能下降。
- [ ] master 无 DFE 时 bucket 使用旧逐 key 布局，且无 mtime ioctl 告警刷屏。
- [ ] client 无 AHE 时逐 range `hipMemcpyAsync` 回退生效。

## 4. 有 .so 冒烟 / Smoke Test With Providers

- [ ] 日志出现 `dfe_provider=loaded path=...`；HIP 环境出现 AHE 加载成功。
- [ ] `io_batch`（读/写）、`align_batch_offset`、`configure_write_fd`、
      `refresh_file_mtime`、`buffer_policy` 均被调用且有预期效果。
- [ ] ABI 不匹配（版本或 struct_size 不一致）时安全降级，不崩溃。
- [ ] 性能不低于拆分前的 HYGON-AI 树（记录 baseline benchmark）。

## 5. 保密 / Confidentiality

- [ ] 私有源码、`.so`、`.co` 均未进入公有提交与公有 wheel。
- [ ] 公有树不含私有常量、私有环境变量名与私有日志字符串（执行泄露审计）。
- [ ] 公有 CI 使用 fake provider 验证契约与降级，不包含真实算法。

## 6. 可替换性 / Replaceability

- [ ] DFE 与 AHE 可独立升级、独立回滚；一方 ABI 变化不影响另一方。
- [ ] 升级私有库前已确认公有侧 ABI 版本，不匹配时先升级公有。

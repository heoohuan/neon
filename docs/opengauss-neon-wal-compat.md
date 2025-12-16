# openGauss ↔ Neon WAL 兼容性研究与迁移方案（Phase 0）

作者: 自动生成文档（由 Copilot 助手生成）
日期: 2025-12-11

目的
- 本文档为将 Neon 的 Postgres 内核替换为 openGauss（重点关注 WAL 日志的解码、回放与兼容性）准备的研究与决策资料。
- Phase 0（groundwork）：不修改代码，仅做调研、收集工件并制定可执行迁移策略与 PoC 测试矩阵。

范围
- 只关注 WAL（XLOG）相关的编解码与回放适配，包括：XLog record 布局、WAL header、CLOG、Two‑phase/2PC、SMGR（页面/页头）以及 redo/apply 路径。
- 不在本阶段实现生产代码改动；但会制定清单与模板，供 Phase 1（编码）使用。

目录
- **一. 主要目标与产物**
- **二. 环境准备（推荐）**
- **三. 需要收集的工件**
- **四. 符号/结构比对清单**
- **五. 对比文档模板（表格化）与示例条目**
- **六. 研究步骤（逐条可执行）**
- **七. PoC 测试矩阵**
- **八. 风险评估与缓解措施**
- **九. 时间估计与分阶段优先级**
- **十. 交付物清单 & 后续步骤**

**一. 主要目标与产物**
- 目标：彻底理解 openGauss 与 Neon 在 WAL 层面的差异，形成可执行的兼容策略，优先保证事务可见性与数据一致性。
- 产物（Phase 0）：
  - `docs/opengauss-neon-wal-compat.md`（本文件）
  - `research/` 下的原始工件（hexdump、bindgen 错误日志、示例 WAL）
  - 差异表格（表格化的逐字段对比）
  - PoC 测试矩阵与验证脚本/命令清单

**二. 环境准备（推荐）**
- 原则：在 Linux 环境中做构建与 bindgen，避免 macOS 与 Linux 头文件/宏不兼容导致的 clang 解析错误。推荐使用 Colima（macOS 无 GUI 的 Docker 替代）。

- 快速命令（macOS，zsh）：
```bash
# 安装（若尚未安装 Homebrew/Colima）
brew install colima docker

# 启动 Colima（根据机器配置调整 cpu/memory）
colima start --cpu 4 --memory 8 --disk 50

# 验证 docker
docker version
docker info
```

- 构建 openGauss 的建议（概要）：
  - 在仓库中新建 `tools/opengauss-build/`，放置 `Dockerfile` 与 `build_opengauss.sh`。
  - 在容器内执行 openGauss 的 `configure`/`make`/`make install`（或相应的 build 流程），把 `include/` 与生成的 `pg_config.h`、`pg_config_os.h` 等安装到 `opengauss-install` 前缀目录（该目录挂载到宿主，便于后续 bindgen 使用）。
  - 在同一容器内运行 `cargo build -p postgres_ffi`，让 bindgen 与 clang 在产生 headers 的相同平台上运行。

**三. 需要收集的工件**
- openGauss 安装 include tree：`opengauss-install/include/*`（必须包含 configure 生成的头文件，例如 `pg_config.h`）
- 示例 WAL 段（openGauss 产生）：`pg_wal/0000000100000000...`（二进制）以及 `hexdump` 文本
- Neon 产生的 WAL 段示例
- bindgen 错误日志（如果有）
- `bindings_opengauss.rs`（若已生成）
- 小型测试集：一组事务（包含 heap insert/update/delete、btree 修改、clog 更新、2PC prepare/commit/abort、checkpoint）

收集文件示例命令（在容器或宿主）：
```bash
# 导出 include tree
tar -czf opengauss-install.tar.gz -C tools/opengauss-build opengauss-install

# 导出示例 WAL 段（hexdump）
hexdump -C pg_wal/000000010000000000000001 > out/wal_segment_0001.hexdump
```

**四. 符号/结构比对清单**
- 在 openGauss 与 Neon 两端需要对比的关键 symbol/struct/API：
  - XLog 相关：`XLogRecord`, `XLogRecPtr`, `XLogPageHeaderData`, `XLogRecordHeader`（或同义）
  - WAL header、timeline id、checksum、segment size
  - rmgr（resource manager）ID 与 record type 列表
  - CLOG（commit log）格式：页布局、每事务位的语义、拓展字段
  - Two‑phase（2PC）格式：prepare/commit/abort record 的字段与序列化方式
  - SMGR、page header 与 PD（page header）字段：`pd_lower`, `pd_upper`, `pd_flags` 等
  - WAL replay/redo 调度点：redo handler 调用约定、apply 顺序、锁/visibility 处理逻辑

建议搜索命令（在对应源码目录）：
```bash
rg "XLogRecord|XLogInsert|XLogRecPtr|clog|twophase|redo|smgr|pd_lower|pd_upper" -S
```

**五. 对比文档模板（表格化）**
在 `docs/opengauss-neon-wal-compat.md` 中我们用下表模板记录每个对比项（示例条目在后）：

| 项目 | Neon 位置 (文件/符号) | openGauss 位置 (文件/符号) | 字段/布局差异 | 语义差异 | 风险等级 | 迁移建议 |
|---|---|---|---|---|---:|---|

示例条目（XLogRecord header）：

| XLogRecord header | `open libs/wal_decoder` 中的 `XLogRecord` 解码代码 | `openGauss/src/include/xlogrecord.h`（示例） | 字段顺序相似，但 openGauss 在 header 中包含额外的 `xl_info` flags，alignment 可能不同 | 字段语义大体一致，但 `xl_info` 标志位意义需逐位比对 | High | 在 `wal_decoder` 中增加 openGauss-specific 解码器：基于字节偏移解析 header，避免直接依赖 C 结构体 layout |

（注：上表为模板，后续需把每一行细化为具体文件/行引用与字节偏移说明）

**六. 研究步骤（逐条，可执行）**
1. 环境准备：通过 Colima 启动 Linux 容器，构建 openGauss 并把 include tree 安装到宿主挂载路径（`tools/opengauss-build/opengauss-install`）。
2. 在同一容器内运行 `cargo build -p postgres_ffi` 来触发 bindgen：记录所有 clang 错误（若有），并把错误按缺失 typedef / 未定义宏 / 语义冲突分类。把错误输出保存在 `research/bindgen-errors.txt`。
3. 识别 openGauss 与 Postgres (Neon 依赖的 headers) 的差异点列表，优先级按照对回放语义影响排序（例如：CLOG/2PC > heap record payload > 次要 rmgr）。
4. 提取并归档样本 WAL 记录（至少每类一条），放入 `research/wal-samples/`，并生成 `hexdump` 版本以便对比。
5. 基于样本编写对比腳本或小程序（Python/Rust），逐字节解析样本并打印字段偏移与数值，用以验证表格中的字段偏移假设。
6. 完成 `docs/opengauss-neon-wal-compat.md` 的差异表格与迁移建议草稿（本文件即为模板，建议在 `docs/` 下继续维护）。

**七. PoC 测试矩阵**
- 单条 record 解析（Unit PoC）
  - 输入：openGauss 生成的 WAL record（二进制）
  - 操作：使用 `wal_decoder` 中的 openGauss 解码器解析
  - 验证：字段值与通过 openGauss 源代码解析得到的值一致（或与 hexdump 对齐）

- 事务可见性测试（Integration PoC）
  - 场景：执行事务（包含 2PC），产生 WAL，向 Neon pageserver 回放
  - 验证：回放后读取页面，确认事务可见性（是否 commit/abort 行为正确）

- 全链路回放（System PoC）
  - 场景：小型 openGauss 实例生成 N 个 WAL segment → 使用 Neon 的 pageserver 回放并校验数据完整性
  - 验证：checksum、tuple counts、索引一致性

每个 PoC 都需要列出：前置步骤、输入文件、运行命令、验证脚本与期望输出样例。

**八. 风险评估与缓解措施**
- Bindgen 在 macOS 上失败：缓解——在 Linux 容器中运行 bindgen（推荐）。
- openGauss 在 WAL 语义上存在重大差异：缓解——采用渐进兼容策略：优先实现对 CLOG/2PC 的解析与映射，再实现常见 rmgr。
- 性能退化：缓解——在 PoC 阶段收集基准并在必要时优化解析实现（例如使用零拷贝或快速字段抽取）。
- 许可证/合规性风险：缓解——检查 openGauss 的开源协议条款，评估与 Neon 许可的兼容性。

**九. 时间估计与分阶段优先级（粗略）**
- Phase 0（本阶段）：1–2 周（完成差异文档、环境准备、PoC 矩阵与风险评估）
- Phase 1（编码 PoC）：2–4 周（在 `wal_decoder` 中实现 openGauss-specific 解析、生成测试样例）
- Phase 2（集成）：2–6 周（pageserver 回放、safekeeper 集成、端到端测试）
- Phase 3（优化与提交）：2–4 周（性能优化、文档、CI 集成）

**十. 交付物清单 & 后续步骤**
- 本阶段交付物：
  - `docs/opengauss-neon-wal-compat.md`（本文件）
  - `tools/opengauss-build/`（可选：Dockerfile 与构建脚本）
  - `research/`（建议：errors, hexdumps, bindings）
- 推荐后续步骤：
  1. 在容器内生成 `opengauss-install` include tree 并运行 bindgen（得到 `bindings_opengauss.rs`）。
  2. 根据差异表先在 `libs/wal_decoder` 内实现 openGauss-specific 解析器（先做 CLOG/2PC）。
  3. 编写 PoC 测试并验证 replay 行为。

如需，我可以把 `tools/opengauss-build/Dockerfile` 和 `build_opengauss.sh` 写入仓库并生成一个初始差异表条目（基于仓库当前代码）。请回复是否需要我把示例脚本写入仓库。 

---
备注：本文件为 Phase 0 的模板与策略文档，是后续编码工作的基础。建议将研究过程中的每次发现都追加到 `research/` 目录中，并定期更新此文档以反映新发现。

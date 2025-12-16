# openGauss vs PostgreSQL XLogRecord 字节级映射（给 Neon 开发者）

本文档给出 openGauss 与 PostgreSQL（Neon 当前假定）的 XLogRecord 固定头字节布局对比、CRC 偏移差异、page-magic 差异，以及在 Neon 中实现最小兼容 shim（让 openGauss WAL 能够通过现有 pipeline）的具体改动建议与代码片段。

> 目的：生成一个可操作的字节级规范，指导你如何在 `libs/postgres_ffi` / `libs/wal_decoder` 中做最小改动，快速让 openGauss WAL 能被重组并进入上层解码/回放逻辑（阶段 1：兼容 shim）。

---

## 1. openGauss 固定头（来自 `openGauss/src/include/access/xlog_basic.h`）

typedef struct XLogRecord {
- uint32 xl_tot_len;       /* total len of entire record */
- uint32 xl_term;          /* openGauss term field */
- TransactionId xl_xid;    /* xact id (uint32) */
- XLogRecPtr xl_prev;      /* ptr to previous record in log (uint64) */
- uint8 xl_info;           /* flag bits */
- RmgrId xl_rmid;         /* resource manager (uint8) */
- uint2 xl_bucket_id;     /* bucket id (uint16) */
- pg_crc32c xl_crc;       /* CRC (uint32) */
}

按偏移（byte offsets，从 0 开始）：
- 0..3    : `xl_tot_len` (4 bytes)
- 4..7    : `xl_term` (4 bytes)
- 8..11   : `xl_xid` (4 bytes)
- 12..19  : `xl_prev` (8 bytes)
- 20      : `xl_info` (1 byte)
- 21      : `xl_rmid` (1 byte)
- 22..23  : `xl_bucket_id` (2 bytes)
- 24..27  : `xl_crc` (4 bytes)

因此 openGauss 固定头中 CRC 字段的起始偏移为 **24**（即 CRC 在字节 24..27）。

此外，openGauss 定义的 page magic（WAL page header 的 magic 字段）是：
- `#define XLOG_PAGE_MAGIC 0xD074`（见 `openGauss/src/include/access/xlog_basic.h`）

---

## 2. PostgreSQL（Neon 当前解码器假定的布局）

Neon 当前代码中用于计算 CRC 的常量在 `libs/postgres_ffi/src/xlog_utils.rs` 中定义：

- `pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2;` // 当前值 = 20

这对应的假定布局（按偏移）：
- 0..3    : `xl_tot_len` (4)
- 4..7    : `xl_xid` (4)
- 8..15   : `xl_prev` (8)
- 16      : `xl_info` (1)
- 17      : `xl_rmid` (1)
- 18..19  : `xl_bucket_id` (2)
- 20..23  : `xl_crc` (4)

因此 PostgreSQL 布局下 CRC 偏移为 **20**。注意：Neon code 中用到的 `XLOG_SIZE_OF_XLOG_RECORD` 会根据绑定自动计算，但 `XLOG_RECORD_CRC_OFFS` 是硬编码为 20 的表达式。

---

## 3. 关键差异（对回放与解码的影响）

- openGauss 在固定头中新增 `xl_term`（4 bytes）字段，**使 CRC 偏移从 20 -> 24**。如果使用原有偏移值会导致 CRC 校验失败（WAL record crc mismatch）。
- openGauss page magic = `0xD074`，如果 decoder 只接受 postgresql 的 magic 会在 page header 验证失败。需要在 header 验证中接受 openGauss 的常量或根据配置选择正确的值。
- openGauss `CLOG` rmgr 的 WAL payload 格式（例如 `CLOG_TRUNCATE` 中的 `pageno` 为 `int64`）在某些字段宽度上与 Postgres 实现可能不同；`libs/wal_decoder` 中对 CLOG 等的解码已有处理分支（注意 `decoder.rs` 中对 pg_version 的 32/64 位 pageno 处理）。

总结：最小可行路径是**在重组/CRC 校验与 page-magic 验证处做兼容判断**，使 openGauss WAL 可以被重组并提交给上层解码器，然后在专用 opengauss 解码器中逐步实现语义级的兼容（CLOG/truncate/two-phase 的字段对齐与解码）。

---

## 4. 在 Neon 中实现最小兼容 shim（具体改动建议）

下面列出最小改动点与示例代码片段（你可以按照此说明修改源码，先做最小 shim 验证管道通路）：

1) 在 `WalStreamDecoder` 中增加一个运行时字段 `wal_format`（或将 pg_version 扩展为包含 `WalFormat`），以便在解码时判断是 `Postgres` 还是 `OpenGauss`。

建议：新增类型：

```rust
// 新增枚举（放在 waldecoder 模块或与 WalStreamDecoder 同目录）
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WalFormat {
    Postgres,
    OpenGauss,
}
```

并在 `WalStreamDecoder::new(start_lsn, pg_version)` 的参数列表中添加 `wal_format: WalFormat`（向调用方逐步传递该参数；暂时可在调用处传 `WalFormat::Postgres` 以保持兼容）。

2) 修改 CRC 偏移计算（`libs/postgres_ffi/src/xlog_utils.rs`）

当前：

```rust
pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2; // = 20
```

建议：不要把偏移做为全局常量，而改为函数：

```rust
pub fn xlog_record_crc_offs(wal_format: WalFormat) -> usize {
    match wal_format {
        WalFormat::Postgres => 4 + 4 + 8 + 1 + 1 + 2, // 20
        WalFormat::OpenGauss => 4 + 4 + 4 + 8 + 1 + 1 + 2, // 24 (额外的 xl_term)
    }
}
```

然后在 `waldecoder_handler.rs` 的 `complete_record()` 中用 `xlog_record_crc_offs(self.wal_format)` 替代 `XLOG_RECORD_CRC_OFFS`。

示例片段（`complete_record` 中的 crc 计算处）

```rust
let offs = xlog_record_crc_offs(self.wal_format);
crc = crc32c_append(crc, &recordbuf[offs + 4..]);
crc = crc32c_append(crc, &recordbuf[0..offs]);
```

3) page magic 验证（`waldecoder_handler.rs::validate_page_header`）

当前代码使用 `XLOG_PAGE_MAGIC`（绑定值）。为了兼容 openGauss，可以在 `validate_page_header` 中接受两种 magic，或基于 `wal_format` 做判断：

```rust
if self.wal_format == WalFormat::OpenGauss {
    if hdr.xlp_magic != OPEN_GAUSS_XLOG_PAGE_MAGIC as u16 {
        return Err(...)
    }
} else {
    if hdr.xlp_magic != XLOG_PAGE_MAGIC as u16 {
        return Err(...)
    }
}
```

其中 `OPEN_GAUSS_XLOG_PAGE_MAGIC` 可以在一个新的常量模块中定义为 `0xD074`（或从 opengauss 头直接生成绑定）。

4) bindings / XLogRecord struct 大小注意

当前 `XLOG_SIZE_OF_XLOG_RECORD` 在 `xlog_utils.rs` 以 `size_of::<XLogRecord>()` 计算（依赖于 bindings）。这在 Postgres bindings 下是正确的，但 openGauss 的 XLogRecord 增加字段，理想做法是为 openGauss 生成单独 bindings（长期方案）。短期 shim：crc 偏移与 page magic 可在运行时处理，让 decoder 重组并把原始 bytes 传上去（通过 `NeonWalRecord::Postgres{ rec }` 或新的 `NeonWalRecord::Opengauss{ rec }`），上层再决定如何解析 record bytes（阶段 2 会实现专用解析器）。

5) 调用链改造点（需要更新的调用位置）

- `WalStreamDecoder::new(start_lsn, pg_version)` -> 新增参数 `wal_format`，并在所有调用点传递（`pageserver/src/walingest.rs`、`safekeeper`、`find_end_of_wal` 等）。这些都是较多调用点，但修改简单（向 `new` 传额外枚举）。

---

## 5. 最小操作流程（如何逐步做，命令与顺序）

1) 生成本文件（已创建）。
2) 在本地分支做小改：
   - 增加 `WalFormat` 枚举与 `WalStreamDecoder` 字段（和 `new()` 签名）
   - 在 `xlog_utils.rs` 新增 `xlog_record_crc_offs()` 函数并替代常量使用（并保留旧常量兼容，以免一次性改动太大）
   - 在 `waldecoder_handler.rs` 用 `self.wal_format` 调整 page magic 检查与 CRC 偏移读取
3) 运行单元测试（`cargo test -p postgres_ffi` 或相关），并用一个 openGauss WAL 样本（或手造的一个记录）验证 decoder 不再立刻报 CRC/魔术错误。

命令示例（在仓库根目录）：
```bash
# 新建本地分支
git checkout -b opengauss-wal-shim

# 编辑代码（根据上文建议）
cargo test -p postgres_ffi
```

如果一切通过，可以提交 PR。下一步建议是实现专用 `libs/wal_decoder_opengauss` 来完整解析 openGauss 固有字段并在高层输出兼容的 `NeonWalRecord`。

---

## 6. 要点回顾（工程优先级）

- 必须先处理：CRC 偏移与 page magic（否则 WAL 在低级就被拒绝）。
- 其次处理：生成/映射 openGauss 的 XLogRecord bindings 或在 opengauss 解码器中手动解析字段。短期可用 shim（如上）让数据流通起来。
- 然后实现：CLOG/truncate 的语义解码（映射为现有 `NeonWalRecord::Clog*`）、two‑phase 文件的读写回放。pageserver 已经实现了对 `ClogSetCommitted`/`ClogSetAborted`、`put_twophase_file`/`drop_twophase_file` 的逻辑，主要工作在解码器端将 openGauss WAL 映射为这些事件。

---

如果你愿意，我下一步可以：
- A1（立刻）：在仓库中提交“最小兼容 shim”补丁草案（修改 `WalStreamDecoder::new` 签名、`xlog_utils.rs` 增加函数、`waldecoder_handler.rs` 的 CRC 分支），或
- A2（辅导式）：把上面每一处修改写成补丁（diff），你可以 review 后自己应用。

请选择：我直接创建补丁草案，还是只提供补丁文本由你手动应用？

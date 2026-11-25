"""Chinese localization for the generated report.

The English report stays the single source of truth; this module maps its
lines to Chinese when `report.py --lang zh` is used. Anything not covered by a
rule is left as-is, so a new English line never breaks a report.
"""
import re

RULES = [
    (r"^# (.*) (\d+)-segment key-capacity test$",
     r"# \1 \2 段 key 规模测试"),
    (r"^Package: (.*?) \((.*)\)\. Raw samples and log summaries live in `(.*?)`\.$",
     r"包：\1（\2）。原始采样与日志摘要在 `\3`。"),
    (r"^Same write pressure as always: push resident keys to the per-segment object cap, "
     r"then watch (\d+) eviction rounds? and see whether BatchPutStart exceeds 30 seconds "
     r"before fill-up or during eviction\.$",
     r"同一套写入压力：把常驻 key 堆到段的对象个数上限，再看完 \1 轮驱逐，"
     r"确认 BatchPutStart 会不会在写满前或驱逐期间超过 30 秒。"),
    (r"^(.+?) per segment caps at about ([\d,]+) keys by `bytes/1024` "
     r"\(see the metrics table for the measured cap\)\.$",
     r"\1 段按 `bytes/1024` 折算最多 \2 个 key（实测上限见关键指标）。"),
    (r"^Segment size: n/a\.$", "段大小：未采集。"),
    (r"^## Test method$", "## 测试方法"),
    (r"^Write pressure:$", "写入压力："),
    (r"^- one writer process, (.*) threads, (.*) keys per batch, (.*)-byte values$",
     r"- 一个写入进程，\1 线程，每批 \2 个 key，value \3 字节"),
    (r"^- master with (.*) RPC threads; client RPC timeout 30s "
     r"\(longer calls are recorded as -901\)$",
     r"- Master \1 个 RPC 线程；客户端 RPC 超时 30 秒（超过即记 -901）"),
    (r"^- client_ttl=(.*), key lease (.*)$", r"- client_ttl=\1，key 租约 \2"),
    (r"^- eviction high watermark (.*), ratio (.*), allocation strategy (.*)$",
     r"- 驱逐高水位 \1，每轮比例 \2，分配策略 \3"),
    (r"^- HA disabled$", "- 不开 HA"),
    (r"^- HA enabled$", "- 开启 HA"),
    (r"^Master on (.*?), (.*?)\. Writer on (.*?), using (.*?); it is a pure client with "
     r"no segment, so master_active_clients only counts holders\.$",
     r"Master 在 \1，\2。写入在 \3，使用 \4，是纯客户端，不挂段；"
     r"master_active_clients 只统计 holder。"),
    (r"^Master on (.*?), (.*?)\. Writer on (.*?), using (.*?) and mounting one segment "
     r"\((.*?)\)\.$",
     r"Master 在 \1，\2。写入在 \3，使用 \4 并自己挂一段（\5）。"),
    (r"^## Node and segment layout$", "## 节点与段分配"),
    (r"^\| Node \| Role \| local_hostname \| Segment size \|$",
     "| 节点 | 角色 | local_hostname | 段大小 |"),
    (r"^## Why the object cap is hit first$", "## 为什么先撞对象数上限"),
    (r"^(.*)-byte values do not fill the segment byte capacity\. Segments hit the object "
     r"cap first, new writes return -200 and trigger eviction; the byte watermark is about "
     r"(.*)\.$",
     r"\1 字节 value 占不满段字节容量。段先到对象个数上限，新写入返回 -200 并触发驱逐；"
     r"字节水位约 \2。"),
    (r"^## Write and eviction$", "## 写入与驱逐"),
    (r"^Writing on (.*?) started at (.*?) \((.*?)\); it filled (.*?) keys at (.*?) and "
     r"triggered the first eviction round, about (.*?) after the start\.$",
     r"写入在 \1，\2 开始，填数速率 \3；\5 写满 \4 个 key 并触发第 1 轮驱逐，"
     r"距开始约 \6。"),
    (r"^\| Time \| Event \|$", "| 时刻 | 事件 |"),
    (r"^(.*?) \(container (.*?)\); writing starts \((.*) threads, (.*) keys per batch, "
     r"(.*)-byte values\)$",
     r"\1（容器 \2），开始写入（\3 线程，每批 \4 个 key，value \5 字节）"),
    (r"^round (\d+) \[EVICT-DONE\]$", r"第 \1 轮 [EVICT-DONE]"),
    (r"^\| Round \| Duration \| Keys deleted \|$", "| 轮次 | 耗时 | 删除的 key 数 |"),
    (r"^\| round (\d+) \| (.*?) \| (.*?) \|$", r"| 第 \1 轮 | \2 | \3 |"),
    (r"^† This delta spans adjacent rounds.*$",
     "† 该行的差值覆盖了相邻轮次（采样间隔大于单轮耗时，无法按轮拆分）。"),
    (r"^No paired `\[EVICT-TRIGGER\]` / `\[EVICT-DONE\]` was observed; eviction never "
     r"started\.$",
     "master.log 中没有成对的 `[EVICT-TRIGGER]` / `[EVICT-DONE]`，未进入驱逐。"),
    (r"^(\d+) rounds, ([\d,]+) keys deleted in total \(sampler cumulative\)\.$",
     r"\1合计删除 \2 个 key（sampler 累计值）。"),
    (r"^(\d+) rounds: total deleted n/a\.$", r"\1合计删除：未采集。"),
    (r"^Writer succeeded about (.*) times and failed about (.*) times; failure codes: (.*)\.$",
     r"写入成功约 \1 次，失败约 \2 次，失败构成：\3。"),
    (r"^Writer succeeded / failed: n/a\.$", "写入成功 / 失败：未采集。"),
    (r"^No RPC timeout \(-901\): every failure is -200 \(per-segment object cap\); "
     r"loader log has no Connection timed out\.$",
     "没有 RPC 超时（-901）：失败全部是 -200（段对象数上限），loader 日志无 "
     "Connection timed out。"),
    (r"^No RPC timeout \(-901\): see the failure codes above; loader log has no "
     r"Connection timed out\.$",
     "没有 RPC 超时（-901）：失败构成见上，loader 日志无 Connection timed out。"),
    (r"^No RPC timeout \(-901\): loader log has no Connection timed out\.$",
     "没有 RPC 超时（-901）：loader 日志无 Connection timed out。"),
    (r"^RPC timeouts \(-901\) present: -901=(.*) \((\d+) batches x (.*)\), Connection "
     r"timed out=(\d+) lines; before fill-up (.*), during eviction (.*)\.$",
     r"存在 RPC 超时（-901）：-901=\1（\2 批 × \3），Connection timed out=\4 行；"
     r"其中写满前 \5，驱逐期间 \6。"),
    (r"^(\d+) segments online (.*) \((\d+) holders; the writer mounts none and is not "
     r"counted; master_active_clients peak (.*)\)\.$",
     r"\1 个段在线\2（\3 个 holder；writer 不挂段，不计入；master_active_clients 峰值 \4）。"),
    (r"^Online data clients: n/a\.$", "数据端在线情况：未采集。"),
    (r"^## What the observations show$", "## 现象定性"),
    (r"^- No -901; the loader log has (\d+) `Connection timed out` line\(s\) and no "
     r"start-line phase boundary to split them by\.$",
     r"- 没有 -901；loader 日志有 \1 行 `Connection timed out`，没有阶段分界可拆。"),
    (r"^- No -901 and no Connection timed out at any point\.$",
     "- 全程没有 -901，也没有 Connection timed out。"),
    (r"^- All -901 happened before fill-up: (.*) accumulated before `\[EVICT-TRIGGER\]` "
     r"\((.*)\), and none were added through round (\d+) `\[EVICT-DONE\]` \((.*)\)\.$",
     r"- -901 全部落在写满之前：`[EVICT-TRIGGER]`（\2）之前累计 \1，"
     r"之后到第 \3 轮 `[EVICT-DONE]`（\4）没有新增。"),
    (r"^- -901 kept growing during eviction \((.*)\), so writes are contending with "
     r"eviction/cleanup on locks\.$",
     r"- 驱逐期间仍有新增 -901（\1），说明写入与驱逐/清理在抢锁。"),
    (r"^- master_active_clients peak (\d+) \(= (\d+) holders\), (.*)\.$",
     r"- master_active_clients 峰值 \1（= holder 数 \2），\3。"),
    (r"^## Key metrics$", "## 关键指标"),
    (r"^\| Item \| Reference \| Measured \|$", "| 项 | 参考 | 实测 |"),
    (r"^\| Resident key cap \| (.*) \| (.*) \|$", r"| 常驻 key 上限 | \1 | \2 |"),
    (r"^\| master RSS peak \| (.*) \| (.*) \|$", r"| master 峰值 RSS | \1 | \2 |"),
    (r"^\| BatchPutStart timeout \(-901\) \| (.*) \| (.*) \|$",
     r"| BatchPutStart 超时 (-901) | \1 | \2 |"),
    (r"^\| Segment object cap \(-200\) \| (.*) \| (.*) \|$",
     r"| 段对象数上限 (-200) | 写满后出现 | \2 |"),
    (r"^## Conclusion$", "## 结论"),
    (r"^- Writing started at (.*?); it filled (.*?) keys at (.*?) \(above the (.*?)-per-segment "
     r"estimate of (.*?) from plan; the measured per-segment cap is higher\), taking about "
     r"(.*?)\.$",
     r"- \1 开始写入，\3 写满 \2 个 key（超过 plan 按 \4/段 估算的 \5，"
     r"实测每段对象上限更高），填数用时约 \6。"),
    (r"^- Writing started at (.*?); it filled (.*?) keys at (.*?) \(plan estimated a cap of "
     r"(.*?)\), taking about (.*?)\.$",
     r"- \1 开始写入，\3 写满 \2 个 key（plan 估算上限 \4），填数用时约 \5。"),
    (r"^- Resident keys peaked at (.*) \(eviction never triggered\)\.$",
     r"- 常驻 key 峰值 \1（没有触发驱逐）。"),
    (r"^- All (\d+) eviction rounds? completed, (.*?) to (.*?) per round; (.*?) keys "
     r"deleted in total\.$",
     r"- \1 轮驱逐全部完成，单轮耗时 \2～\3；合计删除 \4 个 key。"),
    (r"^- All (\d+) eviction rounds? completed; (.*?) keys deleted in total\.$",
     r"- \1 轮驱逐全部完成；合计删除 \2 个 key。"),
    (r"^- No paired `\[EVICT-TRIGGER\]` / `\[EVICT-DONE\]` was observed; eviction never "
     r"started\.$",
     "- 没有观察到成对的 `[EVICT-TRIGGER]` / `[EVICT-DONE]`，未进入驱逐。"),
    (r"^- -901 total (.*) \((\d+) batches x (.*)\), Connection timed out (\d+) lines; "
     r"split at the first `\[EVICT-TRIGGER\]` \((.*)\): before fill-up (.*), during "
     r"eviction (.*)\.$",
     r"- -901 共 \1（\2 批 × \3），Connection timed out \4 行；"
     r"以首个 `[EVICT-TRIGGER]`（\5）为界：写满前 \6，驱逐期间 \7。"),
    (r"^  - The timeouts are concentrated between (.*?) and (.*?); none appeared through "
     r"round (\d+) `\[EVICT-DONE\]` \((.*)\)\.$",
     r"  - 超时集中在 \1—\2 这一批，之后到第 \3 轮 `[EVICT-DONE]`（\4）未再出现。"),
    (r"^- -200 total (.*), all after fill-up \(segment object cap exhausted\)\.$",
     r"- -200 共 \1，出现在写满之后（段对象数用尽）。"),
    (r"^- master_active_clients peak (\d+) \(= (\d+) holders\), (.*?); master RSS peak "
     r"(.*?), node available low (.*?)\.$",
     r"- master_active_clients 峰值 \1（= holder 数 \2），\3；master RSS 峰值 \4，"
     r"节点 available 最低 \5。"),
    (r"^## Reproduce$", "## 复现命令"),
    (r"^## Data sources and calculations$", "## 数据来源与计算"),
    (r"^- `run\.env`: WORKERS/BATCH_SIZE/VALUE_SIZE/CLIENT_TTL/LEASE/SEGMENT_SIZE and so on$",
     "- `run.env`：WORKERS/BATCH_SIZE/VALUE_SIZE/CLIENT_TTL/LEASE/SEGMENT_SIZE 等"),
    (r"^- `run\.env`: (.*)$", r"- `run.env`：\1"),
    (r"^- `nodes\.conf`: role/IP/devices/segment size \(columns 5 and 6 optional\)$",
     "- `nodes.conf`：角色/IP/设备/每段大小（第 5、6 列可选）"),
    (r"^- Segment size precedence: `nodes\.conf` column 6 -> `run\.env` "
     r"`SEGMENT_SIZE_<ROLE>` -> `SEGMENT_SIZE` -> command-line default$",
     "- 段大小优先级：`nodes.conf` 第 6 列 -> `run.env` 的 `SEGMENT_SIZE_<ROLE>` -> "
     "`SEGMENT_SIZE` -> 命令行默认值"),
    (r"^- `master\.log`: `\[EVICT-TRIGGER\]` / `\[EVICT-DONE\]` plus timestamps "
     r"\(glog format `(.*)`\)$",
     r"- `master.log`：`[EVICT-TRIGGER]` / `[EVICT-DONE]` 及时间戳（glog 格式 `\1`）"),
    (r"^- `sampler\.csv`: timeline of key_count / active_clients / master_rss / "
     r"evicted_key_count / mem_available$",
     "- `sampler.csv`：key_count / active_clients / master_rss / evicted_key_count / "
     "mem_available 的时间线"),
    (r"^- `loader\.log`: progress JSON lines \(attempted/succeeded/failed/result_codes/"
     r"key_qps/time\)$",
     "- `loader.log`：progress JSON 行（attempted/succeeded/failed/result_codes/"
     "key_qps/time）"),
    (r"^- Missing data is always written as `(.*)`; never invent numbers$",
     r"- 缺失数据一律写 `\1`，不要编造"),
    (r"^- Window: write start -> round (\d+) EVICT-DONE \(sample interval (\d+)s\); "
     r"leftover samples after the window \(test not stopped, or the next run reusing the "
     r"same master port\) are ignored$",
     r"- 统计窗口：写入开始 -> 第 \1 轮 EVICT-DONE（采样间隔 \2s）；窗口之后的残留采样"
     r"（测试未停止、或下一套测试复用同一 master 端口）不计入"),
    (r"^- Round table: durations pair the i-th `\[EVICT-TRIGGER\]` with the i-th "
     r"`\[EVICT-DONE\]`; deletions come from that round's `\[EVICT-RESULT\] evicted_count=` line; "
     r"only when that line is missing does the table fall back to the `evicted_key_count` delta "
     r"between the samples around the round \(`(.*)`, or a †-marked delta that spans adjacent "
     r"rounds\)$",
     r"- 轮次表：耗时按顺序配对第 i 个 `[EVICT-TRIGGER]` 到第 i 个 `[EVICT-DONE]`；删除数取该轮"
     r"`[EVICT-RESULT] evicted_count=` 行；只有该行缺失时才退回用该轮前后 sampler 的 "
     r"`evicted_key_count` 差值（不足时写 `\1`，或用 † 标记覆盖相邻轮次的差值）"),
    (r"^- Total deletions sum the per-round `\[EVICT-RESULT\] evicted_count=` values; when those "
     r"are missing it falls back to the cumulative `evicted_key_count` increase inside the window$",
     "- 合计删除数取各轮 `[EVICT-RESULT] evicted_count=` 之和；缺失时退回窗口内 "
     "`evicted_key_count` 的累计增量"),
    (r"^- Phase split: the first `\[EVICT-TRIGGER\]` is the boundary; `-901` accumulated "
     r"before it counts as before fill-up, anything added after it counts as during "
     r"eviction$",
     "- 阶段划分：取 `[EVICT-TRIGGER]` 首次出现的时刻为界，loader progress 里该时刻之前"
     "累计的 `-901` 记为「写满前」，之后新增的记为「驱逐期间」"),
    (r"^- RSS / available / resident keys in the timeline come from the nearest sampler "
     r"sample \(interval (\d+)s\)$",
     r"- 时间线里的 RSS / available / 驻留 key 取该时刻最近的 sampler 采样（间隔 \1s）"),
    (r"^- `-901` batches = total `-901` / `BATCH_SIZE` \(one BatchPutStart per batch\)$",
     "- `-901` 的批数 = `-901` 总数 / `BATCH_SIZE`（每次 BatchPutStart 一批）"),
    (r"^- master\.log has (\d+) TRIGGER and (\d+) DONE events; this table covers the first "
     r"(\d+) rounds only$",
     r"- master.log 共 \1 次 TRIGGER、\2 次 DONE，本表只统计前 \3 轮"),
]

_COMPILED = [(re.compile(p), r) for p, r in RULES]


def localize(text):
    """Translate an English report line by line; unknown lines stay English."""
    out = []
    for line in text.split("\n"):
        for pattern, repl in _COMPILED:
            new = pattern.sub(repl, line)
            if new != line:
                line = new
                break
        out.append(line)
    return "\n".join(out)

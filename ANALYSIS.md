# Moka 写入、维护与旧代 WriteOp 安全分析

适用代码：当前工作树（基线 commit `2474ad9`，并包含本次新增回归测试）。所有路径均以 `cargo test --features 'sync future'` 同时启用两个实现为准。

## 结论

旧的 `WriteOp::Upsert` 在 key 已被驱逐或显式删除、之后又重新插入后到达维护循环时，不能修改或删除新的 CHT entry。保护不是单独依赖一个递增数字，而是三层身份共同完成：

1. **CHT 身份**：每个逻辑 key 对应一个 bucket，bucket 中保存当前 `MiniArc<ValueEntry>`；删除是 tombstone CAS，重新插入得到新的 `EntryInfo`/`DeqNodes` 身份。
2. **生命周期身份**：成功的 CHT unlink 在 post-CAS 回调中立即调用 `EntryInfo::retire()`；旧 op 持有的 `EntryInfo` 永久为 retired，新插入 entry 的 `EntryInfo` 初始为 alive。
3. **代数与条件 CAS**：更新共享同一 `EntryInfo` 时递增 `entry_gen`，维护侧比较 `entry_gen`、时间戳和（在 admission reject/超重路径）`MiniArc::ptr_eq`；重新插入后的第一代编号可能与旧第一代同为 `1`，所以数字代数只解决“同一身份内的旧更新”，不能单独解决“删除后复用 key”。

因此，仅检查 `WriteOp.entry_gen == 当前 entry_gen` 不足以保证安全；旧代场景的关键是 **指针身份 + retired 单调位 + CHT 删除条件 CAS**。

## 身份模型

### EntryInfo 与 ValueEntry

- `ValueEntry { value, info, nodes }` 是 CHT 中保存的值对象；`new()` 建立新的 `EntryInfo` 和 `DeqNodes`，`new_from()` 在替换值时复用旧 `info/nodes`：`src/common/concurrent.rs:173`、`src/common/concurrent.rs:180`、`src/common/concurrent.rs:191`。
- `EntryInfo` 持有 `is_admitted`、单调的 `is_retired`、`entry_gen`、`policy_gen`、packed expiration state、时间戳和 policy weight：`src/common/concurrent/entry_info.rs:8`。
- `entry_gen` 初始为 `1`、`policy_gen` 初始为 `0`：`src/common/concurrent/entry_info.rs:115`。
- 替换同一 CHT entry 时，`new_value_entry_from()` 复用旧 `EntryInfo`，调用 `incr_entry_gen()`，新 `ValueEntry` 继续共享同一组 deque/timer 节点：`src/sync/base_cache.rs:651`、`src/sync/base_cache.rs:658`、`src/sync/base_cache.rs:661`；future 对应 `src/future/base_cache.rs:781`、`src/future/base_cache.rs:788`、`src/future/base_cache.rs:791`。
- 删除后重新插入走 `new_value_entry()`，分配新的 `EntryInfo`，代数重新从 `1` 开始：`src/sync/base_cache.rs:636`、`src/sync/base_cache.rs:645`；future 对应 `src/future/base_cache.rs:766`、`src/future/base_cache.rs:775`。
- `retire()` 只允许 alive 到 retired 的单调 Release store；读取用 Acquire：`src/common/concurrent/entry_info.rs:141`、`src/common/concurrent/entry_info.rs:168`、`src/common/concurrent/entry_info.rs:173`。
- `is_dirty()` 比较 `entry_gen != policy_gen`，用于跳过尚未被维护层追上的替换/插入：`src/common/concurrent/entry_info.rs:181`。
- expiration time 和 12-bit `expiry_gen` 打包在一个 atomic u64 中，CAS 一起更新：`src/common/concurrent/entry_info.rs:86`、`src/common/concurrent/entry_info.rs:260`。

### WriteOp / ReadOp

- `ReadOp::Hit` 携带被读取时的 `MiniArc<ValueEntry>`；miss 只携带 hash：`src/common/concurrent.rs:303`。
- `WriteOp::Upsert` 携带 `KeyHash`、旧/新 weight 和当时的 `entry_gen`；`WriteOp::Remove` 携带被删除的 `KvEntry` 与删除后递增的代数：`src/common/concurrent.rs:312`。
- op 只是 CHT 已提交变更到策略层的异步镜像，不负责让 map 生效。

### CHT 的线性化点

- get 遍历 bucket，调用用户谓词和 entry callback：`src/cht/segment.rs:282`；底层读取在 `src/cht/map/bucket_array_ref.rs:21`，实际拿到当前 bucket 后执行 `with_entry`：`src/cht/map/bucket_array_ref.rs:38`。
- insert/modify 的 bucket CAS 在 `src/cht/map/bucket.rs:266`，成功返回 previous bucket：`src/cht/map/bucket.rs:276`；上层 post-success callback 在 `src/cht/map/bucket_array_ref.rs:228` 成功后执行，old entry callback 在 `src/cht/map/bucket_array_ref.rs:239`。
- remove 先在 condition 中重新验证当前 entry：`src/cht/map/bucket.rs:145`，再将 bucket 替换为 tombstone：`src/cht/map/bucket.rs:154`、`src/cht/map/bucket.rs:156`。
- remove 的 post-success callback 只在 CAS 成功后运行：`src/cht/map/bucket_array_ref.rs:92`、`src/cht/map/bucket_array_ref.rs:103`；moka 在这里调用 `retire()`，使生命周期位与 unlink 同一个临界顺序汇合：sync `src/sync/base_cache.rs:1113`、`src/sync/base_cache.rs:1118`，future `src/future/base_cache.rs:1256`、`src/future/base_cache.rs:1261`。

## 同步实现调用链

### sync::insert

入口：`moka::sync::Cache::insert`（`src/sync/cache.rs:1476`）。

1. hash 并包装 `Arc<K>`：`src/sync/cache.rs:1477`。
2. `insert_with_hash()` 调 `BaseCache::do_insert_with_hash()`：`src/sync/cache.rs:1482`、`src/sync/cache.rs:1487`。
3. 若配置 listener，先获取 per-key parking-lot mutex，使替换/删除 listener 与同 key 的更新串行：`src/sync/base_cache.rs:494`、`src/sync/base_cache.rs:495`。
4. CHT `insert_with_or_modify()` 是 map 可见性的线性化点：`src/sync/base_cache.rs:512`。
   - insert 分支创建新 `ValueEntry` 和 gen=1 的 `WriteOp`：`src/sync/base_cache.rs:516`、`src/sync/base_cache.rs:517`、`src/sync/base_cache.rs:518`。
   - modify 分支保存 old info，创建新 `ValueEntry`，复用旧 `EntryInfo` 并递增 gen：`src/sync/base_cache.rs:523`、`src/sync/base_cache.rs:530`、`src/sync/base_cache.rs:531`、`src/sync/base_cache.rs:532`。
5. CHT 乐观锁可能重试 closure；op 计数器只让最后一次 closure 产生的 op 进入通道：`src/sync/base_cache.rs:504`、`src/sync/base_cache.rs:510`、`src/sync/base_cache.rs:539`。
6. 自定义 `Expiry::expire_after_create/update` 在 CHT 已提交后、入队前写 per-entry expiration state：`src/sync/base_cache.rs:551`、`src/sync/base_cache.rs:565`、`src/sync/base_cache.rs:668`。
7. 替换通知在 map 替换之后、`WriteOp` 返回前执行：`src/sync/base_cache.rs:600`、`src/sync/base_cache.rs:601`。
8. `schedule_write_op()` 先尝试触发维护，满了则 busy-loop/sleep 后重试运行：`src/sync/cache.rs:1489`、`src/sync/cache.rs:1819`、`src/sync/cache.rs:1832`、`src/sync/cache.rs:1838`。
9. housekeeper 的 run lock 在 `src/common/concurrent/housekeeper.rs:105` 获取；真正 drain 在 `src/sync/base_cache.rs:1190`。
10. drain 先 read channel 后 write channel：`src/sync/base_cache.rs:1218`、`src/sync/base_cache.rs:1223`。`apply_writes()` 解构 op 并调用 `handle_upsert()`：`src/sync/base_cache.rs:1422`、`src/sync/base_cache.rs:1437`、`src/sync/base_cache.rs:1443`。
11. `handle_upsert()` 第一道 retired 检查：`src/sync/base_cache.rs:1515`。若旧 entry 已从 CHT unlink，只同步 `policy_gen` 后返回，不触碰 timer/deque：`src/sync/base_cache.rs:1516`。
12. 若 entry 已 admitted，按同身份更新移动 deque/timer 并设置 policy gen：`src/sync/base_cache.rs:1523`-`src/sync/base_cache.rs:1530`。
13. 新 entry admission 调 `handle_admit()`；第二道 retired 检查在 `src/sync/base_cache.rs:1785`，随后更新计数、timer、AO deque、可选 write-order deque 和 admitted 位：`src/sync/base_cache.rs:1793`、`src/sync/base_cache.rs:1795`、`src/sync/base_cache.rs:1798`、`src/sync/base_cache.rs:1803`、`src/sync/base_cache.rs:1806`。
14. 超重或 TinyLFU rejection 删除 CHT candidate 时，条件同时要求 `EntryInfo` 指针相等和当前 entry gen 相等：`src/sync/base_cache.rs:1551`-`src/sync/base_cache.rs:1557`、`src/sync/base_cache.rs:1651`-`src/sync/base_cache.rs:1657`。这保证延迟 reject 不会删除复用 key 后的新身份。

### sync::get

入口：`moka::sync::Cache::get`（`src/sync/cache.rs:795`）。

1. 进入 `BaseCache::get_with_hash()`：`src/sync/cache.rs:799`。
2. CHT 读在 `BaseCache::do_get_with_hash()` 中发起：`src/sync/base_cache.rs:265`、`src/sync/base_cache.rs:284`。
3. 读线性化点是 CHT bucket load：上层 `src/sync/base_cache.rs:286`，底层 `src/cht/map/bucket_array_ref.rs:38`。callback 立即过滤 per-entry TTL、TTL/TTI、`invalidate_all` 和谓词失效：`src/sync/base_cache.rs:297`、`src/sync/base_cache.rs:300`。
4. 命中后，可变 `Expiry::expire_after_read` 在持有 CHT 返回的 `MiniArc<ValueEntry>` 时执行：`src/sync/base_cache.rs:315`、`src/sync/base_cache.rs:346`。
5. `last_accessed` 在返回前更新：`src/sync/base_cache.rs:357`。值 clone 到 public `Entry`：`src/sync/base_cache.rs:359`、`src/sync/base_cache.rs:365`。
6. read 线性化后异步入 `ReadOp::Hit`，满通道直接丢弃：`src/sync/base_cache.rs:360`、`src/sync/base_cache.rs:364`、`src/sync/base_cache.rs:467`、`src/sync/base_cache.rs:474`、`src/sync/base_cache.rs:476`。
7. drain 时先更新频率，再检查 op 中 entry 是否仍然 alive；retired 则不移动 deque、不重排 timer：`src/sync/base_cache.rs:1402`-`src/sync/base_cache.rs:1410`。
8. alive 时，仅当 read callback 修改了 expiry 才更新 timer wheel；之后移动 AO deque：`src/sync/base_cache.rs:1411`-`src/sync/base_cache.rs:1414`。

### sync::invalidate

单 key 入口：`moka::sync::Cache::invalidate`（`src/sync/cache.rs:1554`）。

1. `remove_entry()` 走 CHT 条件删除：`src/sync/cache.rs:1601`、`src/sync/base_cache.rs:1106`。
2. CHT tombstone CAS 是 map 消失的线性化点：`src/cht/map/bucket.rs:156`；post-CAS retirement 在 `src/sync/base_cache.rs:1118`。
3. map 删除后递增旧 `EntryInfo.entry_gen`，给延迟 WriteOp/ReadOp 留下同身份代数：`src/sync/cache.rs:1606`、`src/sync/cache.rs:1607`。
4. listener 通知在 key lock 内、策略 `WriteOp::Remove` 入队前执行：`src/sync/cache.rs:1609`、`src/sync/cache.rs:1610`；入队前释放 key lock 以防满队列与维护线程互锁：`src/sync/cache.rs:1612`-`src/sync/cache.rs:1617`。
5. `WriteOp::Remove` 携带 retired entry 与新 gen：`src/sync/cache.rs:1625`；调度在 `src/sync/cache.rs:1630`。
6. drain 到 remove 后进入 `handle_remove()`：`src/sync/base_cache.rs:1454`、`src/sync/base_cache.rs:1458`。函数摘除 timer、deque，扣减策略计数并同步 policy gen：`src/sync/base_cache.rs:1874`、`src/sync/base_cache.rs:1882`、`src/sync/base_cache.rs:1905`-`src/sync/base_cache.rs:1915`。
7. `invalidate_all()` 不立即删 map，只设置 `valid_after`：`src/sync/cache.rs:1658`、`src/sync/base_cache.rs:404`-`src/sync/base_cache.rs:406`。后续 get 过滤旧 entry，维护循环经 write/access order deque 删除并产生 `Explicit`：`src/sync/base_cache.rs:2216`-`src/sync/base_cache.rs:2218`、`src/sync/base_cache.rs:2244`、`src/sync/base_cache.rs:2252`。
8. `invalidate_entries_if()` 注册谓词：`src/sync/base_cache.rs:409`；get 即时过滤：`src/sync/invalidator.rs:143`；维护扫描以 snapshot `last_modified == ts` 防止删掉扫描后更新的 entry：`src/sync/invalidator.rs:269`、`src/sync/invalidator.rs:279`；删除条件和 retire 在 `src/sync/invalidator.rs:309`-`src/sync/invalidator.rs:321`，listener cause 为 `Explicit`：`src/sync/invalidator.rs:324`-`src/sync/invalidator.rs:327`。

## Future 实现调用链

### future::insert

入口：`moka::future::Cache::insert`（`src/future/cache.rs:1337`）。

1. hash 并包装 key：`src/future/cache.rs:1338`-`src/future/cache.rs:1340`。
2. `insert_with_hash()` 调 `do_insert_with_hash()`：`src/future/cache.rs:1810`、`src/future/cache.rs:1815`。
3. 插入前先 drain interrupted channel，保证取消的 listener future / write op 最终重放：`src/future/base_cache.rs:482`、`src/future/base_cache.rs:701`。
4. listener 启用时使用 async per-key lock：`src/future/base_cache.rs:490`-`src/future/base_cache.rs:496`。
5. CHT map 线性化仍是同步的 lock-free CAS：`src/future/base_cache.rs:512`；insert/modify op 构造分别在 `src/future/base_cache.rs:516`-`src/future/base_cache.rs:518` 和 `src/future/base_cache.rs:523`-`src/future/base_cache.rs:532`。
6. 自定义 expiry 与替换 listener future 的处理在 `src/future/base_cache.rs:552`、`src/future/base_cache.rs:566`；listener future 被 `CancelGuard` 保存并 await，调用者取消时可稍后重放：`src/future/base_cache.rs:604`-`src/future/base_cache.rs:625`。
7. 公共同步返回前，`insert_with_hash()` 也设置 CancelGuard：`src/future/cache.rs:1816`、`src/future/cache.rs:1817`。
8. `schedule_write_op()` 先尝试 `try_run_pending_tasks()`，满时短 spin，四次后 await `write_op_ch_ready_event`：`src/future/base_cache.rs:633`、`src/future/base_cache.rs:657`、`src/future/base_cache.rs:670`-`src/future/base_cache.rs:694`。
9. future housekeeper 用 async mutex 串行维护任务：`src/future/housekeeper.rs:120`；正在执行的 shared boxed future 保存在锁内，取消后下次恢复：`src/future/housekeeper.rs:167`-`src/future/housekeeper.rs:186`。
10. drain read/write 在 `src/future/base_cache.rs:1341`-`src/future/base_cache.rs:1350`；upsert 入口和 retired 首道检查在 `src/future/base_cache.rs:1569`、`src/future/base_cache.rs:1640`、`src/future/base_cache.rs:1660`。
11. admission 第二道检查在 `src/future/base_cache.rs:1944`；timer/deque/admitted 更新在 `src/future/base_cache.rs:1952`-`src/future/base_cache.rs:1965`。
12. 超重/拒绝路径同样要求 `EntryInfo` 指针和 entry gen 双匹配：`src/future/base_cache.rs:1700`-`src/future/base_cache.rs:1706`、`src/future/base_cache.rs:1808`-`src/future/base_cache.rs:1814`。

### future::get

入口：`moka::future::Cache::get`（`src/future/cache.rs:875`）。

1. 调 `BaseCache::get_with_hash(..., record_read = true)`：`src/future/cache.rs:881`。
2. 首先重放 interrupted ops：`src/future/base_cache.rs:259`、`src/future/base_cache.rs:260`。
3. CHT 读与过期/谓词过滤在同一个 callback 内：`src/future/base_cache.rs:265`-`src/future/base_cache.rs:285`。
4. 可变 `Expiry::expire_after_read` 与 `last_accessed` 更新也在 callback 内：`src/future/base_cache.rs:289`-`src/future/base_cache.rs:332`。
5. callback 返回 clone 值和可选 ReadOp；op 在 callback 外 await 发送，read channel 满时丢弃：`src/future/base_cache.rs:335`-`src/future/base_cache.rs:345`、`src/future/base_cache.rs:349`-`src/future/base_cache.rs:354`、`src/future/base_cache.rs:461`-`src/future/base_cache.rs:471`。
6. 维护侧先增加频率，retired read hit 不触碰 timer/deque：`src/future/base_cache.rs:1549`-`src/future/base_cache.rs:1557`；alive hit 的 timer/AO 更新在 `src/future/base_cache.rs:1558`-`src/future/base_cache.rs:1561`。

### future::invalidate

单 key 入口：`moka::future::Cache::invalidate`（`src/future/cache.rs:1350`）。

1. 进入 `invalidate_with_hash()`：`src/future/cache.rs:1354`、`src/future/cache.rs:1922`。
2. 先重放 interrupted ops：`src/future/cache.rs:1933`。
3. CHT 删除和 post-CAS retire 复用同步 CHT：`src/future/base_cache.rs:375`、`src/future/base_cache.rs:1256`-`src/future/base_cache.rs:1262`。
4. 删除后递增 entry gen 并克隆 remove op：`src/future/cache.rs:1970`-`src/future/cache.rs:1976`。
5. listener future 被放入 CancelGuard 并 await：`src/future/cache.rs:1986`-`src/future/cache.rs:1995`；之后释放 key lock：`src/future/cache.rs:2000`-`src/future/cache.rs:2005`。
6. remove op 自身也放入 CancelGuard，再等待 channel 空位：`src/future/cache.rs:1996`、`src/future/cache.rs:2020`-`src/future/cache.rs:2030`。
7. drain 时 remove 走 `handle_remove()`：`src/future/base_cache.rs:1604`-`src/future/base_cache.rs:1614`、`src/future/base_cache.rs:2033`-`src/future/base_cache.rs:2074`。
8. `invalidate_all` 只设置时间水位：`src/future/base_cache.rs:398`-`src/future/base_cache.rs:400`；谓词失效在 async invalidator 中用 timestamp 条件保护：`src/future/invalidator.rs:313`-`src/future/invalidator.rs:325`，通知为 `Explicit`：`src/future/invalidator.rs:328`-`src/future/invalidator.rs:332`。

## 维护循环、deque 与 timer wheel

### 统一阶段顺序

sync 与 future 的 `do_run_pending_tasks()` 基本同构：

1. 获取策略锁和 timer 锁：sync `src/sync/base_cache.rs:1200`-`src/sync/base_cache.rs:1202`；future `src/future/base_cache.rs:1324`-`src/future/base_cache.rs:1326`。
2. drain reads 和 writes：sync `src/sync/base_cache.rs:1218`-`src/sync/base_cache.rs:1225`；future `src/future/base_cache.rs:1341`-`src/future/base_cache.rs:1350`。
3. timer wheel expiration：sync `src/sync/base_cache.rs:1245`-`src/sync/base_cache.rs:1250`；future `src/future/base_cache.rs:1380`-`src/future/base_cache.rs:1386`。
4. TTL/TTI/`valid_after` deque expiration：sync `src/sync/base_cache.rs:1255`-`src/sync/base_cache.rs:1261`；future `src/future/base_cache.rs:1391`-`src/future/base_cache.rs:1398`。
5. invalidation predicate：sync `src/sync/base_cache.rs:1266`-`src/sync/base_cache.rs:1274`；future `src/future/base_cache.rs:1403`-`src/future/base_cache.rs:1413`。
6. capacity eviction：sync `src/sync/base_cache.rs:1279`-`src/sync/base_cache.rs:1287`；future `src/future/base_cache.rs:1417`-`src/future/base_cache.rs:1426`。
7. 根据 backlog、listener timeout 和 `more_entries_to_evict` 决定是否继续：sync `src/sync/base_cache.rs:1292`-`src/sync/base_cache.rs:1311`；future `src/future/base_cache.rs:1431`-`src/future/base_cache.rs:1450`。
8. 本地计数一次性发布，sync flush epoch 后释放 deque lock：`src/sync/base_cache.rs:1315`-`src/sync/base_cache.rs:1324`；future 对应 `src/future/base_cache.rs:1454`-`src/future/base_cache.rs:1463`。

### Policy deque

- `Deques` 包含 window、main probation、main protected 和 write-order：`src/common/concurrent/deques.rs:10`-`src/common/concurrent/deques.rs:15`。
- admission 只把新策略节点放入 `MainProbation`，节点创建后把 tagged AO 指针写回 `ValueEntry.DeqNodes`：`src/common/concurrent/deques.rs:49`-`src/common/concurrent/deques.rs:63`。
- write-order 节点只在 TTL 或 invalidator 启用时加入：sync `src/sync/base_cache.rs:977`、`src/sync/base_cache.rs:1803`；future `src/future/base_cache.rs:1121`、`src/future/base_cache.rs:1962`。
- dirty deque node 不会被直接当旧值删除；维护循环会把它移到队尾并等待对应 WriteOp：sync AO `src/sync/base_cache.rs:2092`-`src/sync/base_cache.rs:2100`、WO `src/sync/base_cache.rs:2229`-`src/sync/base_cache.rs:2232`。
- capacity eviction 删除前要求 CHT 当前值的 `last_accessed` 仍等于 deque snapshot：sync `src/sync/base_cache.rs:2387`-`src/sync/base_cache.rs:2396`；future `src/future/base_cache.rs:2583` 之后同样以 snapshot 为 condition。
- TinyLFU victim 也用 `last_accessed` 条件保护：sync `src/sync/base_cache.rs:1597`-`src/sync/base_cache.rs:1604`；future `src/future/base_cache.rs:1752`-`src/future/base_cache.rs:1759`。

### Timer wheel

- timer node 自带 `expiry_gen`、`EntryInfo` 和 `DeqNodes` 指针：`src/common/timer_wheel.rs:61`-`src/common/timer_wheel.rs:77`。
- schedule 读取 packed expiration snapshot；若调用时 gen 已过期，直接不建节点：`src/common/timer_wheel.rs:220`-`src/common/timer_wheel.rs:245`。
- reschedule/deschedule 均比较 node 中 gen 与 `DeqNodes.timer_node_expiry_gen`，不匹配则拒绝操作：`src/common/timer_wheel.rs:290`-`src/common/timer_wheel.rs:312`、`src/common/timer_wheel.rs:327`-`src/common/timer_wheel.rs:349`。
- 维护层 `update_timer_wheel()` 原子读取 expiration time/gen，再读取 node/expected gen，按四种状态建、改、删 timer：sync `src/sync/base_cache.rs:1810`-`src/sync/base_cache.rs:1870`；future `src/future/base_cache.rs:1969`-`src/future/base_cache.rs:2029`。
- wheel advance 弹出 node 后再次读取当前 expiration state：已到期才 unset back-pointer 并产生 `Expired`，未到期则重排，无 expiration 则 deschedule：`src/common/timer_wheel.rs:597`-`src/common/timer_wheel.rs:630`。
- wheel 到期不等于必然删除：维护层还会跳过 dirty entry，并在 CHT condition 中确认 per-entry TTL 仍到期：sync `src/sync/base_cache.rs:1981`-`src/sync/base_cache.rs:2001`；future `src/future/base_cache.rs:2149`-`src/future/base_cache.rs:2168`。

## RemovalCause 与 listener 锁语义

`RemovalCause` 定义在 `src/notification.rs:29`-`src/notification.rs:41`。

- **Expired**：timer wheel per-entry expiration：sync `src/sync/base_cache.rs:2006`，future `src/future/base_cache.rs:2173`；TTL/TTI deque expiration：sync `src/sync/base_cache.rs:2080`、`src/sync/base_cache.rs:2217`，future `src/future/base_cache.rs:2250`、`src/future/base_cache.rs:2396`。
- **Size**：超重 candidate、TinyLFU victim/reject、容量 LRU：sync `src/sync/base_cache.rs:1562`、`src/sync/base_cache.rs:1610`、`src/sync/base_cache.rs:1663`、`src/sync/base_cache.rs:2401`；future 对应 `src/future/base_cache.rs:1712`、`src/future/base_cache.rs:1763`、`src/future/base_cache.rs:1821`、`src/future/base_cache.rs:2598`。
- **Explicit**：单 key invalidate 默认 `Explicit`，但如果值按 TTL/TTI 已过期则改报 `Expired`：sync `src/sync/base_cache.rs:2473`-`src/sync/base_cache.rs:2491`；future `src/future/base_cache.rs:2684`-`src/future/base_cache.rs:2715`。谓词失效固定 `Explicit`：sync `src/sync/invalidator.rs:324`-`src/sync/invalidator.rs:327`；future `src/future/invalidator.rs:328`-`src/future/invalidator.rs:332`。
- **Replaced**：同 key CHT replacement 的旧值通知，初始为 `Replaced`；若旧值按固定 TTL/TTI 或 invalidation watermark 已逻辑过期，则改报 `Expired`/`Explicit`：sync `src/sync/base_cache.rs:2443`-`src/sync/base_cache.rs:2469`；future `src/future/base_cache.rs:2643`-`src/future/base_cache.rs:2680`。

锁语义：

- **sync replacement listener**：在 per-key lock 内、CHT 已替换后同步运行；不持有 deque/timer 维护锁。入口 `src/sync/base_cache.rs:600`，实际调用 `src/notification/notifier.rs:25`-`src/notification/notifier.rs:37`。
- **sync explicit invalidate listener**：在 per-key lock 内、CHT 已删除后同步运行；不持有 deque/timer 锁：`src/sync/cache.rs:1609`-`src/sync/cache.rs:1617`。
- **sync maintenance listener**：在 deque lock 和 timer wheel lock 内同步运行，例如 `src/sync/base_cache.rs:2006` 与 `src/sync/base_cache.rs:2401`；通知函数本身在 `src/sync/base_cache.rs:777`-`src/sync/base_cache.rs:788`。
- **future replacement / explicit listener**：调用者在 per-key async lock 内 await；不持有 deque/timer 锁，但被 CancelGuard 保护：`src/future/base_cache.rs:604`-`src/future/base_cache.rs:625`、`src/future/cache.rs:1986`-`src/future/cache.rs:2005`。
- **future maintenance listener**：在 async deque/timer guard 内 await，慢 listener 会直接延长维护临界区：`src/future/base_cache.rs:907`-`src/future/base_cache.rs:918`、`src/future/base_cache.rs:2171`-`src/future/base_cache.rs:2174`。

## 旧 WriteOp 命中重新插入 entry 的证明

考虑下面的交错：

1. T1 insert `k -> v1`，CHT bucket 当前为 `E1 = ValueEntry { info: I1, nodes: N1 }`，WriteOp `U1` 携带 `I1` 和 `entry_gen=1`。
2. T1 或维护流程把 `E1` admission 到 deque/timer。
3. T2 invalidate/evict `k`。CHT condition 对当前 bucket 成立，tombstone CAS 成功；post-CAS closure 对 `I1.retire()`。
4. 策略层稍后摘除 `N1` 中的节点；即使摘除延迟，`I1.is_retired` 已经为 true。
5. T3 insert `k -> v2`。因为 bucket 是 tombstone，CHT 走 insert，而不是 modify；它分配 `E2 = ValueEntry { info: I2, nodes: N2 }`，`I2.entry_gen=1`、`I2.is_retired=false`。
6. 旧 `U1` 此时才被 drain。

关键观察：

- `U1.value_entry.info` 的地址是 `I1`，CHT 当前值的 info 地址是 `I2`。
- `I1.is_retired == true` 是在成功 tombstone CAS 的 post-success callback 中发布的；drain 侧 Acquire load 能看到该状态。
- `I2.entry_gen == 1` 可能与 `U1.entry_gen == 1` 相同，证明代数数字本身不能区分这两个生命期。
- `handle_upsert()` 第一关基于 `I1.is_alive()==false` 直接返回：sync `src/sync/base_cache.rs:1515`；future `src/future/base_cache.rs:1660`。
- 即使未来重构遗漏第一关，`handle_admit()` 对同一 `I1` 再做 retired 检查：sync `src/sync/base_cache.rs:1785`；future `src/future/base_cache.rs:1944`。
- 超重或 admission reject 路径要反向删除 CHT candidate 时，还要求当前 bucket 的 `EntryInfo` 与 op 是同一指针，且当前 gen 等于 op gen：sync `src/sync/base_cache.rs:1555`-`src/sync/base_cache.rs:1556`、`src/sync/base_cache.rs:1655`-`src/sync/base_cache.rs:1656`；future `src/future/base_cache.rs:1704`-`src/future/base_cache.rs:1705`、`src/future/base_cache.rs:1812`-`src/future/base_cache.rs:1813`。因此旧 op 无法删除 `I2/E2`。
- deque/timer 节点身份也是每个生命期独立的 `N1/N2`：新建见 `src/common/concurrent.rs:180`-`src/common/concurrent.rs:188`，替换复用见 `src/common/concurrent.rs:191`-`src/common/concurrent.rs:198`。旧 op 即使绕过 retired 检查，最多污染 `N1` 和本地计数，不能通过 key 找到新 CHT 值；reject 删除仍被指针条件挡住。

所以旧 `WriteOp` 不能破坏新 entry。要重现历史 orphan/重复计数问题，必须删除 **retired 生命周期身份检查这一类保护**；只改数字代数比较无法覆盖删除后重新插入。

## 共享与不可共享部分

### 两种实现共享

- CHT、bucket CAS、post-success callback：`src/cht/segment.rs:381`、`src/cht/map/bucket_array_ref.rs:64`、`src/cht/map/bucket.rs:156`、`src/cht/map/bucket.rs:266`。
- `EntryInfo`、`ValueEntry`、`KvEntry`、`ReadOp`、`WriteOp`：`src/common/concurrent.rs:88`、`src/common/concurrent.rs:173`、`src/common/concurrent.rs:303`、`src/common/concurrent.rs:312`。
- deque、TinyLFU/frequency sketch、timer wheel、packed expiry generation：`src/common/concurrent/deques.rs:10`、`src/common/timer_wheel.rs:162`、`src/common/concurrent/entry_info.rs:86`。
- removal causes：`src/notification.rs:29`。
- 维护阶段顺序、condition 内容、retire 与 policy gen 处理基本相同。

### 不可共享 / 分叉

- sync housekeeper 是 parking-lot mutex；调用者线程同步执行维护：`src/common/concurrent/housekeeper.rs:26`、`src/common/concurrent/housekeeper.rs:105`。future housekeeper 是 async mutex + cancellable shared task：`src/future/housekeeper.rs:26`、`src/future/housekeeper.rs:114`、`src/future/housekeeper.rs:154`。
- sync 写通道满后 sleep 重试：`src/sync/cache.rs:1832`-`src/sync/cache.rs:1839`；future spin 后 await event：`src/future/base_cache.rs:670`-`src/future/base_cache.rs:694`。
- sync listener 是同步闭包；future listener 返回 future，panic 分别包住 future 创建和 future poll：`src/notification/notifier.rs:25`-`src/notification/notifier.rs:37`、`src/future/notifier.rs:27`-`src/future/notifier.rs:55`。
- future 额外有 interrupted channel 与 `CancelGuard`，保证调用者取消时 listener future 和 write op 可重放：`src/future/base_cache.rs:701`-`src/future/base_cache.rs:739`、`src/future/cache.rs:1816`-`src/future/cache.rs:1843`、`src/future/cache.rs:1984`-`src/future/cache.rs:2031`。
- sync 策略锁是 `parking_lot::Mutex`；future 是 `async_lock::Mutex`，频率 sketch 也使用 async RwLock：sync `src/sync/base_cache.rs:879`-`src/sync/base_cache.rs:881`；future `src/future/base_cache.rs:1009`-`src/future/base_cache.rs:1011`。

### 唤醒点与等待点

- sync read op：get 前可能 `try_run_pending_tasks()`，但 read channel 满仍直接丢弃：`src/sync/base_cache.rs:472`-`src/sync/base_cache.rs:477`。
- sync write op：发送前触发非阻塞维护；满后 sleep 50 微秒重试：`src/sync/cache.rs:1832`-`src/sync/cache.rs:1839`。
- 手动 sync `run_pending_tasks()` 使用 blocking run lock：`src/sync/cache.rs:1763`-`src/sync/cache.rs:1767`。
- future read op：`try_run_pending_tasks()` 后 try-send，满则丢弃：`src/future/base_cache.rs:461`-`src/future/base_cache.rs:471`。
- future write op：channel 满且 spin 过后等待 `write_op_ch_ready_event`：`src/future/base_cache.rs:683`-`src/future/base_cache.rs:694`。
- future housekeeper 在维护结束后唤醒所有等待者：`src/future/housekeeper.rs:126`-`src/future/housekeeper.rs:129`；try-run 结束也唤醒：`src/future/housekeeper.rs:146`-`src/future/housekeeper.rs:151`。
- future drain 期间 channel 腾出空间会按空闲槽位数通知：`src/future/base_cache.rs:1359`-`src/future/base_cache.rs:1367`。

## 风险与复现

### 风险 1：旧 WriteOp 命中新 generation

**后果**

- 旧 op 把已删除生命期的 AO/WO/timer 节点重新接回策略结构，产生 orphan。
- 本地维护计数重复增加，容量判断错误，LRU eviction 可能停在无法从 CHT 删除的节点前。
- 如果超重/reject 删除只按 key 而不校验当前 `EntryInfo` 指针与 gen，旧 op 可能误删重新插入后的新值。

**确定性复现步骤**

1. 构造 max capacity 1 的 LRU cache，禁用自动 housekeeping，使用受控 `Clock::mock()`。
2. insert `7 -> 10`，手工发送并 drain 其 upsert，使旧 entry 已 admitted。
3. 调内部 `remove_entry()` 触发同一个 CHT tombstone CAS 与 `retire()`，再手工调用 `handle_remove()` 模拟该删除 op 的策略清理。
4. 推进 mock clock，insert `7 -> 20` 但先不 drain。
5. 按固定顺序发送旧 upsert，再发送新 upsert；最后只执行一次维护。
6. 断言 CHT 可读回 `20`，且 `entry_count=1`、`weighted_size=1`。

这正是新增测试 `stale_upsert_from_retired_generation_cannot_admit_after_reinsert` 做的事情：`src/sync/base_cache.rs:2706`。它没有线程、没有海量循环；通道顺序由测试直接决定，时间由 mock clock 推进。

**保护存在时**：旧 op 在 `handle_upsert()` 的 retired 检查处返回；新 op 正常 admit，测试稳定通过。

**去掉哪类检查会失败**：删除 `handle_upsert()` 的 retired gate（`src/sync/base_cache.rs:1515`）以及 `handle_admit()` 的二次 retired gate（`src/sync/base_cache.rs:1785`）后，旧 op 会重复增加本地计数/节点；本次实测测试失败为 `entry_count` 左值 `2`、期望值 `1`。若进一步移除 reject/超重路径的 `MiniArc::ptr_eq(info, current.info)` 与 `entry_gen` 双条件（`src/sync/base_cache.rs:1555`-`src/sync/base_cache.rs:1556`），旧 op 可对新 CHT bucket 发起错误删除。只删除一个 gate 时，另一个 gate 会阻断本测试的最终破坏，因此文档把它归为“生命周期身份检查”一类，而不是单点断言。

### 风险 2：listener 阻塞造成维护积压

**后果**

- read/write channel 达到 flush point 后，新的 write op 需要等待维护；维护被 listener 卡住会反压插入、失效和替换。
- deque/timer 中过期或超容量 entry 不能及时摘除，`entry_count()`/`weighted_size()` 保持近似旧值。
- future 中 await listener 的任务可能被取消；任务被保存，后续由 interrupted channel 重放，不会丢失，但完成时间继续后移。

**复现步骤（sync）**

1. builder 配置 max capacity 1 和 eviction listener。
2. listener 第一次收到 `RemovalCause::Size` 时等待一个 test barrier 或 oneshot。
3. insert key 1，再 insert key 2；让第二个 insert 触发容量驱逐。
4. 另一个线程尝试 `run_pending_tasks()`，它会在 deque/timer lock 内进入 listener。
5. 在 barrier 释放前观察第三个 insert：write channel 达到阈值后在 `schedule_write_op()` 中 sleep/retry，无法完成策略收敛。
6. 释放 barrier，再 drain，backlog 清空。

**复现步骤（future）**

1. async listener 在 `Notify` 上等待。
2. 触发一次 size expiration/eviction。
3. `run_pending_tasks()` await listener 时持有 async deque/timer guard。
4. 其他 insert 先尝试 try-run，失败后 spin，再 await `write_op_ch_ready_event`。
5. notify listener 后，housekeeper 结束并 notify 等待者，插入继续。

现有缓解：listener 存在时维护循环有 100ms timeout（`src/common/concurrent/constants.rs:19`-`src/common/concurrent/constants.rs:20`，sync `src/sync/base_cache.rs:1305`-`src/sync/base_cache.rs:1311`，future `src/future/base_cache.rs:1444`-`src/future/base_cache.rs:1450`）。但 timeout 只让本轮维护返回并设置 `more_entries_to_evict`，不能强制终止已在运行的 sync closure 或 future listener；下一轮仍可能阻塞。生产建议让 listener 非阻塞、把慢 I/O 投递到独立 worker，并把队列满/失败作为 backpressure 处理。

### 风险 3：可变 Expiry 与并发读取重排 timer

**后果**

- `expire_after_read()` 返回的 duration 依赖外部状态时，并发 read callback 完成顺序可能与发起顺序不同。
- 旧 read callback 若晚写 expiration state，可能把新 read 计算出的到期时间覆盖成旧值。
- 若 timer node 只保存裸指针而不校验 expiry generation，随后的 reschedule/deschedule 可能操作已被替换或删除的 node。

**当前保护**

- expiration time 与 `expiry_gen` 在同一个 atomic u64 中 CAS 更新，读写是一致 snapshot：`src/common/concurrent/entry_info.rs:242`-`src/common/concurrent/entry_info.rs:304`。
- get 把 callback 是否实际改变 expiry 记录在 `ReadOp::Hit.is_expiry_modified`：sync `src/sync/base_cache.rs:346`-`src/sync/base_cache.rs:364`；future `src/future/base_cache.rs:321`-`src/future/base_cache.rs:340`。
- timer node 保存 expiry gen：`src/common/timer_wheel.rs:70`-`src/common/timer_wheel.rs:76`。
- schedule 不信任过期参数，重新读当前 state 并比较 gen：`src/common/timer_wheel.rs:228`-`src/common/timer_wheel.rs:239`。
- reschedule/deschedule 拒绝 stale node：`src/common/timer_wheel.rs:290`-`src/common/timer_wheel.rs:312`、`src/common/timer_wheel.rs:327`-`src/common/timer_wheel.rs:349`。
- wheel fire 时再次读取当前 expiration state，未到期就重排，无到期时间就摘除：`src/common/timer_wheel.rs:597`-`src/common/timer_wheel.rs:630`。
- 即使 timer node 误报过期，CHT 删除 condition 仍以当前 `EntryInfo` 的 per-entry expiration state 为准：sync `src/sync/base_cache.rs:1997`-`src/sync/base_cache.rs:2001`；future `src/future/base_cache.rs:2164`-`src/future/base_cache.rs:2168`。

**确定性复现/测试钩子建议**

- 用 `Clock::mock()` 固定 T0，配置一个可由 `AtomicU32` 控制返回短/长 duration 的 `Expiry`。
- 用两个 `Barrier`/oneshot 手动控制两个 get 中 `expire_after_read()` callback 的返回顺序：A 先开始并计算短 TTL，B 后开始计算长 TTL，但让 A 的返回值最后写回。
- 先不 drain read ops，按 B 后 A 的固定顺序发送两个 `ReadOp::Hit`。
- 推进到短 TTL 与长 TTL 之间并运行维护。正确结果是 entry 仍存在；随后推进到长 TTL，entry 才被删除。
- 若移除 packed gen 或 timer node gen 校验，旧 pointer 的 reschedule/deschedule 应被拒绝或重建；若 CHT expiration condition 也被移除，才会观察到提前删除。

注意：用户提供的 `Expiry` 必须是确定性纯函数；依赖锁外可变状态会让“逻辑上的最新读取”本身存在争用语义。实现能防止 stale node 内存安全问题和错误 CHT 删除，但不保证为非确定性回调串行化业务顺序。

## 新增回归测试

- 测试位置：`src/sync/base_cache.rs:2706`。
- 使用 `Clock::mock()`：`src/sync/base_cache.rs:2712`；受控时间推进在 `src/sync/base_cache.rs:2732`、`src/sync/base_cache.rs:2761`。
- 禁用自动维护：`reconfigure_for_testing()` 调用位于 `src/sync/base_cache.rs:2726`，实现会关闭 housekeeper auto-run（`src/sync/base_cache.rs:740`-`src/sync/base_cache.rs:746`）。
- 手工保持旧 upsert、触发 CHT retire/remove、再插入新值，并固定通道顺序：`src/sync/base_cache.rs:2733`-`src/sync/base_cache.rs:2771`。
- 测试明确断言删除后第一次插入和重新插入的数字代数相同：`src/sync/base_cache.rs:2767`，因此它验证的是生命周期身份，而非碰 u16 递增。
- benchmark 不用于证明正确性；该测试的断言是 map 值、entry count 与 weighted size。

## 验证命令

```sh
cargo build --all-targets --features 'sync future'
cargo test --features 'sync future'
test -s ANALYSIS.md
```

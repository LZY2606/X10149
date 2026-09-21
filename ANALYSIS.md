# Moka 写入/维护路径实现分析：entry 身份、generation 与陈旧 `WriteOp` 防护

- 对应 commit：`2474ad9`（*Initial environment snapshot (X10149)*）。
- 特性组合：`--features 'sync future'`。文中所有 `文件:行号` 均对应该 commit；唯一新增的代码是文末第 6 节引用的回归测试（位于文件末尾，不影响本文引用行号）。
- 结论先行：**key 不是 entry 的身份，`EntryInfo` 指针才是。** 旧 generation 的 `WriteOp` 即使在 key 被驱逐后重新插入时到达，也无法修改新 entry，因为维护层在消费 `WriteOp` 时依次用 `is_retired`（CHT 成员身份）、`MiniArc::ptr_eq(EntryInfo)`（entry 身份）和 `entry_gen`（同一 entry 内的版本）三道判据拒绝陈旧操作。

---

## 1. 总体模型：两级结构与三套代际

```
用户线程 (sync / async)
  │  ① 直接操作 lock-free CHT（SegmentedHashMap）——真相源
  │     insert/update/invalidate 在这里线性化
  ▼
CHT: Arc<K> -> MiniArc<ValueEntry<K,V>>   src/common/concurrent.rs:173
              ValueEntry { value, info: MiniArc<EntryInfo>, nodes: MiniArc<Mutex<DeqNodes>> }
  │  ② 策略操作以 op 形式投入有界 channel（不与 CHT 写入同步）
  ▼
read_op_ch (ReadOp)   write_op_ch (WriteOp)        src/common/concurrent.rs:303, src/common/concurrent.rs:312
  │
  ▼
维护循环（housekeeper 触发，sync 由用户线程/维护线程执行，future 在 async 任务中执行）
  Inner::do_run_pending_tasks
    apply_reads → apply_writes → timer wheel 过期 → deque 过期 → 谓词失效 → 容量驱逐
  线性化策略状态，全部在 deqs / timer_wheel 维护锁内
```

`EntryInfo`（`src/common/concurrent/entry_info.rs`）持有三套彼此独立的判据：

| 判据 | 类型/位置 | 语义 | 读改方式 |
| --- | --- | --- | --- |
| `is_admitted` | `AtomicBool`，entry_info.rs:141 | 是否已进入策略 deque（deque 成员身份） | Acquire/Release，`set_admitted` entry_info.rs:137 |
| `is_retired` | `AtomicBool`，entry_info.rs:78 | 是否已从 CHT 摘除（CHT 成员身份）；单调 false→true | `is_alive()` entry_info.rs:152；`retire()` entry_info.rs:168，只在 CHT 摘除 CAS 的 post-success 回调里调用 |
| `entry_gen` / `policy_gen` | `AtomicU16`，entry_info.rs:81/85 | CHT 版本 / 策略层版本；不相等即 dirty | `incr_entry_gen` entry_info.rs:195；`set_policy_gen` entry_info.rs:204（带回绕保护，拒绝回退）；`is_dirty()` entry_info.rs:181 |
| `expiration_state` | 打包 `AtomicU64`，entry_info.rs:99 | 高 52 位过期时间 + 低 12 位 `expiry_gen`，原子快照 | `expiration_state()` entry_info.rs:242；`set_expiration_time()` entry_info.rs:261（CAS 循环，每次更新 gen+1 回绕） |

另有 timer 节点侧的代际：`DeqNodes.timer_node_expiry_gen`（`src/common/concurrent.rs:145`），与 `expiry_gen` 配对存取（`set_timer_node` concurrent.rs:164、`timer_node_with_expiry_gen` concurrent.rs:168、`take_timer_node` concurrent.rs:260），用于检测 timer wheel 中悬挂/陈旧的 `NonNull` 节点。

关键不变量：

1. **CHT 是真相源。** insert/update/remove 在线性化点立即改变 CHT；deque、timer wheel、计数器只是“镜像”，由 op 异步追赶。
2. **`retire()` 与 CHT bucket 指针 CAS 同一线性化点。** 它只在 `cht::remove_entry_if_and` 的 `with_previous_entry` 回调中执行（sync `Inner::remove_entry` `src/sync/base_cache.rs:1106`；future `src/future/base_cache.rs:1250`）。赢得 CAS 的线程恰好执行一次回调（`src/cht/segment.rs:381`），因此 `debug_assert!(!is_retired)` 成立、不需要 RMW。
3. **entry 身份是 `MiniArc<EntryInfo>` 的指针。** update 复用同一个 `EntryInfo`（`new_value_entry_from`，sync base_cache.rs:651；future base_cache.rs:781），但 evict+reinsert 分配全新的 `EntryInfo`（`new_value_entry`，sync base_cache.rs:636；future base_cache.rs:766）。`entry_gen` 从 1 重新开始（entry_info.rs:116），因此跨 key 的“代”号会重合，绝不能单独用来判身份。
4. 维护层任何策略变更前必须确认 entry 仍 alive。

---

## 2. Sync 调用链

### 2.1 `sync::Cache::insert`

公开入口 `Cache::insert`（`src/sync/cache.rs:1476`）→ `insert_with_hash`（cache.rs:1482）→ `BaseCache::do_insert_with_hash`（`src/sync/base_cache.rs:482`）。

1. 若启用了阻塞式移除通知，先取 per-key 锁（base_cache.rs:494-496，`maybe_key_lock`）。
2. 调 CHT `insert_with_or_modify`（base_cache.rs:507；CHT 实现在 `src/cht/segment.rs:408`）：
   - `on_insert`（base_cache.rs:509）：`new_value_entry`（base_cache.rs:636）→ 全新 `EntryInfo`，`entry_gen` 初值 1，构造 `WriteOp::new_upsert`（concurrent.rs:366，携带 `key_hash`、`MiniArc<ValueEntry>`、`entry_gen=1`、old/new weight）。
   - `on_modify`（base_cache.rs:521）：`new_value_entry_from`（base_cache.rs:651）**复用旧 `EntryInfo`**、`incr_entry_gen()`（base_cache.rs:661；entry_info.rs:195，`AcqRel` fetch_add 回绕），记录 `OldEntryInfo`（concurrent.rs:386，保留旧 last_accessed/last_modified）。
   - CHT 采用乐观锁，闭包可能重入；`op_cnt1/op_cnt2` 计数器（base_cache.rs:484）识别“最后一次”闭包调用（base_cache.rs:532-541）。
3. **线性化点：CHT bucket CAS**（`src/cht/segment.rs:408` 内部）。此后任何 `get` 都能看到新值，与策略层是否已 drain 无关。
4. `do_post_insert_steps`（base_cache.rs:544）/ `do_post_update_steps`（base_cache.rs:562）：若配置了自定义 `Expiry`，在用户线程上调用 `expire_after_create/update`（base_cache.rs:671/693），`set_expiration_time` 原子改写过期时间并 bump `expiry_gen`；更新还可能在用户线程触发 `Replaced`/`Expired` 通知（`notify_upsert` base_cache.rs:607、2443）。
5. `schedule_write_op`（`src/sync/cache.rs:1819`）：忙循环 `try_send` 写 `write_op_ch`；满时 `sleep(50µs)`（constants.rs:23，cache.rs:1838），每轮先调 `apply_reads_writes_if_needed`（base_cache.rs:389）→ `housekeeper.try_run_pending_tasks`（housekeeper.rs:110，`run_lock.try_lock()`），按 `WRITE_LOG_FLUSH_POINT=64`（constants.rs:5）或 300ms 周期（constants.rs:2，housekeeper.rs:100）决定是否顺手 drain。
6. **唤醒点**：housekeeper 没有后台线程（sync）。维护只由任意用户线程在上述阈值处顺手执行，或显式 `Cache::run_pending_tasks`（cache.rs:1764）。

### 2.2 `sync::Cache::get`

入口 `Cache::get`（`src/sync/cache.rs:795`）→ `BaseCache::get_with_hash`（base_cache.rs:217）→ `do_get_with_hash`（base_cache.rs:265）。

1. **线性化点：CHT 查找** `get_key_value_and_then`（base_cache.rs:276；cht segment.rs 对应读路径）。闭包内按“当时”的 TTL/TTI/`valid_after`/谓词判定是否过期失效（base_cache.rs:285-299），过期按 miss 处理。
2. 命中后在用户线程执行自定义 `Expiry::expire_after_read`（base_cache.rs:337），返回 `is_expiry_modified` 标志；该标志随 `ReadOp::Hit`（base_cache.rs:345；concurrent.rs:303）带走。同时 `set_last_accessed(now)`（base_cache.rs:343）。
3. `record_read_op`（base_cache.rs:467）先 `apply_reads_if_needed`（base_cache.rs:613）→ 同样走 housekeeper 阈值；**channel 满时 `ReadOp` 被直接丢弃**（base_cache.rs:472-476，`Full` 当成功）。这是有意的：timer wheel 到期时还会复核真实过期时间，丢弃读不影响正确性。
4. 维护侧 `Inner::apply_reads`（base_cache.rs:1392）：freq sketch 计数；**`is_alive()` 检查在 base_cache.rs:1404**——陈旧 `ReadOp::Hit` 指向已 retire 的 entry 时跳过 timer 重排与 `move_to_back_ao`；随后 `update_timer_wheel`（base_cache.rs:1413、1810）与 `deqs.move_to_back_ao`（base_cache.rs:1415）。

### 2.3 `sync::Cache::invalidate`（含 `remove`）

入口 `Cache::invalidate`（cache.rs:1554）/ `remove`（cache.rs:1569）→ `invalidate_with_hash`（cache.rs:1577）。

1. 启用通知时先尽量取 per-key 锁（cache.rs:1584-1596）。
2. `BaseCache::remove_entry`（base_cache.rs:381）→ `Inner::remove_entry`（base_cache.rs:1106）→ CHT `remove_entry_if_and`（`src/cht/segment.rs:381`）：
   - **线性化点与 retire 同一 CAS**：post-success 回调中 `v.entry_info().retire()`（base_cache.rs:1114；entry_info.rs:168）并返回 `KvEntry`（concurrent.rs:88）。输掉 CAS 的线程拿不到回调，所以 `retire()` 恰好一次。
3. `incr_entry_gen()`（cache.rs:1607）使陈旧的 update/timer 操作随后能被 `is_dirty()`/版本比较识别；通知 `notify_invalidate`（cache.rs:1610 → base_cache.rs:2473），默认 cause 为 `Explicit`，但按旧时间戳在当时已过期则降级为 `Expired`（base_cache.rs:2480-2491）。
4. 先放 key 锁（cache.rs:1616，避免“队列满→忙等 drain→drain 又要同一 key 锁”的死锁），再 `WriteOp::Remove`（cache.rs:1625，concurrent.rs:320，带 `entry_gen`）入队。
5. 维护侧 `apply_writes`（base_cache.rs:1422）→ `handle_remove`（base_cache.rs:1874）：带 `expiry_gen` 摘 timer 节点（base_cache.rs:1877-1882），`handle_remove_without_timer_wheel`（base_cache.rs:1891，`debug_assert!(is_retired)`）在 `is_admitted` 时 unlink AO/WO deque 并回滚计数，否则只 `unset_q_nodes`，最后 `set_policy_gen` 使 dirty 清零。

---

## 3. Future 调用链

### 3.1 `future::Cache::insert`

入口 `Cache::insert`（`src/future/cache.rs:1337`）→ `insert_with_hash`（future/cache.rs:1810）→ `BaseCache::do_insert_with_hash`（`src/future/base_cache.rs:476`）。

与 sync 的对照差异：

1. 开头先 `retry_interrupted_ops()`（base_cache.rs:479、701）：如果此前的 async 任务在等待监听器 future 或排队 `WriteOp` 时被取消，`CancelGuard` 已把 `InterruptedOp`（future + op，或只 op）放入 `interrupted_op_ch`，这里先补做，**保证取消不会丢通知或丢写**（base_cache.rs:701-742）。
2. CHT `insert_with_or_modify`（base_cache.rs:511）及 on_insert/on_modify（base_cache.rs:513/525；`new_value_entry` 766、`new_value_from` 781）与 sync 完全相同，线性化点同为 CHT bucket CAS。
3. 更新路径的监听器调用被包成 `Shared<BoxFuture>` 并配合 `CancelGuard`（base_cache.rs:607-628）：任务取消时 future 与 `upd_op` 被保存，下次 insert/get/invalidate 时由 `retry_interrupted_ops` 续做。
4. `schedule_write_op`（base_cache.rs:633）：前 4 次失败自旋（base_cache.rs:672-681），之后 `ch_ready_event.listen().await`（base_cache.rs:692）让出执行器。
5. **唤醒点**：`Housekeeper::run_pending_tasks`（`src/future/housekeeper.rs:114`）/ `try_run_pending_tasks`（housekeeper.rs:132）结束时 `write_op_ch_ready_event.notify(usize::MAX)`（housekeeper.rs:127/149），唤醒所有满队列等待者。维护任务本体是存在 `current_task` 锁里的 `Shared<BoxFuture>`（housekeeper.rs:177-190）；被取消时不丢弃，下轮恢复（housekeeper.rs:170-175）。

### 3.2 `future::Cache::get`

入口 `Cache::get`（`src/future/cache.rs:875`）→ `BaseCache::get_with_hash`（`src/future/base_cache.rs:243`）。

- 记录读之前也先 `retry_interrupted_ops()`（base_cache.rs:258）。
- CHT 查找与过期判定（base_cache.rs:262-279）、`expire_after_read`、ReadOp 逻辑与 sync 同构；维护侧 `apply_reads`（base_cache.rs:1534）的 `is_alive()` 门在 base_cache.rs:1551。
- 触发维护是 `apply_reads_if_needed`（base_cache.rs:743）→ `hk.try_run_pending_tasks(inner).await`（housekeeper.rs:132）。

### 3.3 `future::Cache::invalidate`

入口 `Cache::invalidate`（future/cache.rs:1350）→ `invalidate_with_hash`（future/cache.rs:1922）。

1. `BaseCache::remove_entry`（base_cache.rs:375）→ `Inner::remove_entry`（base_cache.rs:1250）：**retire 与 CHT CAS 同一线性化点**（base_cache.rs:1254-1260）。
2. `WriteOp::Remove` 构造（future/cache.rs:1963）与 `notify_invalidate`（base_cache.rs:2684）都用 `CancelGuard` 保护（future/cache.rs:1968-1985），`future.await` 期间任务被取消会把 future+op 存入 interrupted 通道；同样在释放 key 锁后才排队 op（future/cache.rs:1996-1997）。
3. 维护侧 `apply_writes`（base_cache.rs:1569）→ `handle_remove`（base_cache.rs:2033），逻辑与 sync 相同；`apply_reads`/`apply_writes` 使用 `async_lock` 并在 `do_run_pending_tasks`（base_cache.rs:1314）中持锁。

---

## 4. 维护循环、policy deque、timer wheel 与通知

### 4.1 循环顺序（两种实现相同）

sync `Inner::do_run_pending_tasks`（`src/sync/base_cache.rs:1190`）；future 异步版（`src/future/base_cache.rs:1314`）。

1. 取维护锁：sync 为 parking_lot `deques.lock()` + `timer_wheel.lock()`（base_cache.rs:1203-1204）；future 为 `async_lock`（base_cache.rs:1325-1326）。
2. `apply_reads`（1392/1534）→ `apply_writes`（1422/1569），按 channel 当前长度批量 drain。
3. timer wheel 到期 `evict_expired_entries_using_timers`（1952/2110）。
4. WO/AO deque 到期扫描 `evict_expired_entries_using_deqs`（2023/2190）。
5. 谓词失效 `invalidate_entries`（2267/2458）。
6. 容量驱逐 `evict_lru_entries`（2329/2521）。
7. channel 积压达到 flush point 且未超 `max_log_sync_repeats` 时再来一轮（sync base_cache.rs:1292）；启用监听器时有 `maintenance_task_timeout` 兜底超时（sync base_cache.rs:1306-1310），防止慢监听器把用户线程长时间卡在维护里。

### 4.2 `handle_upsert` 中的三道判据（sync base_cache.rs:1495；future base_cache.rs:1640）

1. **生命周期门**：`if !entry.entry_info().is_alive() { set_policy_gen(gen); return; }`（sync 1515-1518；future 1660-1663）。陈旧 Upsert 指向的 `EntryInfo` 已 retire → 不触碰任何策略状态，只同步 `policy_gen` 消除 dirty。
2. **已 admitted → 当 update**（sync 1524-1532）：调权重、`update_timer_wheel`、AO/WO move-to-back，`set_policy_gen(gen)`（带回绕保护，乱序到达的旧 gen 不会把 policy_gen 改小，entry_info.rs:204-222）。
3. **容量不足 + TinyLFU 准入**（sync 1583 起）：`admit` 扫描 victim 时跳过 `is_dirty() || !is_alive()`（sync 1718；future 1877）；victim 摘除与 reject 路径的 CHT 条件是 **`MiniArc::ptr_eq(旧 EntryInfo, 当前 EntryInfo) && entry_gen() == gen`**（sync 1555-1556、1655-1656；future 1704-1705、1812-1813）。
   - `entry_gen == gen` 防“同一 entry 的旧 op 摘除新版本”；
   - **`ptr_eq` 防“旧 entry 的 op 摘除重插入后的新 entry”**——新旧 `EntryInfo` 的 gen 号可能恰好相同（都从 1 开始），这是“旧 WriteOp 命中新 generation”场景的关键防线。
4. `handle_admit`（sync 1758；future 1917）入口还有一次 best-effort `is_alive()` 复检（sync 1785；future 1944），覆盖“检查后、admit 前被用户线程无锁 retire”的窗口；注释（sync 1759-1783）解释了残余竞争为何自愈：用户 remove/invalidate 必排 `WriteOp::Remove`，后者看到 admitted 后会 unlink 并回滚计数。

### 4.3 Timer wheel 与 expiry_gen

- `update_timer_wheel`（sync base_cache.rs:1810；future 1969）先原子取 `(expiration_time, expiry_gen)`（entry_info.rs:242），再与 `DeqNodes` 里存的节点指针 + 保存的 gen 配对，分四种情况 schedule/reschedule/deschedule/no-op。
- `TimerWheel::schedule`（`src/common/timer_wheel.rs:220`，gen 校验在 231-240）、`reschedule`（timer_wheel.rs:290，stale 校验 299-309）、`deschedule`（timer_wheel.rs:327，stale 校验 336-346）、`advance`（timer_wheel.rs:393）。节点内嵌的 gen（timer_wheel.rs:143-147）与期望值不符即拒绝操作，避免对已被复用/释放的 `NonNull` 节点动手（use-after-free 防护）。
- `advance(now)` 产生的 `TimerEvent::Expired` 还会被 `is_dirty()` 过滤（sync base_cache.rs:1981；future 2160），CHT 摘除条件再核真实过期（sync base_cache.rs:2000 `is_expired_by_per_entry_ttl`；future 2167）。

### 4.4 RemovalCause 矩阵

`RemovalCause` 定义于 `src/notification.rs:31`。

| Cause | 产生路径（sync / future） | 通知点 |
| --- | --- | --- |
| `Expired` | timer wheel 到期摘除（sync base_cache.rs:2006 / future base_cache.rs:2173）；AO/WO deque 扫描摘除（sync 2217 等 / future 2301、2442）；用户 update/invalidate 时发现旧值按旧时间戳已过期而**降级**（sync 2457/2463/2481/2487 / future `notify_upsert` 2655、`notify_invalidate` 2694） | 维护线程/任务内 |
| `Explicit` | `invalidate`/`remove`（默认 cause，sync `notify_invalidate` 2477 / future 2694）；`invalidate_entries_if` 谓词命中（sync `invalidate_entries` 2267 / future 2458）；`invalidate_all` 走 `valid_after` 时间戳闸门（base_cache.rs:404），不逐条通知 | invalidate 的通知在**用户线程**；谓词路径在维护任务内 |
| `Replaced` | insert/update 覆盖旧值：`notify_upsert` 默认 `Replaced`（sync 2453 / future 2655） | 用户线程/任务（在排 WriteOp 之前） |
| `Size` | TinyLFU reject 摘除候选（sync 1562 / future 1712）；准入 victim 摘除（sync 1610 / future 1763）；超大 entry 拒收（sync 1663 / future 1821）；LRU 容量驱逐（sync `evict_lru_entries` 2401 / future 2598） | 维护线程/任务内 |

### 4.5 Listener 在锁内还是锁外

- **sync**：`EvictionState::notify_entry_removal`（base_cache.rs:777）→ `RemovalNotifier::notify`（`src/notification/notifier.rs:25`）同步调用闭包，`catch_unwind` 包裹，panic 后永久禁用该监听器（notifier.rs:40-44）。
  - `Expired`/`Size`/谓词 `Explicit` 通知发生在 `do_run_pending_tasks` 内，**持有 `deqs` 与 `timer_wheel` 维护锁**（1203-1204），摘除前还持有该 key 的 per-key `Mutex`（如 1548-1549、1594-1595、1993-1994、2384-2385）。
  - `invalidate`/update 的通知在用户线程、key 锁内，但**不持维护锁**（cache.rs:1610；base_cache.rs:601），且在排 `WriteOp` 之前。
- **future**：`RemovalNotifier::notify`（`src/future/notifier.rs:27`）异步，创建 future 与 await 两处都 `catch_unwind`（notifier.rs:44-52）。维护路径上的通知在 `do_run_pending_tasks` 的 `async_lock` 持锁期间 await（base_cache.rs:1325-1326 → 1712/1763/1821/2173/2598）；用户路径通知配 `CancelGuard`，取消安全（future/cache.rs:1968-1985）。
- 含义：两种实现中，**慢监听器都会直接拖慢/阻塞维护循环**（future 中若 listener future 不 yield 还会占住 executor worker），这正是风险 2 的结构性来源。

---

## 5. 两种实现：共享部分与不可共享部分

**共享（`src/common/`）：**

- CHT：`src/cht.rs`、`src/cht/segment.rs`（乐观 bucket CAS；`insert_with_or_modify` segment.rs:408、`remove_entry_if_and` segment.rs:381 的 `with_previous_entry` 回调）。
- Entry 载体与身份：`EntryInfo`/`ValueEntry`/`KvEntry`/`KeyHash`/`OldEntryInfo`、`ReadOp`/`WriteOp`（concurrent.rs:173/88/32/386/303/312）；`DeqNodes` 与 timer expiry_gen（concurrent.rs:139-170）。
- 策略数据结构：`deque.rs`、`concurrent/deques.rs`（Window/Probation/Protected + WO 队列）、`frequency_sketch.rs`（TinyLFU）、`timer_wheel.rs`（分层时间轮 + expiry_gen 校验）、`time.rs`（`Clock`/Mock clock）。
- 触发阈值：`concurrent/constants.rs`（读/写 flush point 64、sync 间隔 300ms）。

**不可共享（按 flavor 各写一份，逻辑同构但同步原语不同）：**

- `BaseCache`/`Inner`：sync `src/sync/base_cache.rs`（parking_lot，阻塞式 `do_run_pending_tasks` 1190）vs future `src/future/base_cache.rs`（`async_lock`，1314）。
- housekeeper：sync `src/common/concurrent/housekeeper.rs` 用 `Mutex<()>` 的 `try_lock`（110），无后台线程，任何用户线程顺手 drain；future `src/future/housekeeper.rs` 用 `Mutex<Option<Shared<BoxFuture>>>` 持有可恢复任务（114/154），并发完成后 `write_op_ch_ready_event` 唤醒等待者。
- 入队等待策略：sync `schedule_write_op` 忙等 + `sleep(50µs)`（cache.rs:1819-1842）；future 自旋 4 次后 `event.listen().await`（base_cache.rs:633-696）。
- 通知与取消：future 独有 `CancelGuard`/`interrupted_op_ch`/`retry_interrupted_ops`（base_cache.rs:479、701）保证 await 期间任务取消不丢 op/通知；sync 没有取消问题。
- 监听器类型：sync `Arc<dyn Fn(...)>`（notification.rs:17）；future `Box<dyn Fn(...) -> ListenerFuture>`（notification.rs:20）。
- sync 独有 `src/sync/segment.rs`（分段缓存门面）。

不可共享的原因是同步/异步互斥原语与唤醒机制不同；共享的前提是策略状态的所有修改都收敛到持锁的维护函数，而 entry 身份判据全部在无锁层（原子标记 + 指针相等），两种 flavor 可以复用同一套判据。

---

## 6. 三个风险与复现

### 风险 1：旧 generation 的 `WriteOp` 在 key 被驱逐并重新插入后到达（最隐蔽）

机制：op 可能在有界 channel 里滞留（sync 入队忙等、future 满队列 await、任务被取消后经 interrupted 通道延迟重排都可能）。旧 `WriteOp::Upsert` 携带的是旧 `EntryInfo_A` 的 `MiniArc`。若 key 已被 invalidate/evict（A retire）并重新插入（全新 `EntryInfo_B`）：

- **没有 is_alive 门** → drain 时把 A 当成未 admitted 候选推进 AO/WO deque：CHT 里没有 A，产生 eviction 循环够不着的 zombie 节点，`weighted_size/entry_count` 只增不减，容量驱逐永久停滞（moka-rs/moka#590）。
- **reject/victim 摘除只比 entry_gen 不比指针** → A 的陈旧 op 若走到摘除路径，可能把同 hash 的 B 当作“自己”摘掉（B 更新两次后 `entry_gen=2`，与 A 的陈旧 gen 恰好相等）。

保护：`handle_upsert` 入口 is_alive 门（sync 1515 / future 1660）+ `handle_admit` 复检（sync 1785 / future 1944）+ 摘除条件 `MiniArc::ptr_eq` 与 `entry_gen` 双条件（sync 1555-1556、1655-1656）+ `retire()` 与 CHT CAS 同点（sync 1106-1122）。

**确定性复现（回归测试）**：`src/sync/base_cache.rs:3670`，`sync::base_cache::tests::gh590::stale_upsert_after_reinsert_cannot_corrupt_new_entry`。测试用与现有 gh590 测试相同的“测试钩子”方式（`reconfigure_for_testing()` 关闭 housekeeper 自动运行，直接 `do_insert_with_hash` 取 op、手工 `write_op_ch.send`、`do_run_pending_tasks` 单步 drain），步骤 t1→t5：

1. t1 insert K=1 并 drain（A，gen 1，已 admit）；
2. t2 `do_insert_with_hash(K=100)` 但**扣住返回的 Upsert A2 不发送**（gen 已变 2）；
3. t3 `queue_remove` + drain：A 被 retire/de-admit，K 离开 CHT；
4. t4 reinsert K=2（全新 EntryInfo_B，gen 1），再 update K=3（B gen 2，与陈旧 op 同号），drain；
5. t5 发送陈旧 A2 并 drain。

断言：CHT 中 K 仍在且值为 3；`entry_count()==1`（没有 orphan 策略节点）；随后插入 50 个 key 后 entry_count 稳定且 ≤ `MAX=4`（不发生停滞）。全程屏障式手工调度，无任何依赖时序的循环。

**去掉哪类检查会失败（已实证）**：临时把 sync `handle_upsert` 入口的 `is_alive()` 门（base_cache.rs:1515）与 `handle_admit` 复检（base_cache.rs:1785）短路（改为 `if false && …`）后，该测试在 `src/sync/base_cache.rs:3718` 处确定性失败（`entry_count` 左值 2、右值 1：陈旧 op 被 admit 成 orphan）；恢复后稳定通过。这属于 **is_retired/is_alive 生命周期检查这一类**；同理，若去掉摘除条件里的 `MiniArc::ptr_eq`，风险落在“旧 op 摘掉新 entry”，`entry_gen` 同号使单靠代际比较无法拦截。既有的 #590 复现测试（sync `src/sync/base_cache.rs:3531`、future `src/future/base_cache.rs:3838` 等）覆盖停滞形态；新测试覆盖“重插入后的新 entry 不被污染”形态，两者互补。

### 风险 2：listener 阻塞造成维护积压

机制：sync 的 `Expired`/`Size` 通知在持 `deqs`/`timer_wheel` 维护锁与 key 锁时同步执行（4.5 节）；慢监听器拉长每次 drain 的持锁时间，其他用户线程的 `try_run_pending_tasks` 抢不到 `run_lock`（housekeeper.rs:110），写 channel 填满后 `schedule_write_op` 在 cache.rs:1831-1839 忙等 sleep，insert 尾延迟暴涨；future 中 listener future 若阻塞 worker 或长时间不完成，`Shared` 维护任务（housekeeper.rs:177）被拖住，满队列的写者全部挂在 `ch_ready_event`（base_cache.rs:692）。

复现步骤（确定性，无需海量循环）：

1. `Cache::builder(4).eviction_listener(|_,_,cause| { std::thread::sleep(200ms); … })`（sync）；future 用 `async move { tokio::time::sleep(…).await }`。
2. 用 `Barrier`/`AtomicBool` 让 listener 第一次收到 `Size` 时阻塞（受控栅栏，而非随机 sleep 碰运气）。
3. 主线程连续 insert 8 个 key 触发驱逐；用受控 `Clock` 或直接在 t4 释放栅栏。
4. 断言：栅栏期间 `write_op_ch.len()` 达到容量上限且后续 insert 的耗时显著放大；释放后 drain 收敛、通知顺序与 cause 正确。
5. 缓解项验证：设置 `HousekeeperConfig::maintenance_task_timeout`，确认单次维护按超时退出（sync base_cache.rs:1301-1307），`more_entries_to_evict` 标志（housekeeper.rs:87-96）使下轮续做。

### 风险 3：可变 `Expiry` 与并发读取重排 timer

机制：`expire_after_read` 是用户提供的可变策略，命中路径在用户线程调用并 `set_expiration_time`（CAS，bump `expiry_gen`；entry_info.rs:261）；维护线程同时在 `apply_reads`/`handle_upsert` 中 `update_timer_wheel`（base_cache.rs:1810）。若时间与 gen 非原子读取，可能把新过期时间配旧节点指针（TOCTOU），或 reschedule/deschedule 到已被复用的 timer 节点。

防护：`expiration_state` 打包为单个 `AtomicU64` 原子快照（entry_info.rs:99、242）；`DeqNodes` 中节点指针与 expiry_gen 成对存取（concurrent.rs:164-170、260-265）；timer wheel 的 schedule/reschedule/deschedule 三处 gen 校验（timer_wheel.rs:231、299、336）；`ReadOp::Hit` 带 `is_expiry_modified`，读 op 丢失（channel 满）时由到期复核兜底（base_cache.rs:320-336、1997）。

复现步骤（确定性）：

1. `Expiry` 实现对同一 key 的 `expire_after_read` 在极短 TTL 与极长 TTL 间按调用计数交替；用 `common::time::Clock::mock()`（受控时钟，见 `src/common/time.rs`）。
2. 一个线程循环 `get`（推进 expiry_gen），维护由主线程在栅栏后**单步** `run_pending_tasks`：先制造一批读 op，再把 mock clock 推进到短 TTL 之后、长 TTL 之前，drain。
3. 断言：只有当前最后一次策略决定为“短 TTL”的 entry 被摘除；长 TTL entry 存活，且不出现 timer wheel 的 gen-mismatch panic（对照 `tests/timer_wheel_panic_test.rs` 的钩子）。
4. 去掉打包原子读或任一 expiry_gen 校验后，该测试应能观察到误摘除（长 TTL entry 消失）或 stale 节点拒绝计数，从而证明防护的必要性。

> 注：benchmark 只证明吞吐，不证明上述任何不变量；正确性证据来自上述确定性测试与既有 #590 复现测试（`src/sync/base_cache.rs:3531`、`src/future/base_cache.rs:3838`、`tests/eviction_stall_sync.rs`、`tests/eviction_stall_future.rs`）。

---

## 7. 验证

- 构建：`cargo build --all-targets --features 'sync future'`。
- 测试：`cargo test --features 'sync future'`（新测试 + 既有全部测试通过；`test -s ANALYSIS.md`）。
- 新增测试：`cargo test --features 'sync future' stale_upsert_after_reinsert_cannot_corrupt_new_entry`。

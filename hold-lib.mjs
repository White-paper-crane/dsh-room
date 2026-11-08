// A11（2026-11-08）：忙 / 冷却只是 hold，不 ack。忙一阵没事，但一个长任务能忙几十分钟，
// 这段时间同房消息会一直冻着：游标不走、两端都不 ack，等忙完一口气全涌进来。
// 现在给 hold 设上限：同一条消息被 hold 超过 HOLD_MAX_MS，就放弃这一轮的注入，
// 在房里说一句「先放下了，回头用 read 看历史」，然后 ack 推游标。
// 为什么是放弃而不是继续等：消息本身还在房间历史里，不会丢；无限等待才会丢——它把整条流水线卡住，
// 而卡住的这段时间里新消息还会不断进来。
export const HOLD_MAX_MS = Number(process.env.DS_TEAMS_HOLD_MAX_MS) || 10 * 60e3;

// 只记「第一次被 hold 的时刻」，同一条消息反复 hold 不会刷新计时。
export function createHoldClock() {
  const since = new Map();
  return {
    mark(key, now = Date.now()) {
      if (!since.has(key)) since.set(key, now);
      return since.get(key);
    },
    at(key) {
      return since.get(key) || 0;
    },
    clear(key) {
      since.delete(key);
    },
    // 只留还挂在 pending 里的条目，避免这张表长年长大。传数组或 Set 都行。
    prune(keys) {
      const set = keys instanceof Set ? keys : new Set(keys || []);
      for (const k of since.keys()) if (!set.has(k)) since.delete(k);
    },
    expired(key, now = Date.now(), maxMs = HOLD_MAX_MS) {
      const t = since.get(key) || 0;
      return t > 0 && now - t >= maxMs;
    },
    get size() {
      return since.size;
    },
  };
}

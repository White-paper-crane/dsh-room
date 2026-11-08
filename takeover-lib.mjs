// 掉线接管的判据（A12，2026-11-08）。
// 为什么抽成纯函数：这段逻辑以前长在 server.mjs 的巡检里，只能靠「服务跑起来」间接验证；
// 而 A12 的坑正是判据本身——只看 claimedAt 超 15 分钟就转卡，忙端刚接下的大活也会被转走，
// 那一端恢复后就和接手端同时干同一张卡（两端各跑一遍、台账互相覆盖）。
//
// 现在转一张卡要同时满足：
//   ① 只有一端失联（两端都断/都正常都不动作）；
//   ② 卡在失联端名下且状态活跃；
//   ③ 认领已超过 takeoverClaimMinMs；
//   ④ 最近 takeoverStaleMs 内没有任何进度信号（heartbeat / updatedAt / claimedAt 三者取最新）；
//   ⑤ 这张卡在 takeoverCooldownMs 内没被接管过（防两端来回踢）。
import { ACTIVE } from "./protocol.mjs";

export const TAKEOVER_DEFAULTS = Object.freeze({
  takeoverClaimMinMs: 15 * 60e3,
  takeoverStaleMs: 5 * 60e3,
  takeoverCooldownMs: 30 * 60e3,
});

function ms(v) {
  return Date.parse(v || "") || 0;
}

// 进度信号取三者最新：心跳 > 台账更新 > 认领时刻。
export function lastProgressMs(e) {
  return Math.max(0, ms(e.heartbeatAt), ms(e.updatedAt), ms(e.claimedAt));
}

// 返回 [{ key, from, to, title }]，按 key 排序，便于日志与测试对齐。
export function decideTakeovers({ now = Date.now(), down = {}, tasks = {}, limits = {} } = {}) {
  const minClaim = Number(limits.takeoverClaimMinMs) || TAKEOVER_DEFAULTS.takeoverClaimMinMs;
  const staleMs = Number(limits.takeoverStaleMs) || TAKEOVER_DEFAULTS.takeoverStaleMs;
  const cooldownMs = Number(limits.takeoverCooldownMs) || TAKEOVER_DEFAULTS.takeoverCooldownMs;
  const downMe = down.me === true;
  const downDaman = down.daman === true;
  if (downMe === downDaman) return []; // 都失联或都正常：没有「谁接谁」
  const from = downMe ? "me" : "daman";
  const to = from === "me" ? "daman" : "me";
  const out = [];
  for (const [key, e] of Object.entries(tasks)) {
    if (!e || typeof e !== "object") continue;
    if (e.assignee !== from) continue;
    if (!ACTIVE.has(e.state)) continue;
    const claimedAt = ms(e.claimedAt);
    if (!claimedAt || now - claimedAt <= minClaim) continue;
    const progress = lastProgressMs(e);
    if (progress && now - progress < staleMs) continue;
    const handoffAt = ms(e.handoff && e.handoff.at) || ms(e.handoffAt);
    if (handoffAt && now - handoffAt < cooldownMs) continue;
    out.push({ key, from, to, title: String(e.title || e.taskId || key) });
  }
  out.sort((a, b) => a.key.localeCompare(b.key));
  return out;
}

// 落账：改派 + 留 handoff 标记（哪一端接的、什么时候接的）+ 一条 takeover 事件。
// handoff.at 既给冷却判据用，也给桥的 nudge 栅栏用（只有接手端会提醒自己去跑）。
export function applyTakeover(entry, { from, to, title, now = Date.now() }) {
  const at = new Date(now).toISOString();
  entry.assignee = to;
  entry.handoff = { from, to, at };
  entry.updatedAt = at;
  const events = Array.isArray(entry.events) ? entry.events : [];
  events.push({ at, type: "takeover", by: "system", detail: `${from} -> ${to}: ${title}`.slice(0, 500) });
  entry.events = events.slice(-200);
  return entry;
}

// stuck-guard-lib.mjs —— 「会话 phase 长期 running 不收敛」自愈判定（BUG #2，2026-11-05）。
//
// 事故（房 room-912504，2026-11-05 05:53）：小满会话的 phase 卡在 running 再也不回 idle，
// 桥 deliver() 的第 ④ 步 busy-hold 每 2 秒重试、永远 hold，60 秒后被对端 takeover 接管。
// 桥的 hold 是对的（不该往正在跑的回合里硬塞），缺的是**自愈**：原来唯一出路只有被人接管。
// 本库给两端桥提供同一条判据：某个会话的 phase 非 idle 连续超过 STUCK_MS，且期间本端桥至少
// 给它递过一次消息（= 有真实 work 在等），就判定卡死，由桥 agent.cancel({kind:"user"}) 强停那轮。
//
// 三条护栏（防止把「正常在做长任务」当卡死杀掉）：
//   ① 有进展就清零：最后一条带 seq 的日志时间戳往前走（回合每推进一步都写日志）→ 视为仍在动；
//   ② 抖动锁：判定卡死后下一轮重新计时，循环只能拉长不能加速；
//   ③ 房内真熟人正在往回写（回话 / review 落库）→ 不杀。
// 纯函数库：不读文件、不碰定时器，便于单测（tests/stuck-guard.test.mjs）。
import fs from "node:fs";

export const STUCK_MS = 180_000;   // phase 非 idle 连续多久算卡死（3 分钟：正常一轮出卡远小于此）
export const STUCK_HOLDS = 1;      // 期间至少被 hold 过几次（有真实 work 在等，不是空转）

const IDLE = new Set(["idle", "waiting", "completed", "maintenance"]);

// phase 归一：桥里 ag.phase 可能是对象 {kind} 也可能是字符串。
export function phaseKind(agent) {
  const ph = agent && agent.phase;
  if (typeof ph === "object" && ph) return String(ph.kind ?? "");
  return String(ph ?? "");
}

// phase 是否「在跑」（非 idle / waiting / completed / maintenance）。
export function isBusyKind(kind) {
  return !!kind && !IDLE.has(kind);
}

// 注入记账：谁往哪个会话递过消息、这次尝试被 hold 在什么 phase。
export function createHoldBook() {
  return new Map();   // sessionId -> { firstBusyAt, lastSeq, holds, lastHoldAt }
}

export function noteHold(book, sessionId, seq, kind, now = Date.now()) {
  if (!sessionId) return null;
  const e = book.get(sessionId) || { firstBusyAt: 0, lastSeq: null, holds: 0, lastHoldAt: 0 };
  if (!e.firstBusyAt) e.firstBusyAt = now;
  // 2026-11-07：lastHoldAt 记的是「当前这条消息第一次被 hold 的时刻」，不是每次重试。
  // 以前每 2 秒重试都刷新它，decideStuck 的 jobAt 就永远被顶到 now，判据恒为「还在动」，
  // 自愈成了死代码（两端日志 stuck-reset 一直是 0）。只有换了新消息才重新计时。
  const s = Number(seq);
  if (!e.lastHoldAt || (Number.isFinite(s) && e.lastSeq !== s)) e.lastHoldAt = now;
  if (Number.isFinite(seq)) e.lastSeq = Number(seq);
  if (isBusyKind(kind)) e.holds += 1;
  book.set(sessionId, e);
  return e;
}

// 真有一条消息被注入成功 → 说明 agent 能跑，清掉这个会话的记账。
export function clearHold(book, sessionId) {
  if (sessionId) book.delete(sessionId);
}

// 一条消息处理完（注入成功 / 判定跳过 / 被对端接管）→ 递送已了结，清零。
export function settleHold(book, sessionId, seq) {
  if (!sessionId) return;
  const e = book.get(sessionId);
  if (!e) return;
  if (e.lastSeq !== null && Number.isFinite(seq) && Number(seq) >= e.lastSeq) book.delete(sessionId);
}

// 从桥的事件日志文件取「最后一条带 seq 的时间」（时间戳往前 = 那一轮 agent 还在出新动作）。
// 只读尾部 TAIL_BYTES：日志一天几十 MB，不能整读。
const TAIL_BYTES = 64 * 1024;
export function lastSeqTimeFromLog(file, sinceMs = 0) {
  let buf;
  try {
    const st = fs.statSync(file);
    const size = st.size;
    if (!size) return 0;
    const start = Math.max(0, size - TAIL_BYTES);
    buf = Buffer.alloc(size - start);
    const fd = fs.openSync(file, "r");
    try { fs.readSync(fd, buf, 0, buf.length, start); } finally { fs.closeSync(fd); }
  } catch { return 0; }
  const lines = buf.toString("utf8").split("\n");
  let best = 0;
  // 保留最后 50 条带 seq 的行就够判「有没有在动」
  let seen = 0;
  for (let i = lines.length - 1; i >= 0 && seen < 50; i--) {
    const ln = lines[i];
    if (!ln || ln.indexOf('"seq"') < 0) continue;
    let j;
    try { j = JSON.parse(ln); } catch { continue; }
    if (typeof j.seq !== "number") continue;
    const t = Date.parse(j.at || "");
    if (!Number.isFinite(t) || t < sinceMs) continue;
    if (t > best) best = t;
    seen++;
  }
  return best;
}

/**
 * 卡死判定（纯函数）。
 * @param {object} now0
 *   kind        本端身份（me / daman），只作日志用
 *   now         当前毫秒
 *   holds       hold 记账 Map（createHoldBook）
 *   activity    [{ id, kind, sinceMs, holds, lastSeqAt }] 本端可见的会话活动（同进程下 = 两端相加）
 *   lastRoomAt  (roomId) => 毫秒：该房最后一条对端消息时间（房内有人在往回写）
 *   roomOf      (sessionId) => roomId|null：本端台账里会话→房；主会话返回 null
 * @returns {{action:"reset"|"none", sessionId?:"", kind?:"", stuckMs?:number, reason?:"", jobAt?:number, hold?:object}}
 */
export function decideStuck({ now = Date.now(), holds, activity = [], lastRoomAt = () => 0, roomOf = () => null } = {}) {
  for (const a of activity) {
    if (!a || !a.id) continue;
    if (!isBusyKind(a.kind)) continue;
    const book = holds?.get?.(a.id) || null;
    // 计时窗口起点 = 这一轮 busy 的开始，或「当前这条消息第一次被 hold」的时刻，取晚者。
    // 有新消息进来就重新计时（那是新活）；同一条消息的重试不重置（重试不是进展）。
    const since = Math.max(Number(a.sinceMs) || 0, Number(book?.lastHoldAt) || 0);
    if (!since || now - since < STUCK_MS) continue;
    const holdsTotal = (Number(a.holds) || 0) + (Number(book?.holds) || 0);
    if (holdsTotal < STUCK_HOLDS) continue;   // 没人给它递过东西 → 可能在干别的活，不碰
    // ① 只认真进展：桥事件日志里又出现了带 seq 的新行。hold 重试、注入尝试都不算。
    const jobAt = Number(a.lastSeqAt) || 0;
    if (jobAt > since) continue;               // 有进展 → 还在动，清零重算
    const rid = roomOf(a.id);
    const roomAt = rid ? Number(lastRoomAt(rid)) || 0 : 0;
    if (roomAt > since) continue;              // ③ 房内刚有人说话、本端正在往回写 → 不杀
    return {
      action: "reset",
      sessionId: String(a.id),
      kind: String(a.kind),
      stuckMs: now - since,
      reason: rid ? "busy-hold timeout" : "main-session busy-hold timeout",
      jobAt,
      hold: a,
    };
  }
  return { action: "none" };
}
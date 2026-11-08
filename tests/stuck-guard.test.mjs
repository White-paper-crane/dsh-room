// stuck-guard-lib 单测（BUG#2 2026-11-05）：卡死自愈判定的全部护栏。
// 纯函数级：不连服务、不碰宿主，只喂时间戳，验证「什么时候该 cancel、什么时候绝对不许动」。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  STUCK_MS, isBusyKind, phaseKind, createHoldBook, noteHold, clearHold, settleHold,
  lastSeqTimeFromLog, decideStuck,
} from "../stuck-guard-lib.mjs";

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; } else { fail++; console.error("FAIL: " + name); } };

const T0 = Date.parse("2026-11-05T06:00:00.000Z");   // 事故时刻附近
const MIN = 60_000;

// ① phase 归一与 busy 判定：对象/字符串都认；idle/waiting/completed/maintenance 都不算在跑。
ok(phaseKind({ phase: { kind: "running" } }) === "running", "phaseKind object");
ok(phaseKind({ phase: "tool" }) === "tool", "phaseKind string");
ok(phaseKind({}) === "", "phaseKind missing");
ok(isBusyKind("running") && isBusyKind("tool") && isBusyKind("thinking"), "running/tool/thinking are busy");
for (const k of ["idle", "waiting", "completed", "maintenance", ""]) ok(!isBusyKind(k), "not busy: " + k);

// ② 记账：firstBusyAt 只记第一次（连刷 30 次 hold 也不往后推），holds 累加，seq 取最新。
const book = createHoldBook();
for (let i = 0; i < 30; i++) noteHold(book, "session-aaa", 8268 + i, "running", T0 + i * 2000);
record({ firstBusyAt: book.get("session-aaa").firstBusyAt === T0, holds: book.get("session-aaa").holds === 30, lastSeq: book.get("session-aaa").lastSeq === 8297 });
function record(o) { for (const [k, v] of Object.entries(o)) ok(v, "hold book " + k); }

// ③ 递进去就销账；没投过的会话查不到账。
clearHold(book, "session-aaa");
ok(!book.has("session-aaa"), "clearHold removes book");
settleHold(book, "session-nope", 1);
ok(true, "settleHold is a no-op for unknown session");

// ④ settleHold 只销「已经了结过」的那笔：seq 落后于最新 hold 的账保留（后面还有 work 在等）。
const book2 = createHoldBook();
noteHold(book2, "s", 100, "running", T0);
noteHold(book2, "s", 200, "running", T0 + 1000);
settleHold(book2, "s", 150);
ok(book2.has("s"), "settleHold keeps book when a newer seq is still pending");
settleHold(book2, "s", 200);
ok(!book2.has("s"), "settleHold drops book when latest seq settled");

// ⑤ 核心：事故复现——phase 卡 running 4 分钟、被 hold 过、没有任何进展 → reset。
const mk = (sinceMs, holds = 3, lastSeqAt = 0) => [{ id: "session-stuck", kind: "running", sinceMs, holds, lastSeqAt }];
const r5 = decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - 4 * MIN) });
ok(r5.action === "reset" && r5.sessionId === "session-stuck" && r5.stuckMs === 4 * MIN, "stuck 4min + held -> reset");

// ⑥ 没到阈值不动（3 分钟内都可能是一轮正常出卡）。
ok(decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - STUCK_MS + 1000) }).action === "none", "just under threshold -> none");

// ⑦ 没人递过消息（holds=0，且记账里也没有）→ 不碰：它可能在干别的活。
ok(decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - 5 * MIN, 0) }).action === "none", "no hold at all -> none");

// ⑧ 护栏①：日志有新进展（lastSeqAt 比干活起点晚）→ 视为还在动，不杀。
ok(decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - 4 * MIN, 3, T0 - 30_000) }).action === "none", "progress in event log -> none");

// ⑨ 记账里的 hold 也算数：activity.holds=0 但 holdBook 有账 → 仍然判卡死。
const b9 = createHoldBook();
noteHold(b9, "session-stuck", 9001, "running", T0 - 4 * MIN);
ok(decideStuck({ now: T0, holds: b9, activity: mk(T0 - 4 * MIN, 0) }).action === "reset", "hold book counts as work waiting");

// ⑩ 护栏③：房内刚有对端消息 / 本端刚注入 → 不杀。
const r10a = decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - 4 * MIN), roomOf: () => "room-x", lastRoomAt: () => T0 - 60_000 });
ok(r10a.action === "none", "peer spoke recently in that room -> none");
const r10b = decideStuck({ now: T0, holds: createHoldBook(), activity: mk(T0 - 4 * MIN), roomOf: () => "room-x", lastRoomAt: () => T0 - 5 * MIN });
ok(r10b.action === "reset", "peer silent while stuck -> reset");

// ⑪ 主会话（不在任何房的台账里）同样受管：reason 区分，房护栏不适用。
const r11 = decideStuck({ now: T0, holds: createHoldBook(), activity: [{ id: "session-main", kind: "tool", sinceMs: T0 - 3 * MIN, holds: 2, lastSeqAt: 0 }], roomOf: () => null });
ok(r11.action === "reset" && r11.reason === "main-session busy-hold timeout", "main session guarded too");

// ⑫ 多个会话里只挑第一个卡死的，idle 的会话一律跳过。
const r12 = decideStuck({
  now: T0,
  holds: createHoldBook(),
  activity: [
    { id: "s-idle", kind: "idle", sinceMs: T0 - 99 * MIN, holds: 9, lastSeqAt: 0 },
    { id: "s-b", kind: "running", sinceMs: T0 - 5 * MIN, holds: 1, lastSeqAt: 0 },
  ],
});
ok(r12.action === "reset" && r12.sessionId === "s-b", "skips idle sessions, picks the stuck one");

// ⑬ 日志尾部取「最后一条带 seq 的时间」：老行不干扰、坏行不炸、sinceMs 能过滤。
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "stuck-log-"));
const file = path.join(dir, "bridge.log");
const at = (s) => new Date(T0 + s * 1000).toISOString();
fs.writeFileSync(file, [
  JSON.stringify({ at: at(-600), action: "inject", seq: 1 }),
  JSON.stringify({ at: at(-300), action: "hold", seq: 8000 }),
  "not json at all",
  JSON.stringify({ at: at(-30), action: "alive" }),          // 没有 seq：不算进展
  JSON.stringify({ at: at(-10), action: "inject", seq: 9000 }),
  "",
].join("\n"), "utf8");
ok(lastSeqTimeFromLog(file) === T0 - 10_000, "lastSeqTimeFromLog picks newest seq'd line");
ok(lastSeqTimeFromLog(file, T0 - 100_000) === T0 - 10_000, "lastSeqTimeFromLog honours sinceMs");
ok(lastSeqTimeFromLog(path.join(dir, "missing.log")) === 0, "missing log -> 0");

// ⑭ 回归（2026-11-07）：同一条消息被 hold 重试 120 次、phase 一直 running 4 分钟 → 仍判 reset。
//    事故根因：每次重试都刷新 lastHoldAt，decideStuck 的 jobAt 被顶到 now，判定恒为「还在动」。
const b14 = createHoldBook();
for (let i = 0; i < 120; i++) noteHold(b14, "session-stuck", 9001, "running", T0 - 4 * MIN + i * 2000);
const r14 = decideStuck({ now: T0, holds: b14, activity: [{ id: "session-stuck", kind: "running", sinceMs: T0 - 4 * MIN, holds: b14.get("session-stuck").holds, lastSeqAt: 0 }] });
ok(r14.action === "reset", "repeated retries of same seq -> still reset");

// ⑮ 等待期间真来了新消息（seq 变了）→ 那是新活，计时窗口重新起算，不杀。
const b15 = createHoldBook();
noteHold(b15, "session-stuck", 9001, "running", T0 - 4 * MIN);
noteHold(b15, "session-stuck", 9002, "running", T0 - 60_000);
const r15 = decideStuck({ now: T0, holds: b15, activity: [{ id: "session-stuck", kind: "running", sinceMs: T0 - 4 * MIN, holds: b15.get("session-stuck").holds, lastSeqAt: 0 }] });
ok(r15.action === "none", "new seq restarts the window");

fs.rmSync(dir, { recursive: true, force: true });
console.log(`\nstuck-guard: ${pass} pass / ${fail} fail`);
process.exit(fail ? 1 : 0);
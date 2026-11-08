// ds-teams · dispatch 台账（C 端 / 大满侧）
// 唯一事实源：data/dispatch-state.json（可用 DS_TEAMS_LEDGER 覆盖，测试用）。
// 给 dispatch.mjs CLI（大满/小满回合都可调）与桥接 watchdog/promote 共用。
// 纪律：先写台账再 spawn；写入 = tmp + rename；终态不再触发 spawn。
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ACTIVE, TERMINAL, CAP, LIMITS, TASK_STATES } from "./protocol.mjs";
// T6（2026-11-07）：名额计算改成纯函数，房间任务与项目任务分桶；见 capacity-lib.mjs。
import { tally, admit } from "./capacity-lib.mjs";

export { CAP };

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const LEDGER = process.env.DS_TEAMS_LEDGER || path.join(ROOT, "data", "dispatch-state.json");
// ACTIVE 占并发名额；queued 排队但不占名额。状态定义见 protocol.mjs。
const DEFAULTS = {
  claimedMs: LIMITS.stallClaimedMs,
  runMs: LIMITS.stallRunMs,
  queuedMs: LIMITS.stallQueuedMs,
  maxAttempts: LIMITS.maxAttempts,
};

export const taskKey = (roomId, taskId) => String(roomId).trim() + "::" + String(taskId).trim();

const nowIso = () => new Date().toISOString();
const ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,80}$/;
const MAX_ARTIFACT_BYTES = 50 * 1024 * 1024;

function pushEvent(e, type, by, detail) {
  e.events = Array.isArray(e.events) ? e.events : [];
  e.events.push({ at: nowIso(), type: String(type || ""), by: String(by || ""), detail: String(detail || "").slice(0, 500) });
  if (e.events.length > 200) e.events = e.events.slice(-200);
}

function sha256File(fp) {
  return crypto.createHash("sha256").update(fs.readFileSync(fp)).digest("hex");
}

function cleanList(v, max = 20) {
  if (!Array.isArray(v)) return [];
  return v.map((x) => String(x || "").trim()).filter(Boolean).slice(0, max);
}

export function readLedger() {
  try {
    const j = JSON.parse(fs.readFileSync(LEDGER, "utf8"));
    if (j && typeof j === "object" && j.tasks && typeof j.tasks === "object") return j;
  } catch {}
  return { version: 1, tasks: {} };
}

export function writeLedger(db) {
  fs.mkdirSync(path.dirname(LEDGER), { recursive: true });
  const tmp = LEDGER + ".tmp-" + process.pid;
  fs.writeFileSync(tmp, JSON.stringify(db, null, 2), "utf8");
  fs.renameSync(tmp, LEDGER);
}

// 跨进程互斥：桌面端桥接与 QQ 端会话都会写台账，读-改-写必须串行。
// A3（2026-11-08）修的是两件事：
//   ① 以前拿到锁后收尾无条件删锁文件。锁一旦被别人破掉并重建，这一删就删掉了别人的锁，
//      互斥当场失效。现在锁里写 {pid, token}，释放前核对 token，不是自己的锁不删。
//   ② 以前 mtime 超 10 秒就破锁，两个进程可能同时动手。现在先看持有者 pid 是否还活着，
//      死了立即破；活着只认超时（默认 10 秒），并用原子 rename 抢走再删——同时破只有一个能赢。
// 等不到锁时抛 code=LEDGER_BUSY（不再是一句笼统的 timeout），调用方按 tick 重试即可。
const LOCK = LEDGER + ".lock";
const LOCK_TIMEOUT_MS = Number(process.env.DS_TEAMS_LOCK_TIMEOUT_MS) || 5000;
const LOCK_STALE_MS = Number(process.env.DS_TEAMS_LOCK_STALE_MS) || 10000;

function pidAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return Boolean(e && e.code === "EPERM"); }
}

function readLockRecord() {
  try { return JSON.parse(fs.readFileSync(LOCK, "utf8")); } catch { return null; }
}

function ledgerBusy(msg) {
  const e = new Error(msg);
  e.code = "LEDGER_BUSY";
  return e;
}

// 抢锁：wx 原子创建；抢到就把身份写进去，写失败不影响持锁（还有 pid 兜底）。
function tryLock(token) {
  let fd;
  try {
    fd = fs.openSync(LOCK, "wx");
  } catch (e) {
    if (e && e.code === "EEXIST") return null;
    throw e;
  }
  try { fs.writeSync(fd, JSON.stringify({ pid: process.pid, token, at: Date.now() })); } catch {}
  return fd;
}

// 破锁：只在「确实死了」时动手；rename 抢走是原子的，两个进程同时破只有一个成功。
// 有 pid 就只认活性——活着的一律不抢，哪怕它握了很久（卡住的进程由调用方超时报错重试，
// 抢活锁才是真的会把两个人的读改写搅在一起，那是 A3 的老毛病）。没有身份信息的老锁才按时间兜底。
function breakStaleLock(token, staleMs) {
  let st;
  try { st = fs.statSync(LOCK); } catch { return true; }   // 已经没了：当作已破
  const rec = readLockRecord();
  if (rec && Number(rec.pid)) {
    if (pidAlive(Number(rec.pid))) return false;
  } else if (Date.now() - st.mtimeMs <= staleMs) {
    return false;
  }
  const grave = LOCK + ".stale-" + token;
  try { fs.renameSync(LOCK, grave); } catch { return false; }   // 别人先破了，或文件还被持锁进程占着
  try { fs.rmSync(grave, { force: true }); } catch {}
  return true;
}

function withLedgerLock(fn, { timeoutMs = LOCK_TIMEOUT_MS, staleMs = LOCK_STALE_MS } = {}) {
  fs.mkdirSync(path.dirname(LEDGER), { recursive: true });
  const token = `${process.pid}-${crypto.randomBytes(6).toString("hex")}`;
  const start = Date.now();
  let fd = null;
  while (fd === null) {
    fd = tryLock(token);
    if (fd !== null) break;
    if (breakStaleLock(token, staleMs)) continue;
    if (Date.now() - start > timeoutMs) {
      const rec = readLockRecord();
      throw ledgerBusy(`ledger busy: waited ${timeoutMs}ms, holder ${rec && rec.pid ? "pid " + rec.pid : "unknown"}`);
    }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
  }
  try {
    return fn();
  } finally {
    try { fs.closeSync(fd); } catch {}
    try {
      const rec = readLockRecord();
      if (rec && rec.token === token) fs.rmSync(LOCK, { force: true });
    } catch {}
  }
}

export function counts(db = readLedger()) {
  return tally(db);
}

// 这个任务现在能不能占名额：全局 + 自己那个桶（房间 / 项目各算各的）。
const fits = (entry, db = readLedger()) => admit(entry, db, CAP);

// 原子 claim：同 (roomId, taskId) 只成功一次。
// 名额满时落 queued（不 spawn），返回 queued:true + 排队位置；有位置才 claimed。
function claimLocked(roomId, taskId, seq, extra = {}) {
  const db = readLedger();
  const k = taskKey(roomId, taskId);
  const prev = db.tasks[k];
  if (prev) return { ok: false, duplicate: true, key: k, state: prev.state, entry: prev };
  const now = new Date().toISOString();
  const entry = {
    roomId: String(roomId).trim(),
    taskId: String(taskId).trim(),
    assignee: extra.assignee ?? "daman",
    state: "claimed",
    attempts: 1,
    sourceSeq: seq ?? null,
    claimedAt: now,
    updatedAt: now,
    subtasks: [],
    teammates: [],
  };
  if (extra.deadlineAt) entry.deadlineAt = extra.deadlineAt;
  let queued = false;
  if (!fits(entry, db).ok) {
    entry.state = "queued";
    entry.queuedAt = now;
    entry.queuePos = Object.values(db.tasks).filter((e) => e.state === "queued").length + 1;
    queued = true;
  }
  db.tasks[k] = entry;
  writeLedger(db);
  return { ok: true, duplicate: false, queued, key: k, entry };
}

// A10（2026-11-08）：`dispatch set` 以前什么都能写，也不看名额——
// 一句 `set <key> running` 就能把 3 个名额撑到 4 个，或者把 done 戳回 claimed。
// 现在按表校验：非法迁移直接拒（中文提示允许哪些），从空闲进活跃状态要先过名额检查。
const TRANSITIONS = Object.freeze({
  queued: ["claimed", "spawned", "running", "blocked", "aborted", "timeout", "failed"],
  claimed: ["spawned", "running", "reported", "queued", "blocked", "aborted", "timeout", "interrupted", "failed", "done"],
  spawned: ["claimed", "running", "reported", "queued", "blocked", "aborted", "timeout", "interrupted", "failed", "done"],
  running: ["spawned", "reported", "blocked", "aborted", "timeout", "interrupted", "failed", "done"],
  reported: ["running", "done", "blocked", "aborted", "timeout", "interrupted"],
  blocked: ["queued", "claimed", "running", "aborted", "done", "timeout"],
  timeout: ["queued", "claimed", "running", "aborted", "done"],
  interrupted: ["queued", "claimed", "running", "aborted", "done"],
  done: ["aborted", "reported"],
  failed: ["queued", "aborted", "done"],
  aborted: ["queued"],
  reviewed: ["done", "aborted"],
});

function setStateLocked(key, state, patch = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (!TASK_STATES.includes(state)) {
    return { ok: false, error: `unknown state ${state}（合法状态：${TASK_STATES.join("|")}）` };
  }
  if (state !== e.state) {
    const allowed = TRANSITIONS[e.state] || [];
    if (!allowed.includes(state)) {
      return { ok: false, error: `illegal transition ${e.state} -> ${state}（允许：${allowed.join("|") || "无"}）` };
    }
    // 空转 → 活跃：得有名额，否则 set 就成了绕过并发上限的后门。
    if (ACTIVE.has(state) && !ACTIVE.has(e.state)) {
      const a = fits(e, db);
      if (!a.ok) {
        return {
          ok: false,
          error: `cap full: ${a.bucket} ${a.used}/${a.cap}, global ${a.global}/${a.globalCap}`,
          cap: { bucket: a.bucket, key: a.key, used: a.used, cap: a.cap, global: a.global, globalCap: a.globalCap },
        };
      }
    }
  }
  const { by = "", ...rest } = patch;
  const from = e.state;
  e.state = state;
  // 从 timeout/interrupted 恢复时清掉旧的失败原因，避免日志里挂着已失效的借口。
  if (state !== "timeout") delete e.timeoutReason;
  if (state !== "blocked") delete e.blockReason;
  Object.assign(e, rest, { updatedAt: new Date().toISOString() });
  if (from !== state) pushEvent(e, "state", by, `${from} -> ${state}`);
  writeLedger(db);
  return { ok: true, entry: e };
}

// 名额释放后按 sourceSeq 顺序把 queued 提升为 claimed；返回被提升的 key 列表。
function promoteLocked() {
  const db = readLedger();
  const q = Object.entries(db.tasks)
    .filter(([, e]) => e.state === "queued")
    .sort((a, b) => (a[1].sourceSeq ?? 0) - (b[1].sourceSeq ?? 0));
  const out = [];
  for (const [k, e] of q) {
    if (!fits(e, db).ok) continue;
    e.state = "claimed";
    e.updatedAt = new Date().toISOString();
    delete e.queuePos;
    out.push(k);
  }
  if (out.length) writeLedger(db);
  return out;
}

// spawn 失败：attempts+1；未到 maxAttempts 回 queued 重试，到了标 failed。
function failLocked(key, reason = "") {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  e.attempts = (e.attempts ?? 1) + 1;
  e.lastError = String(reason || "").slice(0, 300);
  e.updatedAt = new Date().toISOString();
  const max = e.maxAttempts ?? DEFAULTS.maxAttempts;
  const retry = e.attempts <= max;
  e.state = retry ? "queued" : "failed";
  if (retry) e.queuedAt = e.updatedAt;
  writeLedger(db);
  // T2（2026-11-07）：这次没回到队列（真失败）→ 名额空出，就地提升。
  return { ok: true, retry, attempts: e.attempts, state: e.state, promoted: retry ? [] : promoteLocked() };
}

// 超时看门狗：deadlineAt 过期，或 claimed/running 长时间没有进度信号 → timeout。
// A5（2026-11-08）：以前只认 updatedAt，而 updatedAt 是「台账写入时间」——
//   ① 长任务只要不碰台账，claimed 15 分钟/ running 45 分钟就被当成卡死杀掉；
//   ② 反过来任何无关字段的写入又会把计时归零。
// 现在优先用 e.heartbeatAt（touch() 写的进度心跳），没有心跳才退回 updatedAt/claimedAt；
// 任务可以用 e.stallMs 自报时限（例如预计 3 小时的活），deadlineAt 仍然最硬。
function watchdogLocked(opts = {}) {
  const claimedMs = opts.claimedMs ?? DEFAULTS.claimedMs;
  const runMs = opts.runMs ?? DEFAULTS.runMs;
  const queuedMs = opts.queuedMs ?? DEFAULTS.queuedMs;
  const now = Date.now();
  const db = readLedger();
  const out = [];
  for (const [k, e] of Object.entries(db.tasks)) {
    if (TERMINAL.has(e.state)) continue;
    const ts = Date.parse(e.heartbeatAt || "") || Date.parse(e.updatedAt || e.claimedAt || "") || now;
    const stall = Number(e.stallMs) > 0 ? Number(e.stallMs) : 0;
    let reason = "";
    if (e.deadlineAt && Date.parse(e.deadlineAt) < now) reason = "deadline passed";
    else if (stall && now - ts > stall) reason = `${e.state} 无进度 ${Math.round(stall / 60000)} 分钟（自报时限）`;
    else if (e.state === "queued" && now - ts > queuedMs) reason = "queued too long";
    else if (e.state === "claimed" && now - ts > claimedMs) reason = "claimed but never spawned";
    else if (e.state === "spawned" && now - ts > claimedMs) reason = "spawned but never running";
    else if ((e.state === "running" || e.state === "reported") && now - ts > runMs) reason = "running too long";
    if (!reason) continue;
    out.push({ key: k, from: e.state, reason });
    e.state = "timeout";
    e.timeoutReason = reason;
    e.updatedAt = new Date().toISOString();
    pushEvent(e, "timeout", "watchdog", reason);
  }
  if (out.length) writeLedger(db);
  return out;
}

// 进度心跳：长任务每隔一阵子调一次，watchdog 就以它为计时起点；不动状态。
// 终态任务不接心跳（否则错账，看起来像还活着）。
function touchLocked(key, input = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (TERMINAL.has(e.state)) return { ok: false, error: "terminal task: " + e.state };
  const at = nowIso();
  e.heartbeatAt = at;
  e.updatedAt = at;
  const note = String(input.note || "").slice(0, 200);
  if (note) pushEvent(e, "touch", input.by || "", note);
  // 长任务可以顺手自报「多久没动静算卡死」；看门狗用 e.stallMs 覆盖默认阈值。
  const stallMs = Number(input.stallMs);
  if (Number.isFinite(stallMs) && stallMs > 0) e.stallMs = stallMs;
  writeLedger(db);
  return { ok: true, key, entry: e };
}

const REVIEW_VERDICTS = new Set(["pass", "fail"]);

// 复核记录（口径按小满 #1028 卡）：pass -> done；fail -> blocked 并带 blockReason。
function addReviewLocked(key, { by = "daman", verdict, note = "" } = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (!REVIEW_VERDICTS.has(verdict)) return { ok: false, error: "verdict must be pass|fail" };
  const at = new Date().toISOString();
  const why = String(note || "").slice(0, 300);
  e.reviews = Array.isArray(e.reviews) ? e.reviews : [];
  e.reviews.push({ by, verdict, note: why, at });
  if (verdict === "pass") {
    e.state = "done";
    delete e.blockReason;
  } else {
    e.state = "blocked";
    e.blockReason = why || "rejected in review";
  }
  e.updatedAt = at;
  writeLedger(db);
  return { ok: true, entry: e };
}

// 关房条件：该房所有非 aborted 任务都到 done，且每条有一次 pass 复核
// （或显式 noReview: true 的豁免）。空房间不算可关。
export function roomStatus(roomId, db = readLedger()) {
  const rows = Object.values(db.tasks).filter((e) => e.roomId === roomId && e.state !== "aborted");
  // reviewed 是旧口径留下的等价状态（pass 即完成），按 done 处理。
  const pending = rows.filter((e) => !["done", "reviewed"].includes(e.state)).map((e) => e.taskId);
  const unreviewed = rows
    .filter((e) => {
      if (e.noReview === true) return false;
      const rev = Array.isArray(e.reviews) ? e.reviews : [];
      return !rev.some((r) => r.verdict === "pass");
    })
    .map((e) => e.taskId);
  return {
    roomId,
    tasks: rows.length,
    pending,
    unreviewed,
    canClose: rows.length > 0 && pending.length === 0 && unreviewed.length === 0,
  };
}

// 重启语义：非终态一律 interrupted，不自动重拉。
function markInterruptedLocked() {
  const db = readLedger();
  let n = 0;
  for (const e of Object.values(db.tasks)) {
    if (!TERMINAL.has(e.state)) {
      e.state = "interrupted";
      e.updatedAt = new Date().toISOString();
      n++;
    }
  }
  if (n) writeLedger(db);
  return n;
}

// 桥接注入判据：只送「别人发给大满的功能性任务」，daman 自己发的不回灌。
// assignee 一旦写了就是权威：写明给别人的卡，正文提到「大满」也不算我的（#1132 误投）。
export function isTaskForDaman(m, room) {
  if (!m || m.kind !== "task") return false;
  if (!room || (room.status && room.status !== "open")) return false;
  if (m.who === "daman") return false;
  if (m.assignee) return m.assignee === "daman";
  return /大满|daman/.test(String(m.text || ""));
}

/* ---------- 控制面 v2：无房间任务卡 ---------- */

// 建卡只写 queued，不占并发名额；start 才占名额。
function createTaskLocked(input = {}) {
  const db = readLedger();
  const projectId = String(input.projectId || "local").trim();
  const taskId = String(input.taskId || "").trim();
  if (!ID_RE.test(projectId)) return { ok: false, error: "bad projectId" };
  if (!ID_RE.test(taskId)) return { ok: false, error: "bad taskId" };
  const key = taskKey(projectId, taskId);
  if (db.tasks[key]) return { ok: false, duplicate: true, key, entry: db.tasks[key] };
  const title = String(input.title || taskId).trim().slice(0, 200);
  const gate = input.gate && String(input.gate.command || "").trim()
    ? {
        command: String(input.gate.command).trim(),
        cwd: String(input.gate.cwd || ".").trim(),
        timeoutMs: Math.max(1000, Math.min(3_600_000, Number(input.gate.timeoutMs) || 300_000)),
      }
    : null;
  const entry = {
    roomId: projectId,
    projectId,
    taskId,
    title,
    owner: String(input.owner || "lead").trim(),
    assignee: String(input.assignee || "lead").trim(),
    state: "queued",
    attempts: 0,
    acceptance: cleanList(input.acceptance),
    writeScope: cleanList(input.writeScope),
    gate,
    artifacts: [],
    reviews: [],
    events: [],
    subtasks: [],
    teammates: [],
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
  pushEvent(entry, "created", entry.owner, title);
  db.tasks[key] = entry;
  writeLedger(db);
  return { ok: true, duplicate: false, key, entry };
}

function startTaskLocked(key, input = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (ACTIVE.has(e.state)) return { ok: true, already: true, key, entry: e };
  if (["done", "failed", "aborted"].includes(e.state)) {
    return { ok: false, error: "terminal task: " + e.state, key, entry: e };
  }
  const now = nowIso();
  const owner = String(input.owner || e.owner || "lead").trim();
  const assignee = String(input.assignee || e.assignee || owner).trim();
  if (!fits(e, db).ok) {
    e.state = "queued";
    e.owner = owner;
    e.assignee = assignee;
    e.updatedAt = now;
    e.queuePos = Object.values(db.tasks).filter((x) => x.state === "queued").length + 1;
    pushEvent(e, "queued", owner, "cap full");
    writeLedger(db);
    return { ok: true, queued: true, key, entry: e };
  }
  e.state = "claimed";
  e.owner = owner;
  e.assignee = assignee;
  e.claimedAt = now;
  e.updatedAt = now;
  e.attempts = Number(e.attempts || 0) + 1;
  delete e.queuePos;
  delete e.blockReason;
  delete e.timeoutReason;
  pushEvent(e, "started", owner, assignee);
  writeLedger(db);
  return { ok: true, queued: false, key, entry: e };
}

function reportTaskLocked(key, input = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  // A5（2026-11-08）：被 watchdog 判成 timeout/interrupted 的卡，活可能其实已经干完——
  // 以前这种迟到报告直接拒收。
  const canReport = ["claimed", "running", "reported"].includes(e.state);
  const late = ["timeout", "interrupted"].includes(e.state);
  if (!canReport && !late) {
    return { ok: false, error: "cannot report from state " + e.state };
  }
  const list = Array.isArray(input.artifacts) ? input.artifacts : [];
  const out = [];
  for (const raw of list) {
    const rel = typeof raw === "string" ? raw : raw && raw.path;
    if (!rel) continue;
    const abs = path.resolve(ROOT, String(rel));
    if (!fs.existsSync(abs)) return { ok: false, error: "artifact missing: " + rel };
    const st = fs.statSync(abs);
    if (!st.isFile()) return { ok: false, error: "artifact not a file: " + rel };
    if (st.size > MAX_ARTIFACT_BYTES) return { ok: false, error: "artifact too large: " + rel };
    out.push({
      path: path.relative(ROOT, abs).replace(/\\/g, "/") || String(rel),
      size: st.size,
      sha256: sha256File(abs),
      at: nowIso(),
    });
  }
  e.artifacts = Array.isArray(e.artifacts) ? e.artifacts : [];
  for (const a of out) {
    if (!e.artifacts.some((x) => x.path === a.path && x.sha256 === a.sha256)) e.artifacts.push(a);
  }
  // A5（2026-11-08）：被 watchdog 判了 timeout/interrupted 的卡，活可能其实已经干完。
  // 以前这种报告一律被拒，成果只能散在聊天里。现在允许迟到报告：只落 artifacts/summary 留痕，
  // 状态保持在终态（名额不会因此被二次占用），也不重开任务。
  if (!canReport) {
    e.lateReportAt = nowIso();
    e.lateSummary = String(input.summary || "").slice(0, 1000);
    e.updatedAt = e.lateReportAt;
    pushEvent(e, "late-report", input.by || e.owner, e.lateSummary || `${out.length} artifact(s)`);
    writeLedger(db);
    return { ok: true, key, late: true, state: e.state, entry: e, added: out.length };
  }
  e.state = "reported";
  e.reportedAt = nowIso();
  e.reportSummary = String(input.summary || "").slice(0, 1000);
  e.updatedAt = e.reportedAt;
  pushEvent(e, "reported", input.by || e.owner, e.reportSummary || `${out.length} artifact(s)`);
  writeLedger(db);
  return { ok: true, key, entry: e, added: out.length };
}

function recordGateLocked(key, result = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  e.gate = e.gate && typeof e.gate === "object" ? e.gate : {};
  e.gate.last = {
    pass: result.pass === true,
    exitCode: result.exitCode ?? null,
    signal: result.signal || null,
    durationMs: Number(result.durationMs) || 0,
    stdoutHash: String(result.stdoutHash || ""),
    stderrHash: String(result.stderrHash || ""),
    stdoutTail: String(result.stdoutTail || ""),
    stderrTail: String(result.stderrTail || ""),
    command: String(result.command || ""),
    cwd: String(result.cwd || ""),
    evidencePath: String(result.evidencePath || ""),
    at: result.at || nowIso(),
  };
  e.updatedAt = nowIso();
  pushEvent(e, "gate", result.by || e.owner, e.gate.last.pass ? "pass" : `fail (exit ${e.gate.last.exitCode})`);
  writeLedger(db);
  return { ok: true, key, entry: e };
}

function acceptTaskLocked(key, input = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (e.state === "done") return { ok: true, already: true, key, entry: e };
  if (e.state !== "reported") return { ok: false, error: "cannot accept from state " + e.state };
  const note = String(input.note || "").slice(0, 500);
  const force = input.force === true;
  if (!(e.gate && e.gate.last && e.gate.last.pass === true) && !force) {
    return { ok: false, error: "gate not passed; run gate or use --force with --note" };
  }
  if (force && !note) return { ok: false, error: "--force requires --note" };
  const at = nowIso();
  e.state = "done";
  e.acceptedAt = at;
  e.acceptedBy = String(input.by || "you");
  delete e.blockReason;
  e.reviews = Array.isArray(e.reviews) ? e.reviews : [];
  e.reviews.push({ by: String(input.by || "you"), verdict: "pass", note, at });
  e.updatedAt = at;
  pushEvent(e, "accepted", e.acceptedBy, force ? "forced: " + note : note);
  writeLedger(db);
  // T2（2026-11-07）：名额刚空出来，立刻把排队的提上来，不等下一次 15 秒维护窗口。
  return { ok: true, key, entry: e, promoted: promoteLocked() };
}

function rejectTaskLocked(key, input = {}) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e) return { ok: false, error: "unknown key " + key };
  if (!["reported", "blocked"].includes(e.state)) return { ok: false, error: "cannot reject from state " + e.state };
  const note = String(input.note || "").slice(0, 500);
  if (!note) return { ok: false, error: "reject requires --note" };
  const at = nowIso();
  e.state = "blocked";
  e.blockReason = note;
  e.reviews = Array.isArray(e.reviews) ? e.reviews : [];
  e.reviews.push({ by: String(input.by || "you"), verdict: "fail", note, at });
  e.updatedAt = at;
  pushEvent(e, "rejected", input.by || "you", note);
  writeLedger(db);
  // T2（2026-11-07）：blocked 不占名额，同样就地提升。
  return { ok: true, key, entry: e, promoted: promoteLocked() };
}

// T2 配套（2026-11-07）：已 claimed 但还没人去 spawn 的任务，交给桥去叫醒 agent。
// 提醒后打 nudgedAt，5 分钟内不重复（15 秒 tick 反复注入会烧预算）。
// A2（2026-11-08）：提醒没送出去（agent 正忙，桥 return false）时用 clearNudge 立刻还账；
// 否则要干等 5 分钟 TTL，claimed 卡到 15 分钟看门狗上就变 timeout 死单。
// A12（2026-11-08）：接管过的卡只归接手那一端（handoff.to）；传 assignee 时不提醒别人。
const NUDGE_TTL_MS = 300_000;

function claimedAwaitingSpawnLocked(now = Date.now(), { assignee = "" } = {}) {
  const db = readLedger();
  const out = [];
  for (const [k, e] of Object.entries(db.tasks)) {
    if (e.state !== "claimed") continue;
    if (Array.isArray(e.subtasks) && e.subtasks.length) continue;
    if (assignee && e.handoff && e.handoff.to && e.handoff.to !== assignee) continue;
    const last = Date.parse(e.nudgedAt || "") || 0;
    if (last && now - last < NUDGE_TTL_MS) continue;
    out.push(k);
  }
  out.sort();
  for (const k of out) db.tasks[k].nudgedAt = new Date(now).toISOString();
  if (out.length) writeLedger(db);
  return out;
}

// 撤掉一次没送到的提醒；不动 updatedAt（看门狗计时不受影响）。
function clearNudgeLocked(key) {
  const db = readLedger();
  const e = db.tasks[key];
  if (!e || !e.nudgedAt) return { ok: false };
  delete e.nudgedAt;
  writeLedger(db);
  return { ok: true, key };
}

export function listTasks({ projectId = "", all = true } = {}) {
  const db = readLedger();
  return Object.entries(db.tasks)
    .filter(([, e]) => !projectId || e.projectId === projectId || e.roomId === projectId)
    .filter(([, e]) => all || !TERMINAL.has(e.state))
    .map(([key, e]) => ({ key, ...e }))
    .sort((a, b) => String(b.updatedAt || "").localeCompare(String(a.updatedAt || "")));
}

export function controlCounts(db = readLedger()) {
  const tasks = Object.values(db.tasks);
  const byState = {};
  const byProject = {};
  let active = 0;
  for (const e of tasks) {
    byState[e.state] = (byState[e.state] || 0) + 1;
    const pid = e.projectId || e.roomId || "local";
    byProject[pid] = (byProject[pid] || 0) + 1;
    if (ACTIVE.has(e.state)) active++;
  }
  return { all: tasks.length, active, byState, byProject };
}

// 对外接口：所有写操作走同一把跨进程锁（桌面端与 QQ 端会话共写台账）。
export function claim(roomId, taskId, seq, extra = {}) { return withLedgerLock(() => claimLocked(roomId, taskId, seq, extra)); }
export function setState(key, state, patch = {}) { return withLedgerLock(() => setStateLocked(key, state, patch)); }
export function promote() { return withLedgerLock(() => promoteLocked()); }
export function claimedAwaitingSpawn(opts = {}) {
  const o = typeof opts === "string" ? { assignee: opts } : opts || {};
  return withLedgerLock(() => claimedAwaitingSpawnLocked(Date.now(), o));
}
export function clearNudge(key) { return withLedgerLock(() => clearNudgeLocked(key)); }
export function touch(key, input = {}) { return withLedgerLock(() => touchLocked(key, input)); }
export function fail(key, reason = '') { return withLedgerLock(() => failLocked(key, reason)); }
export function watchdog(opts = {}) { return withLedgerLock(() => watchdogLocked(opts)); }
export function addReview(key, opts = {}) { return withLedgerLock(() => addReviewLocked(key, opts)); }
export function markInterrupted() { return withLedgerLock(() => markInterruptedLocked()); }

export function createTask(input = {}) { return withLedgerLock(() => createTaskLocked(input)); }
export function startTask(key, input = {}) { return withLedgerLock(() => startTaskLocked(key, input)); }
export function reportTask(key, input = {}) { return withLedgerLock(() => reportTaskLocked(key, input)); }
export function recordGate(key, result = {}) { return withLedgerLock(() => recordGateLocked(key, result)); }
export function acceptTask(key, input = {}) { return withLedgerLock(() => acceptTaskLocked(key, input)); }
export function rejectTask(key, input = {}) { return withLedgerLock(() => rejectTaskLocked(key, input)); }
export function getTask(key) { return readLedger().tasks[key] || null; }

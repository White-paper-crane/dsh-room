// ds-teams 桥接（小满端 v3）：chat-first-v3 协议。把房间里需要小满处理的消息送进小满的回合。
// v3 协议变化（相对 v2，契约 docs/chat-first-v3.md 第 3 节）：
//   ① 每 tick 先 POST /api/bridge/heartbeat {who,key}；
//   ② GET /api/state?since=cursor&who=本端 不再推进游标；游标只由 POST /api/state/ack {who,key,seq} 推进（累积到 seq 含）。
//      注入成功或判定跳过（not-relevant/claimed-peer/flood 等）即 ack；hold（busy/cooldown）不 ack，下 tick 重试；
//   ③ 丢弃策略：只对 kind=say（服务端实际 kind=text）做同房 flood 上限（>5 留最新）；
//      kind in {task,progress,report,review,file,system} 永不丢弃、永不 flood、永不 backlog-skip；
//   ④ kind=ack 一律不注入，只写日志 action=ack-log 并 ack；
//   ⑤ claim 两端对称（CLIAM_ENABLED=true）：未点名/双点名 say 都可 claim（token 小者赢），
//      输方 skip(reason=claimed-peer) 并 ack；赢方也写 claim 事件（v3 补齐 me 端旧的不对称——旧版赢时不写）；
//      T4（2026-11-07）：claim 范围扩到 file / task / system（不是自己、也不是对端 agent 发的都算），
//      这类卡片一律视为歧义直接声明——原来它们完全不过 claim，两端各注入一次（双投）；
//   ⑥ 同一条消息被 hold 连续超 60s：另一端可 claim 接管注入（每 15s 扫一次，claim 范围扩到被 hold 超时的 say）；
//   ⑦ 心跳/state 连续 3 tick 失败 → 写 error 日志并指数退避 2s→4s→8s…上限 30s，恢复后回 2s；
//   ⑧ 保留 busy 看门狗（15s 复位、不取消 in-flight）、cooldown；CLI 侧差异：小满端只给附件提示不下载文件。
//   ⑨ BUG#2 自愈（2026-11-05 房 room-912504 事故）：phase 非 idle 连续 ≥180s 且期间被 hold 过、
//      日志无新进展、房内也没人刚说话 → 判定卡死，agent.cancel({kind:"user"}) 强停那轮，evt action=stuck-reset。
//      看门狗只管桥自己的 busy 变量，管不了宿主 agent 的 phase；这条才是「永不 hold 死」的出路。
// v3 去掉 me 端旧逻辑：backlog-skip（契约：卡片永不 backlog-skip）；旧 flood 无卡片豁免（契约：卡片永不 flood）。
// 生存纪律（v2 延续）：定时器回调过 run() 吞错；取 ctx 服务走 agents() 兜底；定时器挂 ctx.timeout/ctx.interval
// + 世代锁 + supercede 自熄，防热加载僵尸。
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

// 运行时配置（公开版）：默认按「插件与服务器同仓」解析，可用环境变量覆盖。
//   DS_TEAMS_ROOT          服务器仓库根目录（含 server.mjs）
//   DS_TEAMS_SESSIONS_DIR  宿主会话目录（DSH 的 sessions/）
//   DS_TEAMS_WORKSPACE     宿主工作区目录（新会话的 cwd）
//   DS_TEAMS_DAMAN_SESSION 绑定到固定会话（留空则按房间台账新建）
//   DS_TEAMS_ME_ID         QQ 端宿主身份 id
const ROOT = process.env.DS_TEAMS_ROOT || path.resolve(process.env.DSH_WORKSPACE || process.cwd(), "teams");
const SESS_ROOT = process.env.DS_TEAMS_SESSIONS_DIR || path.join(os.homedir(), ".dsh", "sessions");
const SESS_CWD = process.env.DS_TEAMS_WORKSPACE || path.resolve(ROOT, "..");
import { createUserMessage } from "@deepseek-ai/dsh-llm";
// 动态 import + 时间戳：宿主会缓存同 URL 模块，改了导出不重启就拿到旧的（2026-11-03 事故）。
const { writeEnd, markEndArchived } = await import(pathToFileURL(path.join(ROOT, "room-session-lib.mjs")).href + "?v=" + Date.now());
// BUG#2 自愈（2026-11-05）：会话 phase 长期 running 不收敛 → 桥永 hold，只有被对端 60s takeover 一条出路。
// 判定放共享纯函数库（两端同一条判据），强停走宿主 agent.cancel({kind:"user"})。
const { createHoldBook, noteHold, clearHold, settleHold, phaseKind, isBusyKind, lastSeqTimeFromLog, decideStuck, STUCK_MS } = await import(pathToFileURL(path.join(ROOT, "stuck-guard-lib.mjs")).href + "?v=" + Date.now());
const { createHoldClock } = await import(pathToFileURL(path.join(ROOT, "hold-lib.mjs")).href + "?v=" + Date.now());
// T5（2026-11-07）：闸门拒绝后的房内说明文案，放共享纯函数库，两端同一份、同一幂等键。
const { noticeFor } = await import(pathToFileURL(path.join(ROOT, "wake-notice-lib.mjs")).href + "?v=" + Date.now());
// T4（2026-11-07）：claim 适用范围——file / task / system 也要 claim，否则两端各注入一次（双投）。
const { shouldClaim } = await import(pathToFileURL(path.join(ROOT, "claim-scope-lib.mjs")).href + "?v=" + Date.now());

export const name = "ds-teams-bridge-me";
export const inject = ["agents", "timer", "workspaceController", "workspaceRegistry"];

const PORT_FILE = ROOT + "\\data\\port.txt";
const KEY_FILE = ROOT + "\\data\\key.txt";
const CURSOR_FILE = ROOT + "\\data\\bridge-me-cursor.json";
// 事件日志（D3）：注入/跳过/hold/claim/心跳/退避各落一行 JSON，回归直接 grep。
const EVENT_LOG = ROOT + "\\data\\bridge-me.log";
const ME = process.env.DS_TEAMS_ME_ID || "onebot-private-0000000000";
const DAMAN_SESS = "<session-id>";
const COOLDOWN_MS = 3000;
// P2 单房灰度（2026-11-04）：房 → 专属会话台账（与大满端同一个文件，按 ends.me 取值）。
// 缺失 / enabled:false / 无此房 → 全部退回共用 QQ 会话（旧行为，零差异）。
const ROOM_SESSIONS_FILE = ROOT + "\\data\\room-sessions.json";
// 供应商与模型由代码决定（会议室 v1.1：两端统一 tokenrocks / deepseek-v4.1-flash，
// 不再用官网 deepseek-account；大满端对称文件同步改）。异构评审如需恢复，改本文件
// 或大满端一行为 tokenrocks 的 kimi-k3.1-pro / glm-5.3 即可。
const FALLBACK_PROVIDER = "tokenrocks";
const FALLBACK_MODEL = "deepseek-v4.1-flash";

// ---- v3 端侧配置（参数化；大满端在 ds-teams-bridge.mjs 里换成对称值）----
const WHO = "me";                 // 本端身份（people.json 的键）
const PEER = "daman";             // 对端身份
const MY_NAME = "小满";
const PEER_NAME = "大满";
const CLAIM_ENABLED = true;       // v3：两端对称，都参与 claim（契约 §3.5）
const CLAIM_WAIT_MS = 2000;
const FLOOD_LIMIT = 5;            // 同房 say >5 只留最新（契约 §3.3）
const TAKEOVER_HOLD_MS = 60_000;  // hold 超时阈值（契约 §2.5：连续 hold >60s）
const TAKEOVER_MAX_AGE_MS = 600_000; // 接管只扫 10 分钟内的消息，冷启动不翻旧账
const MY_RE = /小满|\bme\b/i;     // 本端名字（点名判定）
const PEER_RE = /大满|daman/i;    // 对端名字
// 注入白名单（契约 §2.4）：confirm/patrol/未知 kind 一律不注入。
const INJECTABLE = new Set(["text", "say", "task", "progress", "report", "review", "file", "system"]);
// 契约里的 kind=say = 服务端实际落库的用户发言 kind=text；两个都认，兼容服务端改名。
const isSayKind = (m) => !!m && (m.kind === "text" || m.kind === "say");

// ---- wake 治理（契约 docs/wake-governance.md §5/§6）----
// 每次注入前 POST /api/wake/admit 领 wakeId（预算闸），注入后由 outcome watcher 回填 settle。
const WAKE_OUTCOME_WINDOW_MS = 90_000;   // 注入后观察窗口：90s 内本端无房内输出 → NO_REPLY
const WAKE_WATCH_INTERVAL_MS = 2_000;    // outcome watcher 拉状态间隔

const read = (p) => { try { return fs.readFileSync(p, "utf8").trim(); } catch { return ""; } };

const GEN_KEY = Symbol.for("ds-teams-bridge-me.gen");

export function apply(ctx) {
  // 第几代实例（同一进程内递增）。热加载会新建实例但旧实例的定时器不会自动停，
  // 用全局代号让旧实例的所有回调自动失效，避免重复轮询/重复投递。
  const gen = (globalThis[GEN_KEY] = (globalThis[GEN_KEY] ?? 0) + 1);
  const live = () => !disposed && globalThis[GEN_KEY] === gen;
  const log = (m) => { try { ctx.logger?.info?.("[ds-teams-bridge-me] g" + gen + " " + m); } catch {} };
  // 事件日志：{at, side, action, seq, room, kind, reason, ...}；失败静默，绝不因写日志炸桥。
  const evt = (o) => { try { fs.appendFileSync(EVENT_LOG, JSON.stringify({ at: new Date().toISOString(), side: "me", gen, ...o }) + "\n", "utf8"); } catch {} };
  // 事件去抖（D3）：同一键 30 秒内只写一条。hold 每 2 秒重试、flood 每 tick 重算，不去抖会刷屏。
  const evtSeen = new Map();
  const evtOnce = (key, o, ms = 30_000) => {
    const now = Date.now();
    if (now - (evtSeen.get(key) || 0) < ms) return;
    evtSeen.set(key, now);
    if (evtSeen.size > 512) for (const [k, t] of evtSeen) if (now - t > ms) evtSeen.delete(k);
    evt(o);
  };
  const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
  const timers = new Set();
  let disposed = false;
  let cursor = null;
  // C5（2026-11-04 用户「做到完全互不影响」）：按房游标。
  // 以前单个全局游标 + hold 时 break：某房 hold 会挡住同一 tick 里排在它后面的所有房。
  // 现在每房各存自己的 seq，hold 只冻结该房（heldRooms），其他房继续；pullState 的
  // since 取所有房的最小值（多取几条无害，逐房 seq 过滤会挡掉重复）。
  const roomCursors = new Map();
  let heldRooms = new Set();
  const cursorOf = (roomId) => {
    const v = roomCursors.get(roomId);
    return typeof v === "number" && Number.isFinite(v) ? v : (cursor ?? 0);
  };
  const cursorFloor = () => {
    let m = null;
    for (const v of roomCursors.values()) if (Number.isFinite(v) && (m === null || v < m)) m = v;
    return m === null ? cursor : m;
  };
  let busy = false;
  let busySince = 0;
  let lastInject = 0;
  // C5：cooldown 按房计（以前是全局 lastInject，一间房刚注入完，别的房要跟着等 3 秒）。
  const lastInjectByRoom = new Map();
  // C6（BUG#2 2026-11-05）：按会话记「被 hold 在 busy」的起止，供卡死自愈判定；+ 房内最后一条对端消息时间。
  const holdBook = createHoldBook();
  // A11（2026-11-08）：同一条消息被 hold 多久了；超过上限就放弃这一轮注入并 ack。
  const holdClock = createHoldClock();
  const roomLastPeerAt = new Map();
  const lastRoomPeerAt = (roomId) => roomLastPeerAt.get(String(roomId)) || 0;
  let lastArch = 0;
  let lastTakeoverScan = 0;
  // 退避状态（契约 §3.6）：连续 3 tick 失败 → 2→4→8…上限 30s；恢复回 2s。
  let failStreak = 0;
  let pollDelay = 2000;
  let nextAllowedAt = 0;
  // C5：ack 补发按房记（以前是单值，一间房 ack 失败会被另一间房的成功冲掉）。key = roomId 或 "-"。
  const pendingAcks = new Map();
  const PA = (roomId) => (roomId ? String(roomId) : "-");

  const svc = (name) => { try { return ctx.get ? ctx.get(name) ?? void 0 : void 0; } catch { return void 0; } };
  const agents = () => {
    if (!live()) return void 0;
    const viaGet = svc("agents");
    if (viaGet) return viaGet;
    try { return ctx.agents ?? void 0; } catch { return void 0; }
  };
  const run = (label, fn) => () => {
    if (!live()) return;
    try {
      const p = fn();
      if (p && typeof p.then === "function") p.catch((e) => log(label + " failed " + String((e && e.message) ?? e)));
    } catch (e) { log(label + " failed " + String((e && e.message) ?? e)); }
  };
  // 定时器必须挂在 fiber 上（同大满端：本版 cordis 没有用户可见的 dispose 事件，
  // 裸 setTimeout 在重载后成僵尸，ctx 已 inactive 却还每 2 秒轮询）。
  const effectTimers = !!(ctx.timeout && ctx.interval);
  const later = (fn, ms) => { const t = effectTimers ? ctx.timeout(fn, ms) : setTimeout(fn, ms); timers.add(t); return t; };
  const every = (fn, ms) => { const t = effectTimers ? ctx.interval(fn, ms) : setInterval(fn, ms); timers.add(t); return t; };

  const saveCursor = (seq) => { try { fs.writeFileSync(CURSOR_FILE, JSON.stringify({ seq, at: Date.now(), rooms: Object.fromEntries(roomCursors) }), "utf8"); } catch {} };

  // ---- 服务端访问（契约 §3.1/§2.1/§2.5，与大满端同构）----
  const base = () => "http://127.0.0.1:" + (read(PORT_FILE) || "8792");
  const platKey = () => read(KEY_FILE);
  const memberKey = (who) => {
    for (const f of ["people.json", "members.json"]) {
      try { const j = JSON.parse(read(ROOT + "\\data\\" + f)); if (j && j[who] && j[who].key) return j[who].key; } catch {}
    }
    return "";
  };
  const heartbeat = async () => {
    try {
      const r = await fetch(base() + "/api/bridge/heartbeat?k=" + encodeURIComponent(platKey()), {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO) }),
        signal: AbortSignal.timeout(3000),
      });
      return { ok: r.ok, status: r.status };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  // C4（2026-11-04）：可选 roomId —— 带了就按房推进（A 房 hold 不再堵住 B 房），不带=旧全局语义。
  const ackPost = async (seq, roomId) => {
    try {
      const r = await fetch(base() + "/api/state/ack?k=" + encodeURIComponent(platKey()), {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), seq: Number(seq), ...(roomId ? { roomId: String(roomId) } : {}) }),
        signal: AbortSignal.timeout(3000),
      });
      return { ok: r.ok, status: r.status };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  const pullState = async (since) => {
    try {
      const r = await fetch(`${base()}/api/state?since=${Number(since) || 0}&who=${WHO}&k=${encodeURIComponent(platKey())}`, { signal: AbortSignal.timeout(3000) });
      if (!r.ok) return { ok: false, status: r.status };
      return { ok: true, s: await r.json() };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  const claimPost = async (roomId, targetSeq) => {
    try {
      const r = await fetch(`${base()}/api/rooms/${encodeURIComponent(roomId)}/claim?k=${encodeURIComponent(platKey())}`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), targetSeq }), signal: AbortSignal.timeout(3000),
      });
      if (!r.ok) return { ok: false, status: r.status };
      return await r.json().catch(() => ({ ok: false }));
    } catch { return { ok: false }; }
  };
  const claimGet = async (roomId, targetSeq) => {
    try {
      const r = await fetch(`${base()}/api/rooms/${encodeURIComponent(roomId)}/claim?targetSeq=${targetSeq}&k=${encodeURIComponent(platKey())}`, { signal: AbortSignal.timeout(2500) });
      if (!r.ok) return null;
      return await r.json().catch(() => null);
    } catch { return null; }
  };
  // 契约 docs/wake-governance.md §4.1：注入闸门 POST /api/wake/admit?k=<platKey> body {who,key,seq,roomId,kind}
  // → {ok,allow,wakeId?,reason?,budget,highContextAge}。网络失败/超时返回 {ok:false,error}，绝不在无闸状态下注入。
  const wakeAdmit = async (seq, roomId, kind) => {
    try {
      const r = await fetch(base() + "/api/wake/admit?k=" + encodeURIComponent(platKey()), {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), seq: Number(seq), roomId: String(roomId || ""), kind: String(kind || "") }),
        signal: AbortSignal.timeout(3000),
      });
      if (!r.ok) return { ok: false, status: r.status };
      return { ok: true, adm: await r.json().catch(() => null) };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  // 契约 docs/wake-governance.md §4.2：回填 POST /api/wake/settle body {who,key,wakeId,outcome}
  // outcome ∈ REPLIED | NO_REPLY | MONOLOGUE | ERROR。
  const wakeSettle = async (wakeId, outcome) => {
    try {
      const r = await fetch(base() + "/api/wake/settle?k=" + encodeURIComponent(platKey()), {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), wakeId: String(wakeId), outcome: String(outcome) }),
        signal: AbortSignal.timeout(3000),
      });
      return { ok: r.ok, status: r.status };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  // T5（2026-11-07）：闸门拒了不能静默。原实现允许「预算/上下文超限」直接当已处理（ack + 0 注入），
  // 房里看不到任何解释，群主只能猜为什么没人回。这里补一条说明，按「房 + 原因 + 日期」幂等：
  // 服务端 /say 只认 text 种别（body.kind 会被忽略），所以用 text + 【自动提示】前缀；
  // msgId 走服务端幂等（同一 msgId 每房只落一条），重启后也不会重复。
  const postNotice = async (roomId, reason, seq) => {
    const { text, msgId } = noticeFor(reason, seq);
    try {
      const r = await fetch(`${base()}/api/rooms/${encodeURIComponent(roomId)}/say?k=${encodeURIComponent(platKey())}`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), text, msgId }),
        signal: AbortSignal.timeout(3000),
      });
      if (!r.ok) { log(`wake blocked notice failed status=${r.status}`); return false; }
      const j = await r.json().catch(() => null);
      evt({ action: "wake-blocked-notice", room: roomId, reason, seq, msgSeq: (j && j.message && j.message.seq) || null });
      return true;
    } catch (e) {
      log("wake blocked notice error " + String((e && e.message) ?? e).slice(0, 120));
      return false;
    }
  };
  // A11（2026-11-08）：hold 超时后要在房里说一句「先放下了」。这里没有 postSay，
  // 直接用 /say 落一条 text（和自己回话同一个身份）。
  const sayText = async (roomId, text) => {
    try {
      const r = await fetch(`${base()}/api/rooms/${encodeURIComponent(roomId)}/say?k=${encodeURIComponent(platKey())}`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: WHO, key: memberKey(WHO), text }),
        signal: AbortSignal.timeout(3000),
      });
      if (!r.ok) { log(`hold give-up notice failed status=${r.status}`); return false; }
      return true;
    } catch (e) {
      log("hold give-up notice error " + String((e && e.message) ?? e).slice(0, 120));
      return false;
    }
  };
  const serverAckCursor = (s, who) => {    try {
      const obj = s && (s.acks || s.cursors || s.ackSeqs);
      if (obj && typeof obj === "object" && !Array.isArray(obj) && typeof obj[who] === "number" && Number.isFinite(obj[who])) return obj[who];
      if (s && typeof s.ackSeq === "number" && Number.isFinite(s.ackSeq)) return s.ackSeq;
      if (s && typeof s.ack === "number" && Number.isFinite(s.ack)) return s.ack;
    } catch {}
    return null;
  };
  const serverAcks = (s) => { try { const o = s && (s.acks || s.cursors || s.ackSeqs); return o && typeof o === "object" && !Array.isArray(o) ? o : null; } catch { return null; } };

  // ---- 会话/agent 生命周期（v2 原样，仅会话发现不同：onebot 前缀 mtime 扫描）----
  // 小满会话 id 会换（/new、preset 轮换都会新建目录），写死会投进已停用的旧会话。
  // 按 session.v4.jsonl.zstd 的 mtime 取最新的 onebot-private-0000000000- 目录，缺了回退 ME 常量并记日志。
  // C3（2026-11-04）：mtime 回退再加一道闸——房会话/大满主会话永远不当"我的主会话"。
  // 原本只靠 onebot-private- 前缀过滤；若哪天有人把房会话命名成 onebot- 前缀，或群里出现第二个
  // onebot 目录，mtime 最新就可能粘错。这里显式排除台账里所有房会话 + DAMAN。
  const meSessionId = () => {
    try {
      const reserved = new Set(roomSessions().byRoom.values());
      if (DAMAN_SESS) reserved.add(DAMAN_SESS);
      const entries = fs.readdirSync(SESS_ROOT, { withFileTypes: true })
        .filter((d) => d.isDirectory() && d.name.startsWith("onebot-private-0000000000-") && !reserved.has(d.name));
      if (!entries.length) { log("session fallback: no onebot dir, use " + ME); return ME; }
      const st = (d) => { try { return fs.statSync(SESS_ROOT + "\\" + d + "\\session.v4.jsonl.zstd").mtimeMs; } catch { try { return fs.statSync(SESS_ROOT + "\\" + d).mtimeMs; } catch { return -1; } } };
      let best = entries[0].name, bestT = -1;
      for (const d of entries) { const t = st(d.name); if (t > bestT) { bestT = t; best = d.name; } }
      return best;
    } catch (e) { log("session scan failed: " + String((e && e.message) ?? e)); return ME; }
  };

  // ---- 单房灰度：roomId → 专属会话（ROOM_SESSIONS_FILE，按 ends.<WHO> 取值）----
  // 5 秒缓存：改 room-sessions.json 后 ≤5s 生效——这就是回滚通道。
  // B2（2026-11-04）：bySid 缓存同一批叶子的原样对象，供 optsFor(sessionId) 查 provider/model 覆盖。
  let rsCache = { at: 0, byRoom: new Map(), bySid: new Map() };
  const roomSessions = () => {
    if (Date.now() - rsCache.at < 5000) return rsCache;
    let j = null;
    // 台账是人手改的：记事本/PowerShell Set-Content 会带 UTF-8 BOM，JSON.parse 会直接抛
    // "Unexpected token" 然后静默回落成"没配过房会话"——灰度看起来生效其实没生效。剥掉再解。
    try { j = JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); }
    catch (e) { log("room-sessions parse failed: " + String((e && e.message) ?? e).slice(0, 120)); j = null; }
    const byRoom = new Map();
    const bySid = new Map();
    if (j && j.enabled !== false && j.rooms && typeof j.rooms === "object") {
      for (const [rid, r] of Object.entries(j.rooms)) {
        const leaf = r && r.ends && r.ends[WHO];
        // B1：叶子 status=archived（房已归档）→ 不进 byRoom（sessionFor 回落主会话、archGuard 不保活）。
        const sid = leaf && leaf.sessionId && leaf.status !== "archived" ? String(leaf.sessionId) : "";
        if (sid) { byRoom.set(String(rid), String(sid)); bySid.set(String(sid), leaf); }
      }
    }
    rsCache = { at: Date.now(), byRoom, bySid };
    if (byRoom.size) log("room-sessions: " + [...byRoom].map(([r, s]) => r + "->" + s).join(", "));
    return rsCache;
  };
  // 唯一入口：房有专属会话就用它，否则回落 QQ 主会话（= 未启用灰度时的现有行为）。
  const sessionFor = (roomId) => (roomId && roomSessions().byRoom.get(roomId)) || meSessionId();

  // ---- B2（2026-11-04）：按角色分模型 ----
  // 只认非空字符串；空串/非字符串/缺字段一律回落 FALLBACK_*（台账人手改，脏值不能让 resume 抛）。
  const strOpt = (v) => (typeof v === "string" && v.trim() ? v.trim() : "");
  // 按「已解析的 sessionId」查模型：房专属会话命中台账叶子 → 用叶子的 provider/model；
  // 主会话/未配覆盖 → FALLBACK_*（零差异）。口径与 sessionFor 一致，都走同一份 5s 缓存。
  const optsFor = (sessionId) => {
    const leaf = sessionId ? (roomSessions().bySid.get(String(sessionId)) || null) : null;
    return {
      provider: strOpt(leaf && leaf.provider) || FALLBACK_PROVIDER,
      model: strOpt(leaf && leaf.model) || FALLBACK_MODEL,
    };
  };

  // ---- B1（2026-11-04）：自动为 open 房建本端专属会话 + 归档感知 ----
  // 规则（用户 11-04 定）：关房（status=closed）= 只停注入，不归档会话；归档房（从 rooms 消失）= archiveSession。
  // 自动建：15s 节流扫 open 房，缺本端叶子就 create + followup 房简报（create 后必须发一条才落盘，
  // 否则 resume 时 persistence 抛 SessionPersistenceNotFoundError）。幂等：byRoom 命中 / in-flight / create 失败退避。
  // 台账 enabled:false = 总闸（回滚通道），create 直接跳过。
  let autoCreateOn = true;   // 每次扫描现读台账 enabled（改文件 ≤15s 生效，不用重挂载）
  let lastAutoCreate = 0;
  const creatingRoom = new Set();   // in-flight 房
  const failedRoom = new Map();     // roomId → 下次允许重试时刻（退避 60s 起，翻倍上限 15min）
  function roomSessionsEnabled() { try { const j = JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); return j && j.enabled !== false; } catch { return true; } }

  // 房简报（seed 方案 B）：逐字节固定、无时间戳/序号（设计稿 §3.1：缓存友好）。
  const roomBrief = (roomId, title) => {
    return `[ds-teams 房间专属会话] 房间 ${roomId}${title ? " · " + title : ""}。\n` +
      `你是${MY_NAME}（${WHO}），这是你在该房的专属会话，只处理该房消息。回复用：node "<repo>\\say.mjs" say ${roomId} "..." --who ${WHO}。\n` +
      `成员：you（群主，人）、me 小满（QQ）、daman 大满（桌面）。协议与纪律见 docs/chat-first-v3.md。`;
  };

  const ensureRoomSession = async (room) => {
    const rid = String(room.id || "");
    if (!rid || room.status !== "open") return;
    if (!autoCreateOn) return;
    if (roomSessions().byRoom.has(rid)) return;
    if (creatingRoom.has(rid)) return;
    const until = failedRoom.get(rid) || 0;
    if (Date.now() < until) return;
    creatingRoom.add(rid);
    const g = gen;
    try {
      const a = agents();
      if (!a) { log("autoCreate: agents missing"); return; }
      const sessionId = "session-" + randomUUID();
      // B2：新会话按台账叶子的 provider/model 起（可先写模型再让桥建会话）；没写就是 FALLBACK_*。
      // 这里直接读原始叶子而不是 endOpts——房还没有 sessionId 时 endOpts 按设计返回 null，但模型要生效。
      const leaf0 = readLedgerRaw2()?.rooms?.[rid]?.ends?.[WHO] || null;
      const agentOptions = {
        provider: strOpt(leaf0 && leaf0.provider) || FALLBACK_PROVIDER,
        model: strOpt(leaf0 && leaf0.model) || FALLBACK_MODEL,
      };
      log(`autoCreate: room=${rid} -> ${sessionId} provider=${agentOptions.provider} model=${agentOptions.model}`);
      const handle = await a.create({ sessionId, agentOptions, meta: { cwd: SESS_CWD, agentPreset: "standard" }, setup: presetSetup });
      const ag = handle?.agent ?? handle;
      if (!ag || typeof ag.followup !== "function") throw new Error("create returned no agent");
      await ag.followup(createUserMessage({
        content: [{ type: "text", text: roomBrief(rid, room.title) }],
        source: { kind: "plugin:ds-teams" },
      }));
      if (!live() || g !== gen) return;   // 热加载竞争中退出：不写台账，防旧代污染
      writeEnd(ROOM_SESSIONS_FILE, rid, WHO, { sessionId, createdAt: new Date().toISOString(), status: "active", provider: agentOptions.provider, model: agentOptions.model });
      rsCache.at = 0;   // 立刻失效缓存，本 tick 后续注入即用新会话
      evt({ action: "room-session-created", room: rid, sessionId, who: WHO, provider: agentOptions.provider, model: agentOptions.model });
      log(`autoCreate: ok room=${rid} ${sessionId} model=${agentOptions.model}`);
      failedRoom.delete(rid);
    } catch (e) {
      failedRoom.set(rid, Date.now() + Math.min(900_000, 60_000 * Math.pow(2, (failedRoom.get(rid) ? 1 : 0))));
      evtOnce("autoCreate-fail:" + rid, { action: "room-session-create-fail", room: rid, who: WHO, message: String((e && e.message) ?? e).slice(0, 200) });
      log("autoCreate failed room=" + rid + " " + String((e && e.message) ?? e));
    } finally {
      creatingRoom.delete(rid);
    }
  };

  const ensureRoomSessions = async (rooms) => {
    if (Date.now() - lastAutoCreate < 15_000) return;
    lastAutoCreate = Date.now();
    autoCreateOn = roomSessionsEnabled();   // 总闸现读：enabled:false → ≤15s 停建（回滚通道）
    for (const r of (rooms || [])) {
      if (disposed || !live()) break;
      if (r.status !== "open") continue;
      await ensureRoomSession(r);
    }
  };

  // 归档感知：房从 rooms 消失（被 archiveMove 搬走）→ 本端叶子若有会话则 archiveSession + 台账标 archived。
  // 关房（closed）不动——已由 relevant() 的 status!=="open" 停注入，会话保留可回看。
  let knownRoomIds = null;   // null=未初始化（首轮只记基线）
  const archiveGoneRooms = async (rooms) => {
    const nowIds = new Set((rooms || []).map((r) => String(r.id)));
    if (knownRoomIds === null) { knownRoomIds = nowIds; return; }
    const gone = [...knownRoomIds].filter((id) => !nowIds.has(id));
    knownRoomIds = nowIds;
    if (!gone.length) return;
    const ledger = readLedgerRaw2();
    for (const rid of gone) {
      const leaf = ledger && ledger.rooms && ledger.rooms[rid] && ledger.rooms[rid].ends && ledger.rooms[rid].ends[WHO];
      if (!leaf || !leaf.sessionId || leaf.status === "archived") continue;
      try {
        const ctl = svc("workspaceController");
        if (ctl) await ctl.archiveSession({ sessionId: leaf.sessionId, stopActivity: true });
        markEndArchived(ROOM_SESSIONS_FILE, rid, WHO);
        rsCache.at = 0;
        evt({ action: "room-session-archived", room: rid, sessionId: leaf.sessionId, who: WHO });
        log(`room gone -> archived session ${leaf.sessionId} (room=${rid})`);
      } catch (e) {
        evtOnce("room-archive-fail:" + rid, { action: "room-session-archive-fail", room: rid, who: WHO, message: String((e && e.message) ?? e).slice(0, 200) });
      }
    }
  };
  const readLedgerRaw2 = () => { try { return JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); } catch { return null; } };
  const presetSetup = async (agentCtx) => {
    try {
      const presets = ctx.get("agentPresets");
      if (presets) await presets.mount(agentCtx, "standard");
    } catch (e) { log("preset mount failed " + String((e && e.message) ?? e)); }
  };


  const resumeWithContext = async (wantId) => {
    const a = agents();
    if (!a) return void 0;
    const id = wantId || meSessionId();
    // 会议室（docs/meeting-room.md §3）：模型代码决定、每次 resume 强制对齐。
    // B2（2026-11-04）按角色分模型：房专属会话按 optsFor(id) 查台账叶子覆盖，主会话= FALLBACK_*。
    const opts = optsFor(id);
    const setup = async (agentCtx) => {
      try {
        const presets = ctx.get("agentPresets");
        if (presets) await presets.mount(agentCtx, "standard");
      } catch (e) { log("preset mount failed " + String((e && e.message) ?? e)); }
    };
    const r = await a.resume({ resumeSessionId: id, agentOptions: opts, setup });
    log("resumed " + id + " model=" + (opts.model ?? "?") + " provider=" + (opts.provider ?? "?"));
    return r;
  };

  const liveAgent = async (wantId) => {
    const id = wantId || meSessionId();
    const a = agents();
    if (!a) { log("agents missing"); return void 0; }
    let h;
    try { h = a.get(id); } catch (e) { log("get threw " + String((e && e.message) ?? e)); return void 0; }
    if (h) {
      const cand = h.agent ?? h;
      if (cand && typeof cand.followup === "function") return cand;
      log("stale handle -> resume");
    }
    try {
      const r = await resumeWithContext(id);
      return r?.agent ?? r;
    } catch (e) { log("resume failed " + String((e && e.message) ?? e)); return void 0; }
  };

  const keepWarm = async () => {
    const a = agents();
    if (!a) return;
    const id = meSessionId();
    const h = a.get(id);
    if (h) return;
    try { await resumeWithContext(); } catch (e) { log("keepWarm failed " + String((e && e.message) ?? e)); }
  };

  // 防归档守卫（v2 保留）：QQ 端会话常被自动归档，归档后 agent 收不到新回合，房间从此静默。
  // 每 60s（+启动 3s）把大小满两个会话从归档名单里拉回来。
  const archGuard = async () => {
    if (Date.now() - lastArch < 60000) return;
    lastArch = Date.now();
    const reg = svc("workspaceRegistry");
    const ctl = svc("workspaceController");
    if (!reg || !ctl) return;
    try {
      const archived = new Set(reg.archivedSessionIds || []);
      // 保活：DAMAN_SESS + 本端主会话 + 房专属会话（B1 房间感知：台账 status=archived 的不保活——
      // 房归档后会话该归档就归档，被本守卫拉回=归档永远失败，正是 11-04 用户遇到的「归档不了」）。
      // 但房从 rooms 消失前先关房（closed），closed 房的会话仍保活（用户：关房=只停注入不归档）。
      const alive = new Set([...roomSessions().byRoom.values()]);
      // 11-04 定档：写死的 DAMAN_SESS 移出保活（大满主会话想归档就归档，用户拍板）；
      // 只保本端主会话 + 未归档房的会话。房归档→会话自动出局（B1 房间感知）。
      const keep = [meSessionId(), ...alive];
      for (const sid of new Set(keep)) {
        if (archived.has(sid)) {
          await ctl.unarchiveSession({ sessionId: sid });
          evt({ action: "unarchive", sessionId: sid });
          log("unarchived " + sid);
        }
      }
    } catch (e) { log("archGuard failed " + String((e && e.message) ?? e)); }
  };

  // ---- 消息判定（v3 对称规则，契约 §2.5；与大满端同构）----
  const isProtocolMsg = (m) => /^我接 #\d+\s*$/.test(String(m.text || "").trim()) || /^我 #\d+ 让给 @/.test(String(m.text || "").trim());

  const relevant = (m, rooms) => {
    if (!m || m.recalled) return false;
    if (m.who === WHO) return false; // 2026-11-05 自激修复：自己发的消息一律不投回自己会话
    if (isProtocolMsg(m)) return false;
    const r = rooms.find((x) => x.id === m.roomId);
    if (!r) return false;
    if (r.status && r.status !== "open") return false;
    // system 只有「掉线转派」这类必须送达的才投；建群/解散/拉人/改规则等公告只进日志。
    if (m.kind === "system") return /⚠️|掉线|转给/.test(String(m.text || ""));
    // 任务卡：assignee 权威；没写 assignee 看正文点名。
    if (m.kind === "task") {
      if (m.who === WHO) return false;
      if (m.assignee) return m.assignee === WHO;
      return MY_RE.test(String(m.text || ""));
    }
    if (m.who === "you") {
      const t = String(m.text || "");
      const toMe = MY_RE.test(t);
      const toPeer = PEER_RE.test(t);
      if (toPeer && !toMe) return false;   // 只点对方：对方接
      return true;                          // 点我 / 都点 / 都没点
    }
    if (m.who === PEER) {
      // 结构化产物（方案/报告/评审/进度/交付）低频高价值：互投，保留「反驳与补充」通道；
      // 闲聊（text/say）只有点名我才投，防 ping-pong 烧 token。task 卡走上面的 assignee 分支。
      if (m.kind === "report" || m.kind === "review" || m.kind === "progress" || m.kind === "file") return true;
      return MY_RE.test(String(m.text || ""));
    }
    // 其他真人（受邀进来的 h-xxxx、手机端等）：也算「用户说话」。
    // 2026-11-05 修：以前这里直接 return false，房间里只有超级管理员说话才会有人回，
    // 受邀的人 @ 谁都没人理。现在点名对方只让对方接，其余都投，靠 claim 协议防抢答。
    const gt = String(m.text || "");
    if (PEER_RE.test(gt) && !MY_RE.test(gt)) return false;
    return true;
  };

  // CLI 侧差异（与旧版一致）：小满端不下载附件，只给路径提示。图片让 agent 用读图工具、
  // Word/PDF 让 agent 用 python 提取（都支持 HTTP ?k= 拉取）。
  const fmtSize = (n) => (n >= 1048576 ? (n / 1048576).toFixed(1) + " MB" : n >= 1024 ? (n / 1024).toFixed(1) + " KB" : n + " B");
  const fileHintLines = (m) => {
    const f = m.file;
    if (!f) return [];
    const web = (f.url ? `网页下载 ${base()}${f.url}?k=${encodeURIComponent(platKey())}` : "");
    return [
      `【附件】${f.name || "未命名"}（${fmtSize(f.size || 0)}，${f.mime || "未知类型"}）`,
      f.path ? `本地路径 ${f.path}` : "",
      web,
      "图片用读图工具；Word/PDF 用 python 提取后处理。",
    ].filter(Boolean);
  };

  // 2026-11-03 16:41 纯确认防刷屏：连续两条纯 ack 不互相唤醒（与大满端同构）。
  const ACK_RE = /^[好的收到明白了了解行嗯嘞ok，,。.!！~～\s]{1,12}$/i;
  const ackState = new Map();
  const isAckMsg = (m) => { const t = String(m.text || "").trim(); return t.length > 0 && t.length <= 12 && ACK_RE.test(t); };

  // 歧义判定（内联 v2 claim-lib）：用户发的、指向不明（都点名或都不点名）的普通聊天才走 claim。
  const isAmbiguousMsg = (m) => {
    // 2026-11-05：以前只认 who==="you"，受邀成员的「不点名」消息不进入 claim，
    // 两个 agent 都投就会双回。现在任何非 agent 的真人说话都走 claim。
    if (!m || m.who === WHO || m.who === PEER) return false;
    if (m.kind === "task") return false;
    const t = String(m.text || "");
    return MY_RE.test(t) === PEER_RE.test(t);
  };

  const claimedSeq = new Set();
  const ackSeq = async (seq, roomId) => {
    const a = await ackPost(seq, roomId);
    if (a.ok) { pendingAcks.delete(PA(roomId)); return; }
    if (a.status >= 400 && a.status < 500) {
      pendingAcks.delete(PA(roomId));
      evtOnce("ack-rej:" + seq, { action: "error", where: "ack", seq, status: a.status });
      return;
    }
    pendingAcks.set(PA(roomId), { seq, roomId: roomId || null });
    evtOnce("ack-net:" + seq, { action: "error", where: "ack", seq, status: a.status, message: a.error });
  };

  // ---- wake outcome watcher（契约 docs/wake-governance.md §6）----
  // 注入成功后启动：基线=注入后第一次拉取时该房最大 seq；每 WAKE_WATCH_INTERVAL_MS 拉一次状态，
  // 本端 who===WHO 出现 seq>基线 的房内输出（say/progress/report/review/ack，不含 system）→ settle REPLIED；
  // 超过 WAKE_OUTCOME_WINDOW_MS 无输出 → settle NO_REPLY。每端最多 1 个 watcher：
  // 新注入替换旧的，旧 wake 直接 settle NO_REPLY（不重复等待）。
  const WAKE_REPLY_KINDS = new Set(["text", "say", "progress", "report", "review", "ack"]);
  let wakeWatch = null;   // { wakeId, seq, roomId, baseSeq, deadline, timer }

  const settleWake = async (wakeId, outcome) => {
    const r = await wakeSettle(wakeId, outcome);
    if (!r.ok) evtOnce("wake-settle-fail:" + wakeId, { action: "wake-settle-fail", wakeId, outcome, status: r.status, message: r.error });
  };

  const finishWake = (outcome, w = wakeWatch) => {
    if (!w || wakeWatch !== w) return;   // 已被替换：不重复 settle
    wakeWatch = null;
    try { clearTimeout(w.timer); clearInterval(w.timer); } catch {}
    timers.delete(w.timer);
    evt({ action: "wake-outcome", wakeId: w.wakeId, outcome, seq: w.seq, room: w.roomId });
    log(`wake ${w.wakeId} -> ${outcome}`);
    settleWake(w.wakeId, outcome);
  };

  const startWakeWatch = (wakeId, seq, roomId) => {
    if (wakeWatch) log(`wake ${wakeWatch.wakeId} superseded by new inject, settle NO_REPLY`);
    finishWake("NO_REPLY");   // 契约 §6：新注入替换旧 watcher，旧 wake 直接 settle NO_REPLY
    const w = { wakeId, seq, roomId, baseSeq: null, deadline: Date.now() + WAKE_OUTCOME_WINDOW_MS, timer: null };
    wakeWatch = w;
    const tick = run("wake-watch", async () => {
      if (!live() || wakeWatch !== w) return;   // 被替换/卸载：静默停
      const st = await pullState(0);
      if (st.ok && st.s) {
        const msgs = Array.isArray(st.s.messages) ? st.s.messages : [];
        // nudge（roomId 为空）不盯单房：本端在任何房的新输出都算唤醒生效。
        const roomMsgs = w.roomId ? msgs.filter((x) => x && x.roomId === w.roomId) : msgs.filter((x) => x);
        if (w.baseSeq === null) {
          let roomMax = 0;
          for (const x of roomMsgs) { const s = Number(x.seq); if (Number.isFinite(s) && s > roomMax) roomMax = s; }
          w.baseSeq = Math.max(roomMax, Number(w.seq) || 0);
        }
        const replied = roomMsgs.some((x) => x && !x.recalled && x.who === WHO && x.kind !== "system"
          && WAKE_REPLY_KINDS.has(x.kind) && Number(x.seq) > w.baseSeq);
        if (replied) return finishWake("REPLIED", w);
      }
      if (Date.now() >= w.deadline) return finishWake("NO_REPLY", w);
    });
    w.timer = every(tick, WAKE_WATCH_INTERVAL_MS);
    later(tick, 0);
  };

  const deliver = async (m, room, opts = {}) => {
    log(`deliver seq=${m.seq} who=${m.who} kind=${m.kind} room=${m.roomId}${opts.takeover ? " (takeover)" : ""}`);
    // ① 纯确认 ping-pong 抑制。
    const prev = ackState.get(m.roomId || "-");
    ackState.set(m.roomId || "-", { seq: m.seq, ack: isAckMsg(m) });
    if (prev && prev.ack && prev.seq < m.seq && isAckMsg(m)) {
      evtOnce("ack:" + m.seq, { action: "ack-skip", seq: m.seq, room: m.roomId, who: m.who, prev: prev.seq });
      log(`ack-skip #${m.seq} (prev ack #${prev.seq})`);
      return true;
    }
    // 2026-11-05：以前只把超管的话当「用户聊天」，受邀成员的发言既不 claim 也不回。
    // T4（2026-11-07）：claim 范围扩到「不是自己、也不是对端 agent 发的」全部消息——
    // 以前 file / task / system 卡片两端都不 claim，同一张图、同一张任务卡被各注入一次（双投）。
    const claimRelevant = m.who !== WHO && m.who !== PEER;
    const claimNow = () => shouldClaim(m, { who: WHO, peer: PEER, ambiguous: isAmbiguousMsg(m) });
    // ② 已有 claim 裁决（可能是我 hold 期间对方接管了）：对方赢 → skip(claimed-peer) 并 ack。
    let claimMine = false;
    if (claimRelevant) {
      const st0 = await claimGet(m.roomId, m.seq);
      if (st0?.winner && st0.winner !== WHO) {
        evtOnce("claim:" + m.seq, { action: "skip", seq: m.seq, room: m.roomId, reason: "claimed-peer", winner: st0.winner });
        log(`claimed-peer skip #${m.seq} (winner=${st0.winner})`);
        return true;
      }
      claimMine = st0?.winner === WHO;
    }
    // ③ agent 存在性（hold）。P2 灰度：这间房配了专属会话就用它，否则仍用 QQ 主会话。
    const sid = sessionFor(m.roomId);
    const ag = await liveAgent(sid);
    log(`agent seq=${m.seq} -> ${ag ? (typeof ag.followup) : "none"} phase=${ag ? String(ag.phase?.kind ?? ag.phase) : "-"}`);
    if (!ag || typeof ag.followup !== "function") { log("no agent"); return false; }
    // ④ busy / ⑤ cooldown（hold，不 ack）。claim POST 在之后（v2 顺序：busy 方不占 claim）。
    //    BUG#2：hold 同时记账（哪个会话、哪条 seq、卡在什么 phase），卡死自愈靠这份账判断「有真 work 在等」。
    const phKind = phaseKind(ag);
    if (isBusyKind(phKind)) {
      noteHold(holdBook, sid, m.seq, phKind);
      holdClock.mark("busy:" + m.seq);
      evtOnce("hold:" + m.seq + ":busy:" + phKind, { action: "hold", seq: m.seq, room: m.roomId, kind: m.kind, reason: "busy:" + phKind, sessionId: sid });
      // A11（2026-11-08）：忙太久就把这条先放下（消息还在房历史里），ack 推游标；
      // 否则长任务期间的每条消息都要冻到任务结束。
      if (holdClock.expired("busy:" + m.seq)) {
        const waitedMin = Math.max(1, Math.round((Date.now() - holdClock.at("busy:" + m.seq)) / 60000));
        evt({ action: "hold-give-up", seq: m.seq, room: m.roomId, reason: "busy:" + phKind, waitedMin });
        log(`busy(${phKind}) give up #${m.seq} after ${waitedMin}min`);
        await sayText(m.roomId, `我这边还在忙（${phKind}），消息 #${m.seq} 等了 ${waitedMin} 分钟，先放下了；忙完我用 read 回看房间历史。`);
        holdClock.clear("busy:" + m.seq);
        settleHold(holdBook, sid, m.seq);
        return true; // ack：游标往前走，不再为这一条无限 hold
      }
      log(`busy(${phKind}) hold #${m.seq}`);
      return false;
    }
    if (Date.now() - (lastInjectByRoom.get(m.roomId) || 0) < COOLDOWN_MS) {
      evtOnce("hold:" + m.seq + ":cooldown", { action: "hold", seq: m.seq, room: m.roomId, kind: m.kind, reason: "cooldown" });
      log("cooldown hold #" + m.seq);
      return false;
    }
    // ⑥ claim 声明（契约 §2.5/§3.5）：v3 两端对称——歧义/接管时 POST，赢也写 claim 事件。
    if (claimRelevant && CLAIM_ENABLED && !claimMine && !claimedSeq.has(m.seq) && (claimNow() || opts.takeover)) {
      claimedSeq.add(m.seq);
      if (claimedSeq.size > 1000) { let n = 0; for (const k of claimedSeq) { claimedSeq.delete(k); if (++n >= 500) break; } }
      const posted = await claimPost(m.roomId, m.seq);
      if (posted?.ok && posted.claim) {
        await sleep(CLAIM_WAIT_MS);
        const st2 = await claimGet(m.roomId, m.seq);
        const winner = st2?.winner ?? null;
        if (winner && winner !== WHO) {
          evtOnce("claim:" + m.seq, { action: "skip", seq: m.seq, room: m.roomId, reason: "claimed-peer", winner, token: posted.claim.token });
          log(`claimed-peer skip #${m.seq} (winner=${winner} mine=${posted.claim.token})`);
          return true;
        }
        evtOnce("claim-mine:" + m.seq, { action: "claim", seq: m.seq, room: m.roomId, winner: winner || WHO, token: posted.claim.token, takeover: !!opts.takeover });
      } else {
        evtOnce("claim-fail:" + m.seq, { action: "claim-fail", seq: m.seq, room: m.roomId, status: posted?.status ?? 0 });
        // claim 失败：继续投（宁可多回一次，不静默丢）。
      }
    }
    // ⑦ 唤醒闸门（契约 docs/wake-governance.md §6）：注入前必须先 admit 领 wakeId。
    //    admit 网络失败 → hold、下 tick 重试（return false 不 ack，绝不无闸注入）；
    //    allow===false / ok===false → 预算或上下文超限，视为已处理（上层 ack、0 注入）。
    let wakeId = "";
    {
      const adm = await wakeAdmit(m.seq, m.roomId, m.kind);
      if (!adm.ok) {
        evtOnce("wake-admit-fail:" + m.seq, { action: "wake-admit-fail", seq: m.seq, room: m.roomId, kind: m.kind, status: adm.status, message: adm.error });
        log(`wake admit failed #${m.seq}` + (adm.status ? ` status=${adm.status}` : " (net)"));
        return false;
      }
      const am = adm.adm || {};
      if (am.ok === false || am.allow === false) {
        const reason = String(am.reason || (am.ok === false ? "REJECTED" : "COUNT"));
        evt({ action: "wake-blocked", reason, seq: m.seq, room: m.roomId, kind: m.kind, who: m.who });
        log(`wake blocked #${m.seq} reason=${reason}`);
        // T5：说一声再放下（幂等，同房同原因每天一条）
        await postNotice(m.roomId, reason, m.seq);
        return true;
      }
      if (!am.allow) {
        // 200 但响应残缺：不猜，hold 下 tick 重试。
        evtOnce("wake-admit-fail:" + m.seq, { action: "wake-admit-fail", seq: m.seq, room: m.roomId, kind: m.kind, status: adm.status, message: "bad admit body" });
        log(`wake admit bad body #${m.seq}`);
        return false;
      }
      wakeId = String(am.wakeId || "");
    }
    // ⑧ 组装并注入（CLI 侧差异：fileHint 不下载；taskHint 带 --assignee me；hint 带 --who me）。
    const head = `[ds-teams · ${m.roomId}${room?.title ? " · " + room.title : ""}] ${m.who} · ${m.kind} · #${m.seq}`;
    const floodNote = opts.floodSkipped > 0 ? `（该房间本次共 ${opts.floodSkipped + 1} 条新消息，已跳过前 ${opts.floodSkipped} 条，只送最新一条；需要细节用 read 看房间）` : "";
    const takeoverNote = opts.takeover ? `（接管：该消息被 ${PEER_NAME} hold 超过 60 秒，由 ${MY_NAME} 接手）` : "";
    const hint = `（回复：node "<repo>\\say.mjs" say ${m.roomId} "..." --who me；你是小满）`;
    const isTask = m.kind === "task";
    const taskId = String(m.taskId || "").trim() || ("auto-" + m.seq);
    const directive = isTask
      ? [`【任务卡】taskId=${taskId} assignee=${m.assignee || "me"} deadline=${m.deadlineAt || "未写"}`,
         `① 先 claim：node "<repo>\\dispatch.mjs" claim ${m.roomId} ${taskId} --seq ${m.seq} --assignee me`,
         "② claim 返回 duplicate=true（退出码 3）就无视，别再拉人。",
         `③ 成功则回一句「收到 ${taskId}，拆 s1/s2」，再 spawn_teammate（prompt 带 roomId/taskId/验收标准/子 taskId=${taskId}.s1），并 set spawned --sub ${taskId}.s1 ...`,
         "④ 子任务 progress/report 用子 taskId，结束时 review 并按需关房。"]
      : [];
    // 会议室（docs/meeting-room.md §4.4）：report 卡注入时加评审任务头——只提实质问题，没意见就沉默。
    const reviewDirective = m.kind === "report" && m.who === PEER
      ? [`【评审任务】${PEER_NAME} 交了一份报告，你是指定评审方。`,
         "评审规则：只提实质问题（事实错/逻辑断/证据缺/风险漏）；没有实质问题就不要输出任何内容（沉默即通过，系统会自动收敛）。",
         `有意见时交卡：node "<repo>\\say.mjs" review ${m.roomId} --verdict issues "结论/证据/改法" --who me`,
         "评审通过时交卡：node \"<repo>\\say.mjs\" review " + m.roomId + " --verdict pass \"评审通过\" --who me",
         "禁止客套、禁止复述报告内容。报告全文："]
      : [];
    // 契约 §6：followup 抛错 → settle ERROR（保留原 error 事件）。
    const failEvt = (e) => {
      evt({ action: "error", seq: m.seq, room: m.roomId, kind: m.kind, message: String((e && e.message) ?? e).slice(0, 300) });
      if (wakeId) settleWake(wakeId, "ERROR");
    };
    const fileHint = fileHintLines(m);
    const quoteLine = m.quote ? `↪ 引用 #${m.quote.seq} ${m.quote.who}：${String(m.quote.text || "").slice(0, 200)}` : "";
    try {
      const p = ag.followup(createUserMessage({
        content: [{ type: "text", text: [head, floodNote, takeoverNote, ...directive, ...reviewDirective, quoteLine, String(m.text || "").slice(0, 1200), ...fileHint, hint].filter(Boolean).join("\n") }],
        source: { kind: "plugin:ds-teams" },
      }));
      if (p && typeof p.catch === "function") p.catch(failEvt);
    } catch (e) { failEvt(e); log("followup threw " + String((e && e.message) ?? e)); return false; }
    lastInject = Date.now();
    lastInjectByRoom.set(m.roomId, lastInject);
    clearHold(holdBook, sid);   // BUG#2：这份账等的东西已经投进去了
    holdClock.clear("busy:" + m.seq);   // A11：hold 计时作废
    log(`inject seq=${m.seq} room=${m.roomId} kind=${m.kind}`);
    evt({ action: "inject", seq: m.seq, room: m.roomId, kind: m.kind, who: m.who, taskId: m.taskId || "", takeover: !!opts.takeover, floodSkipped: opts.floodSkipped || 0, wakeId });
    if (wakeId) startWakeWatch(wakeId, m.seq, m.roomId);   // 契约 §6：注入成功 → 启动 outcome watcher
    return true;
  };

  // 接管扫描（契约 §2.5/§3.6）：每 15s 一次，找「只点对方、>60s、对方没 ack 也没回话、无 claim 裁决」的 say，
  // 由我 claim 接手注入。每轮最多接管一条。
  const takeoverScan = async (rooms) => {
    if (Date.now() - lastTakeoverScan < 15000) return;
    lastTakeoverScan = Date.now();
    try {
      const st = await pullState(0);
      if (!st.ok) return;
      const s = st.s;
      const all = Array.isArray(s.messages) ? s.messages : [];
      const acks = serverAcks(s);
      const recent = all.slice(-200);
      for (const m of recent) {
        if (!m || m.recalled || !isSayKind(m) || m.who !== "you") continue;
        const t = String(m.text || "");
        if (!(PEER_RE.test(t) && !MY_RE.test(t))) continue;   // 只点对方（我这边按 not-relevant ack 过的）
        const age = Date.now() - Date.parse(m.ts || 0);
        if (!(age > TAKEOVER_HOLD_MS && age < TAKEOVER_MAX_AGE_MS)) continue;
        if (acks && typeof acks[PEER] === "number" && Number(acks[PEER]) >= Number(m.seq)) continue;
        const replied = all.some((x) => x.roomId === m.roomId && x.who === PEER && Number(x.seq) > Number(m.seq) && !x.recalled);
        if (replied) continue;
        const stc = await claimGet(m.roomId, m.seq);
        if (stc?.winner) continue;
        const room = rooms.find((x) => x.id === m.roomId) || null;
        if (!room || (room.status && room.status !== "open")) continue;
        const done = await deliver(m, room, { takeover: true });
        if (done) {
          evt({ action: "takeover", seq: m.seq, room: m.roomId, ageSec: Math.round(age / 1000) });
          log(`takeover #${m.seq} (held ~${Math.round(age / 1000)}s)`);
        }
        break;   // 每轮最多接管一条
      }
    } catch (e) { evt({ action: "error", where: "takeover-scan", message: String((e && e.message) ?? e).slice(0, 300) }); }
  };

  // ---- BUG#2 自愈（2026-11-05 房 room-912504）：phase 长时间非 idle → 强停那一轮 ----
  // 事故：小满会话从 05:53:18 起 phase=running 再不回 idle，deliver 第 ④ 步每 2s hold，
  // 每条消息都只能等对端 60s takeover——桥没有「自己爬起来」的路。宿主的 hold 是对的
  // （不能往正在跑的回合里硬塞），所以这里只做一件事：判定卡死后 agent.cancel({kind:"user"})，
  // 让 kick() 的循环收尾回 idle，下一 tick 那条被 hold 的消息自然重投。
  // 判据与护栏在 stuck-guard-lib.mjs（两端同一份，单测覆盖）；这里只负责取现场。
  let stuckBusy = false;
  let lastStuckResetAt = 0;
  const STUCK_RETRY_MS = 30_000;   // 强停一次后 30s 内不重复动手（给取消结算腾时间）
  // 本进程里所有活着的 agent（两端桥同进程：me 会话 / daman 主会话 / 各房专属会话都在这里）。
  const liveAgents = () => {
    const a = agents();
    let list = [];
    try { list = typeof a?.list === "function" ? a.list() : []; } catch { return []; }
    const out = [];
    for (const ag of list) {
      const id = String(ag?.id ?? ag?.sessionId ?? "");
      if (!id) continue;
      out.push({ id, kind: phaseKind(ag), ag });
    }
    return out;
  };
  const stuckGuard = async () => {
    if (stuckBusy) return;   // 上一轮还没结束（cancel 后 agent 结算中），别叠
    if (Date.now() - lastStuckResetAt < STUCK_RETRY_MS) return;
    try {
      const act = liveAgents();
      const busyIds = new Set(act.filter((x) => isBusyKind(x.kind)).map((x) => x.id));
      // hold 记账只留还在忙的：agent 已回 idle 的账没有意义（也会让下一轮误判）。
      for (const sid of [...holdBook.keys()]) if (!busyIds.has(sid)) holdBook.delete(sid);
      if (!busyIds.size) return;
      const logT = lastSeqTimeFromLog(EVENT_LOG);
      const rooms = [];
      for (const [rid, sid] of roomSessions().byRoom) if (busyIds.has(sid)) rooms.push({ rid, sid });
      stuckBusy = true;
      const r = decideStuck({
        now: Date.now(),
        holds: holdBook,
        // 同进程两端相加：对方桥看到同一份 agent 状态，这里只取自己的面不动对方逻辑。
        activity: act.map((x) => {
          const b = holdBook.get(x.id) || null;
          return {
            id: x.id,
            kind: x.kind,
            sinceMs: b?.firstBusyAt || 0,
            holds: b?.holds || 0,
            lastSeqAt: logT || 0,   // 只认桥事件日志里的真进展；hold 重试不算（2026-11-07）
          };
        }),
        lastRoomAt: lastRoomPeerAt,
        roomOf: (sid) => { const f = rooms.find((y) => y.sid === sid); return f ? f.rid : null; },
      });
      if (r.action !== "reset") return;
      const hit = act.find((x) => x.id === r.sessionId) || null;
      const ag = hit?.ag;
      if (!ag || typeof ag.cancel !== "function") return;
      lastStuckResetAt = Date.now();
      try {
        ag.cancel({ kind: "user" });   // = 用户点「停止」：清 inbox + abort 当前 turn
        evt({
          action: "stuck-reset",
          sessionId: r.sessionId,
          phase: r.kind,
          stuckMs: r.stuckMs,
          holds: (holdBook.get(r.sessionId)?.holds) || 0,
          lastSeq: holdBook.get(r.sessionId)?.lastSeq ?? null,
          reason: r.reason,
        });
        log(`stuck guard: cancel ${r.sessionId} (phase=${r.kind} ${Math.round(r.stuckMs / 1000)}s, holds=${(holdBook.get(r.sessionId)?.holds) || 0})`);
      } catch (e) {
        evtOnce("stuck-cancel-fail:" + r.sessionId, { action: "stuck-cancel-fail", sessionId: r.sessionId, message: String((e && e.message) ?? e).slice(0, 200) });
      }
    } catch (e) {
      evtOnce("stuck-guard-err", { action: "error", where: "stuck-guard", message: String((e && e.message) ?? e).slice(0, 300) });
    } finally {
      stuckBusy = false;
    }
  };

  const tick = async () => {
    if (!live()) return;
    // busy 看门狗（契约 §3.8）：15s 强制复位；不取消 in-flight（与大满端同构）。
    if (busy) {
      if (Date.now() - busySince < 15000) return;
      log("busy watchdog reset after " + Math.round((Date.now() - busySince) / 1000) + "s");
      evtOnce("watchdog", { action: "watchdog", afterMs: Date.now() - busySince });
      busy = false;
    }
    // 退避门控（契约 §3.6）：固定 2s interval 驱动 + nextAllowedAt 门控。
    if (Date.now() < nextAllowedAt) return;
    busy = true;
    busySince = Date.now();
    try {
      // ① 心跳（契约 §3.1）。
      const hb = await heartbeat();
      if (!hb.ok) evtOnce("hb:" + (hb.status || "net"), { action: "error", where: "heartbeat", status: hb.status, message: hb.error });
      // ② 拉状态。
      const st = await pullState(cursor ?? 0);
      if (!st.ok) evtOnce("state:" + (st.status || "net"), { action: "error", where: "state", status: st.status, message: st.error });
      // ③ 退避记账（契约 §3.6）。
      if (hb.ok && st.ok) {
        if (failStreak) log("poll recovered, back to 2s");
        failStreak = 0;
        pollDelay = 2000;
      } else {
        failStreak += 1;
        if (failStreak >= 3) {
          pollDelay = Math.min(30000, 2000 * Math.pow(2, failStreak - 2));
          evtOnce("backoff:" + failStreak, { action: "error", where: "backoff", streak: failStreak, pollDelayMs: pollDelay, hbOk: hb.ok, stOk: st.ok });
        }
      }
      if (!st.ok || !st.s) return;
      const s = st.s;
      const msgs = Array.isArray(s.messages) ? s.messages : [];
      const rooms = Array.isArray(s.rooms) ? s.rooms : [];
      // ④ 补 ack。
      for (const [k, p] of [...pendingAcks]) {
        const a = await ackPost(p.seq, p.roomId);
        if (a.ok || (a.status >= 400 && a.status < 500)) pendingAcks.delete(k);
      }
      // ⑤ 游标初始化（首次）：文件 → 服务端 ack 游标 → 冷启动采用最新并 ack 一次。
      if (cursor === null) {
        try {
          const j = JSON.parse(read(CURSOR_FILE) || "null");
          cursor = (j && typeof j.seq === "number" && Number.isFinite(j.seq)) ? j.seq : null;
          if (j && j.rooms && typeof j.rooms === "object") {
            for (const [k, v] of Object.entries(j.rooms)) if (Number.isFinite(Number(v))) roomCursors.set(String(k), Number(v));
          }
        } catch { cursor = null; }
        // 服务端现在直接暴露 cursor（所有 open 房的低水位）与 roomCursors（C4），优先采信它。
        const srv = (s && typeof s.cursor === "number" && Number.isFinite(s.cursor)) ? s.cursor : serverAckCursor(s, WHO);
        if (typeof srv === "number" && srv > (cursor ?? -1)) cursor = srv;
        const srvRooms = (s && s.roomCursors && typeof s.roomCursors === "object") ? s.roomCursors[WHO] : null;
        if (srvRooms && typeof srvRooms === "object") {
          for (const [k, v] of Object.entries(srvRooms)) {
            const n = Number(v);
            if (Number.isFinite(n) && n > (roomCursors.get(String(k)) ?? -1)) roomCursors.set(String(k), n);
          }
        }
        if (cursor === null) {
          cursor = Number(s.seq || 0);
          evt({ action: "cold-start", from: 0, to: cursor });
          log("cold start: cursor=" + cursor + " (server ack cursor not exposed)");
          const a = await ackPost(cursor);
          if (!a.ok && !(a.status >= 400 && a.status < 500)) pendingAcks.set("-", { seq: cursor, roomId: null });
        } else {
          evt({ action: "cursor-restore", cursor, serverSeq: Number(s.seq || 0) });
        }
        saveCursor(cursor);
        return;
      }
      // ⑥ 逐条处理 pending（v3 无 backlog-skip：卡片永不丢；flood 只对 say）。
      const pending = msgs.filter((m) => Number(m.seq ?? -1) > cursor).sort((a, b) => Number(a.seq) - Number(b.seq));
      if (pending.length) {
        const sayCount = new Map();
        const lastSaySeq = new Map();
        for (const m of pending) if (isSayKind(m)) {
          sayCount.set(m.roomId, (sayCount.get(m.roomId) || 0) + 1);
          lastSaySeq.set(m.roomId, Number(m.seq));
        }
        const floodRooms = new Set();
        for (const [roomId, n] of sayCount) if (n > FLOOD_LIMIT) floodRooms.add(roomId);
        heldRooms = new Set();
        for (const m of pending) {
          if (disposed || !live()) break;
          const seq = Number(m.seq ?? -1);
          if (seq <= cursorOf(m.roomId)) continue;
          if (heldRooms.has(m.roomId)) continue;   // 该房本轮已 hold：保住房内顺序，不越过它
          const room = rooms.find((x) => x.id === m.roomId) || null;
          const floodKept = isSayKind(m) && floodRooms.has(m.roomId) && seq === lastSaySeq.get(m.roomId);
          let skipReason = null;
          if (isSayKind(m) && floodRooms.has(m.roomId) && !floodKept) {
            skipReason = "flood";
          } else if (!relevant(m, rooms)) {
            skipReason = "not-relevant";
          } else if (m.kind === "ack") {
            evt({ action: "ack-log", seq, room: m.roomId, who: m.who, kind: "ack" });
            skipReason = "ack-log";
          } else if (!INJECTABLE.has(m.kind)) {
            skipReason = "not-injectable";
          }
          if (skipReason) {
            if (skipReason === "flood") evtOnce("flood:" + m.roomId + ":" + lastSaySeq.get(m.roomId), { action: "flood", room: m.roomId, skipped: (sayCount.get(m.roomId) || 0) - 1, kept: lastSaySeq.get(m.roomId) });
            else if (skipReason !== "ack-log") evtOnce("skip:" + seq + ":" + skipReason, { action: "skip", seq, room: m.roomId, kind: m.kind, reason: skipReason });
            roomCursors.set(m.roomId, seq);
            cursor = cursorFloor();
            saveCursor(cursor);
            await ackSeq(seq, m.roomId);   // C4：按房推进
            continue;
          }
          const floodSkipped = floodKept ? (sayCount.get(m.roomId) || 0) - 1 : 0;
          const done = await deliver(m, room, { floodSkipped });
          if (m.who && m.who !== WHO) roomLastPeerAt.set(m.roomId, Date.now());   // 房内最后一条对端消息（卡死自愈护栏③）
          // BUG#2：只有「真的了结」才销账。hold（done=false）说明这条还在等，账必须留着——
          // 这正是卡死判据里「有真 work 在等」的证据，提前销账会让自愈永远不触发。
          if (!done) { heldRooms.add(m.roomId); continue; }   // hold：只冻结这一个房，别的房照走
          settleHold(holdBook, sessionFor(m.roomId), seq);
          roomCursors.set(m.roomId, seq);
          cursor = cursorFloor();
          saveCursor(cursor);
          await ackSeq(seq, m.roomId);   // C4：按房推进
        }
      }
      // ⑥ B1：自动建会话（15s 节流）+ 归档房感知（房消失→archiveSession+台账标 archived）。
      await ensureRoomSessions(rooms);
      await archiveGoneRooms(rooms);
      // ⑦ 接管扫描（每 15s，契约 §2.5）——这是「等对方接管」，⑧' 的自愈才是「自己爬起来」。
      await takeoverScan(rooms);
      // ⑧' BUG#2 自愈：会话 phase 卡死在非 idle ≥180s → 强停那轮、下一条消息自然重投。
      await stuckGuard(rooms);
      // ⑨ 防归档守卫（CLI 侧差异）。
      await archGuard();
    } catch (e) {
      evt({ action: "error", where: "tick", message: String((e && e.message) ?? e).slice(0, 300) });
    } finally {
      busy = false;
      nextAllowedAt = Date.now() + pollDelay;
    }
  };

  log("apply loaded g" + gen + " pid=" + process.pid + " node=" + process.version
    + " effectTimers=" + effectTimers
    + " ctxAgents=" + (() => { try { return typeof ctx.agents; } catch (e) { return "throw:" + String((e && e.message) ?? e); } })()
    + " getAgents=" + (() => { try { return svc("agents") ? "yes" : "no"; } catch (e) { return "throw"; } })());
  try {
    ctx.effect(() => {
      log("mounted g" + gen);
      return () => log("unloaded g" + gen + " (effect cleanup ran)");
    }, "ds-teams-bridge-me.lifecycle");
  } catch {}
  later(run("warm", () => keepWarm()), 2000);
  every(run("warm", () => keepWarm()), 15000);
  later(run("poll", tick), 1000);
  every(run("poll", tick), 2000);
  later(run("arch", () => archGuard()), 3000);
  every(run("arch", () => archGuard()), 60000);
  every(run("beat", () => { log("alive g" + gen + " seq=" + (cursor ?? "?") + " agents=" + (agents() ? "yes" : "no")); }), 60000);
  // 被新一代实例取代后自行熄火（双保险）。
  every(() => {
    if (disposed || globalThis[GEN_KEY] === gen) return;
    disposed = true;
    try { finishWake("NO_REPLY"); } catch {}   // 熄火前把未回填的 wake 收口
    for (const t of timers) { try { clearTimeout(t); clearInterval(t); } catch {} }
    timers.clear();
    log("superseded by g" + globalThis[GEN_KEY] + ", timers cleared");
  }, 5000);

  try {
    ctx.on("dispose", () => {
      disposed = true;
      try { finishWake("NO_REPLY"); } catch {}   // 卸载前把未回填的 wake 收口
      for (const t of timers) { try { clearTimeout(t); clearInterval(t); } catch {} }
      timers.clear();
      log("disposed g" + gen + ", timers cleared");
    });
  } catch {}
}

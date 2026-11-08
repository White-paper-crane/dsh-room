// ds-teams 桥接（大满端 v3）：chat-first-v3 协议。把房间里需要大满处理的消息送进大满的回合。
// v3 协议变化（相对 v2，契约 docs/chat-first-v3.md 第 3 节）：
//   ① 每 tick 先 POST /api/bridge/heartbeat {who,key}；
//   ② GET /api/state?since=cursor&who=本端 不再推进游标；游标只由 POST /api/state/ack {who,key,seq} 推进（累积到 seq 含）。
//      注入成功或判定跳过（not-relevant/claimed-peer/flood 等）即 ack；hold（busy/cooldown）不 ack，下 tick 重试；
//   ③ 丢弃策略：只对 kind=say（服务端实际 kind=text）做同房 flood 上限（>5 留最新）；
//      kind in {task,progress,report,review,file,system} 永不丢弃、永不 flood、永不 backlog-skip；
//   ④ kind=ack 一律不注入，只写日志 action=ack-log 并 ack；
//   ⑤ claim 两端对称（CLIAM_ENABLED=true）：未点名/双点名 say 都可 claim（token 小者赢），
//      输方 skip(reason=claimed-peer) 并 ack；赢方也写 claim 事件（v3 补齐 me 端旧的不对称）；
//      T4（2026-11-07）：claim 范围扩到 file / task / system（不是自己、也不是对端 agent 发的都算），
//      这类卡片一律视为歧义直接声明——原来它们完全不过 claim，两端各注入一次（双投）；
//   ⑥ 同一条消息被 hold 连续超 60s：另一端可 claim 接管注入（每 15s 扫一次，claim 范围扩到被 hold 超时的 say）；
//   ⑦ 心跳/state 连续 3 tick 失败 → 写 error 日志并指数退避 2s→4s→8s…上限 30s，恢复后回 2s；
//   ⑧ 保留 busy 看门狗（15s 复位、不取消 in-flight）、cooldown、附件下载、stall 报警、dispatch 台账维护。
// 生存纪律（v2 延续，17:26/23:40 事故教训）：
//   ① 定时器回调一律过 run()，同步抛/异步 reject 都吞掉只记日志；
//   ② 取 ctx 服务统一走 agents() 兜底，取不到静默退出；
//   ③ 定时器挂 ctx.timeout/ctx.interval（effect 自动清理）+ 世代锁 + supercede 自熄，防热加载僵尸。
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
// 动态 import + 时间戳：宿主会缓存同 URL 的 dispatch-lib，改了导出不重启就拿到旧模块（2026-11-03 事故）。
// v3 只用台账维护 watchdog/promote；任务卡路由判定已内联（两端对称），不再依赖 isTaskForDaman。
const { watchdog, promote, claimedAwaitingSpawn, clearNudge } = await import(pathToFileURL(path.join(ROOT, "dispatch-lib.mjs")).href + "?v=" + Date.now());
// B1（2026-11-04）：房会话台账写库（与小满端共用 room-session-lib.mjs）。
// B2（2026-11-04）：endOpts = 台账叶子 provider/model 覆盖的纯函数（按角色分模型）。
const { writeEnd, markEndArchived, endOpts } = await import(pathToFileURL(path.join(ROOT, "room-session-lib.mjs")).href + "?v=" + Date.now());
// BUG#2 自愈（2026-11-05）：会话 phase 长期 running 不收敛 → 桥永 hold，只有被对端 60s takeover 一条出路。
// 判定放共享纯函数库（两端同一条判据，与 ds-teams-bridge-me.mjs 对称）。
const { createHoldBook, noteHold, clearHold, settleHold, phaseKind, isBusyKind, lastSeqTimeFromLog, decideStuck } = await import(pathToFileURL(path.join(ROOT, "stuck-guard-lib.mjs")).href + "?v=" + Date.now());
const { createHoldClock, HOLD_MAX_MS } = await import(pathToFileURL(path.join(ROOT, "hold-lib.mjs")).href + "?v=" + Date.now());
// T5（2026-11-07）：闸门拒绝后的房内说明文案，放共享纯函数库，两端同一份、同一幂等键。
const { noticeFor } = await import(pathToFileURL(path.join(ROOT, "wake-notice-lib.mjs")).href + "?v=" + Date.now());
// T4（2026-11-07）：claim 适用范围——file / task / system 也要 claim，否则两端各注入一次（双投）。
const { shouldClaim } = await import(pathToFileURL(path.join(ROOT, "claim-scope-lib.mjs")).href + "?v=" + Date.now());

export const name = "ds-teams-bridge";
export const inject = ["agents", "timer", "workspaceController"];

const PORT_FILE = ROOT + "\\data\\port.txt";
const KEY_FILE = ROOT + "\\data\\key.txt";
const CURSOR_FILE = ROOT + "\\data\\bridge-daman-cursor.json";
// 事件日志（D3）：注入/跳过/hold/claim/心跳/退避各落一行 JSON，回归直接 grep。
const EVENT_LOG = ROOT + "\\data\\bridge-daman.log";
const DAMAN = process.env.DS_TEAMS_DAMAN_SESSION || "";
// P2 单房灰度（2026-11-04）：房 → 专属会话台账。只读文件；**改文件即回滚**，不用重挂载。
// 缺失 / enabled:false / 无此房 → 全部退回上面那个共用会话（旧行为，零差异）。
const ROOM_SESSIONS_FILE = ROOT + "\\data\\room-sessions.json";
const COOLDOWN_MS = 3000;
// 供应商与模型由代码决定，不随 GUI 选择漂移（会议室 v1.1：用户 2026-11-04 要求两端统一走
// 自定义供应商 TokenRocks 的 flash 档，不再用官网 deepseek-account）。providers 定义见
// 桌面 profile 的 cordis.patch.yml llm-pi-ai 段：tokenrocks / deepseek-v4.1-flash（1M 窗口）。
// 若要恢复异构评审场，把本行改成 tokenrocks 的 kimi-k3.1-pro 或 glm-5.3 即可（一行切换）。
//
// 2026-11-04 异构评审场（P1）：大满端切到 glm-5.3（智谱旗舰，1M 上下文，¥8/¥28，缓存命中 ¥2）。
// 依据 docs/research-hetero-review.md §5：能力差距 > 家族差异，故选「更强且不同家族」；
// GLM-5.3 始终开启思考，reasoningEfforts 在 profile 里映射 low→low / medium→high / high→max。
// 回滚：把 FALLBACK_MODEL 改回 deepseek-v4.1-flash，重挂载即可（见 docs 交接 §5.2）。
// 2026-11-05 群主拍板（房 room-912504）：glm-5.3 出字太贵（入¥0.98/出¥3.08，贵 4.9 倍），
// 大满端切 glm-5.3-flash（异家族、约 1/6 价）。回滚：改回 glm-5.3，重挂载（见 docs 交接 §5.2）。
// 注：B1 房专属会话台账叶子若无 provider/model，仍回落 FALLBACK_*；故改这里即对未配覆盖的新房生效。
const FALLBACK_PROVIDER = "tokenrocks";
const FALLBACK_MODEL = "glm-5.3-flash";

// ---- v3 端侧配置（参数化；小满端在 ds-teams-bridge-me.mjs 里换成对称值）----
const WHO = "daman";               // 本端身份（people.json 的键）
const PEER = "me";                 // 对端身份
const MY_NAME = "大满";
const PEER_NAME = "小满";
const CLAIM_ENABLED = true;        // v3：两端对称，都参与 claim（契约 §3.5）
const CLAIM_WAIT_MS = 2000;
const FLOOD_LIMIT = 5;             // 同房 say >5 只留最新（契约 §3.3）
const TAKEOVER_HOLD_MS = 60_000;   // hold 超时阈值（契约 §2.5：连续 hold >60s）
const TAKEOVER_MAX_AGE_MS = 600_000; // 接管只扫 10 分钟内的消息，冷启动不翻旧账
const MY_RE = /大满|daman/i;       // 本端名字（点名判定）
const PEER_RE = /小满|\bme\b/i;    // 对端名字
// 注入白名单（契约 §2.4）：confirm/patrol/未知 kind 一律不注入。
const INJECTABLE = new Set(["text", "say", "task", "progress", "report", "review", "file", "system"]);
// 契约里的 kind=say = 服务端实际落库的用户发言 kind=text；两个都认，兼容服务端改名。
const isSayKind = (m) => !!m && (m.kind === "text" || m.kind === "say");

// ---- wake 治理（契约 docs/wake-governance.md §5/§6）----
// 每次注入前 POST /api/wake/admit 领 wakeId（预算闸），注入后由 outcome watcher 回填 settle。
const WAKE_OUTCOME_WINDOW_MS = 90_000;   // 注入后观察窗口：90s 内本端无房内输出 → NO_REPLY
const WAKE_WATCH_INTERVAL_MS = 2_000;    // outcome watcher 拉状态间隔

const read = (p) => { try { return fs.readFileSync(p, "utf8").trim(); } catch { return ""; } };

const GEN_KEY = Symbol.for("ds-teams-bridge.gen");

export function apply(ctx) {
  // 第几代实例（同一进程内递增）。热加载会新建实例但旧实例的定时器不会自动停，
  // 用全局代号让旧实例的所有回调自动失效，避免重复轮询/重复投递。
  const gen = (globalThis[GEN_KEY] = (globalThis[GEN_KEY] ?? 0) + 1);
  const live = () => !disposed && globalThis[GEN_KEY] === gen;
  const log = (m) => { try { ctx.logger?.info?.("[ds-teams-bridge] g" + gen + " " + m); } catch {} };
  // 事件日志：{at, side, action, seq, room, kind, reason, ...}；失败静默，绝不因写日志炸桥。
  const evt = (o) => { try { fs.appendFileSync(EVENT_LOG, JSON.stringify({ at: new Date().toISOString(), side: "daman", gen, ...o }) + "\n", "utf8"); } catch {} };
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
  // A11（2026-11-08）：同一条消息被 hold 多久了；超过 HOLD_MAX_MS 就放弃这一轮注入并 ack。
  const holdClock = createHoldClock();
  const roomLastPeerAt = new Map();
  const lastRoomPeerAt = (roomId) => roomLastPeerAt.get(String(roomId)) || 0;
  let lastMaint = 0;
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
  // 定时器必须挂在 fiber 上：本版 cordis 没有用户可见的 dispose 事件（只 emit
  // internal/plugin、internal/status），ctx.on("dispose") 永远不触发；裸 setTimeout
  // 在重载后成了僵尸，ctx 已 inactive 却还在每 2 秒轮询（= "agents missing" 刷屏）。
  // ctx.timeout/ctx.interval 走 ctx.effect，fiber 卸载时自动清理。
  const effectTimers = !!(ctx.timeout && ctx.interval);
  const later = (fn, ms) => { const t = effectTimers ? ctx.timeout(fn, ms) : setTimeout(fn, ms); timers.add(t); return t; };
  const every = (fn, ms) => { const t = effectTimers ? ctx.interval(fn, ms) : setInterval(fn, ms); timers.add(t); return t; };

  const saveCursor = (seq) => { try { fs.writeFileSync(CURSOR_FILE, JSON.stringify({ seq, at: Date.now(), rooms: Object.fromEntries(roomCursors) }), "utf8"); } catch {} };

  // ---- 服务端访问 ----
  const base = () => "http://127.0.0.1:" + (read(PORT_FILE) || "8792");
  const platKey = () => read(KEY_FILE);
  const memberKey = (who) => {
    // 写操作要两个 key：平台 ?k=（key.txt）+ 成员 body.key（people.json / members.json）。
    for (const f of ["people.json", "members.json"]) {
      try { const j = JSON.parse(read(ROOT + "\\data\\" + f)); if (j && j[who] && j[who].key) return j[who].key; } catch {}
    }
    return "";
  };
  // 契约 §3.1：POST /api/bridge/heartbeat body {who,key}
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
  // 契约 §2.1：POST /api/state/ack body {who,key,seq}（游标推进到 seq 含）
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
  // 契约 §2.1/§3.2：GET /api/state?since=N&who=本端（不再推进游标）
  const pullState = async (since) => {
    try {
      const r = await fetch(`${base()}/api/state?since=${Number(since) || 0}&who=${WHO}&k=${encodeURIComponent(platKey())}`, { signal: AbortSignal.timeout(3000) });
      if (!r.ok) return { ok: false, status: r.status };
      return { ok: true, s: await r.json() };
    } catch (e) { return { ok: false, error: String((e && e.message) ?? e).slice(0, 120) }; }
  };
  const postSay = async (roomId, text) => {
    try {
      const r = await fetch(`${base()}/api/rooms/${encodeURIComponent(roomId)}/say?k=${encodeURIComponent(platKey())}`, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ who: "daman", key: memberKey("daman"), text }), signal: AbortSignal.timeout(3000),
      });
      if (!r.ok) return null;
      return await r.json().catch(() => null);
    } catch { return null; }
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
  // T5（2026-11-07）：闸门拒了不能静默。原实现「预算/上下文超限」直接当已处理（ack + 0 注入），
  // 房里看不到任何解释。这里补一条说明，按「房 + 原因 + 日期」幂等：
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
  // 服务端 ack 游标（防御性读取）：v3 状态响应若暴露 acks/cursors/ackSeqs 就用它兜底本地文件。
  const serverAckCursor = (s, who) => {
    try {
      const obj = s && (s.acks || s.cursors || s.ackSeqs);
      if (obj && typeof obj === "object" && !Array.isArray(obj) && typeof obj[who] === "number" && Number.isFinite(obj[who])) return obj[who];
      if (s && typeof s.ackSeq === "number" && Number.isFinite(s.ackSeq)) return s.ackSeq;
      if (s && typeof s.ack === "number" && Number.isFinite(s.ack)) return s.ack;
    } catch {}
    return null;
  };
  const serverAcks = (s) => { try { const o = s && (s.acks || s.cursors || s.ackSeqs); return o && typeof o === "object" && !Array.isArray(o) ? o : null; } catch { return null; } };

  // ---- 会话/agent 生命周期（v2 原样）----
  // ---- 单房灰度：roomId → 专属会话（ROOM_SESSIONS_FILE）----
  // 5 秒缓存：改 room-sessions.json 后 ≤5s 生效——这就是回滚通道（比环境变量/重挂载快得多）。
  // B2（2026-11-04）：bySid 缓存同一批叶子的原样对象，供 optsFor(sessionId) 查 provider/model 覆盖。
  let rsCache = { at: 0, byRoom: new Map(), bySid: new Map() };
  const roomSessions = () => {
    if (Date.now() - rsCache.at < 5000) return rsCache;
    let j = null;
    // 台账是人手改的：记事本/PowerShell Set-Content 会带 UTF-8 BOM，JSON.parse 会直接抛
    // "Unexpected token" 然后静默回落成"没配过房会话"——灰度看起来生效其实没生效。剥掉再解，
    // 解不出来就吼一声（别让配置坏了还静默走共用会话）。
    try { j = JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); }
    catch (e) { log("room-sessions parse failed: " + String((e && e.message) ?? e).slice(0, 120)); j = null; }
    const byRoom = new Map();
    const bySid = new Map();
    if (j && j.enabled !== false && j.rooms && typeof j.rooms === "object") {
      for (const [rid, r] of Object.entries(j.rooms)) {
        const leaf = r && r.ends && r.ends[WHO];
        // B1：叶子 status=archived（房已归档）→ 不进 byRoom（sessionFor 回落主会话、不保活）。
        const sid = leaf && leaf.sessionId && leaf.status !== "archived" ? String(leaf.sessionId) : "";
        if (sid) { byRoom.set(String(rid), String(sid)); bySid.set(String(sid), leaf); }
      }
    }
    rsCache = { at: Date.now(), byRoom, bySid };
    if (byRoom.size) log("room-sessions: " + [...byRoom].map(([r, s]) => r + "->" + s).join(", "));
    return rsCache;
  };
  // 唯一入口：房有专属会话就用它，否则回落共用会话（= 未启用灰度时的现有行为）。
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

  // ---- B1（2026-11-04）：自动为 open 房建本端专属会话 + 归档感知（与小满端对称） ----
  // 规则（用户 11-04 定）：关房=只停注入不归档会话；归档房（从 rooms 消失）= archiveSession + 台账标 archived。
  // create 后必须 followup 一条房简报才落盘，否则 resume 抛 SessionPersistenceNotFoundError。
  let autoCreateOn = true;   // 每次扫描现读台账 enabled（改文件 ≤15s 生效）
  let lastAutoCreate = 0;
  const creatingRoom = new Set();
  const failedRoom = new Map();     // roomId → 下次允许重试时刻（退避 60s 起，翻倍上限 15min）
  function roomSessionsEnabled() { try { const j = JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); return j && j.enabled !== false; } catch { return true; } }
  const readLedgerRaw2 = () => { try { return JSON.parse(fs.readFileSync(ROOM_SESSIONS_FILE, "utf8").replace(/^\uFEFF/, "")); } catch { return null; } };
  const presetSetup = async (agentCtx) => {
    try {
      const presets = ctx.get("agentPresets");
      if (presets) await presets.mount(agentCtx, "standard");
    } catch (e) { log("preset mount failed " + String((e && e.message) ?? e)); }
  };

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
      rsCache.at = 0;
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
    autoCreateOn = roomSessionsEnabled();
    for (const r of (rooms || [])) {
      if (disposed || !live()) break;
      if (r.status !== "open") continue;
      await ensureRoomSession(r);
    }
  };

  // 归档感知：房从 rooms 消失 → 本端叶子若有会话则 archiveSession + 台账标 archived。
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


  // QQ 会话 id 会换（/new、preset 轮换都会新建目录）。写死一个 id 会投进已停用的旧会话：
  // 2026-11-02 21:09 就投进了 mvxli42x，而当时活动的会话是 mvz3ntya，于是房间里"我不说话"。
  // 这里按会话文件最后写入时间取当前活动的 QQ 会话。
  // 先认 DAMAN；目录不在就回退到 SESS_ROOT 下最新的 session-* 目录，并记一条 fallback 日志（不静默）。
  const meSessionId = () => {
    try {
      if (fs.existsSync(SESS_ROOT + "\\" + DAMAN)) return DAMAN;
      const st = (d) => { try { return fs.statSync(SESS_ROOT + "\\" + d + "\\session.v4.jsonl.zstd").mtimeMs; } catch { try { return fs.statSync(SESS_ROOT + "\\" + d).mtimeMs; } catch { return -1; } } };
      // 房会话不是"主会话"，绝不能被 mtime 回退选中（否则某天 DAMAN 目录没了，
      // 回退会把大满的 agent 粘到一间房的会话上）。
      const reserved = new Set(roomSessions().byRoom.values());
      const dirs = fs.readdirSync(SESS_ROOT).filter((d) => {
        if (reserved.has(d)) return false;
        try { return fs.statSync(SESS_ROOT + "\\" + d).isDirectory(); } catch { return false; }
      });
      if (!dirs.length) { log("session fallback: DAMAN missing, no session dir, keep DAMAN"); return DAMAN; }
      let best = dirs[0], bestT = -1;
      for (const d of dirs) { const t = st(d); if (t > bestT) { bestT = t; best = d; } }
      log(`session fallback: DAMAN missing -> ${best} (mtime ${new Date(bestT).toISOString()})`);
      return best;
    } catch (e) { log("session fallback failed: " + String((e && e.message) ?? e)); return DAMAN; }
  };

  const resumeWithContext = async (wantId) => {
    const a = agents();
    if (!a) return void 0;
    const id = wantId || meSessionId();
    // 会议室（docs/meeting-room.md §3）：模型代码决定、每次 resume 强制对齐（不用会话残留的旧模型）。
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

  // ---- 消息判定（v3 对称规则，契约 §2.5）----
  // 声明/让位消息是 v2 协议残留，不是要回的内容：不注入。
  const isProtocolMsg = (m) => /^我接 #\d+\s*$/.test(String(m.text || "").trim()) || /^我 #\d+ 让给 @/.test(String(m.text || "").trim());
  // system 公告里真正要人处理的那几类；其余（建群/解散/归档）只写日志。
  const URGENT_SYS = /⚠|掉线|失联|转给|接管|降级|degraded|takeover/i;

  const relevant = (m, rooms) => {
    if (!m || m.recalled) return false;
    if (m.who === WHO) return false; // 2026-11-05 自激修复：自己发的消息一律不投回自己会话
    if (isProtocolMsg(m)) return false;
    const r = rooms.find((x) => x.id === m.roomId);
    if (!r) return false;
    if (r.status && r.status !== "open") return false;
    // system 卡：只有「必须送达」的告警（掉线/转派/降级/接管）才投；建群/解散等公告只进日志。
    // 2026-11-03 v3 噪音回归：原来是 kind==='system' 一刀切 true，建群公告会把人叫醒一次。
    if (m.kind === "system") return URGENT_SYS.test(String(m.text || ""));
    // 任务卡：assignee 权威；没写 assignee 看正文点名（与 v2 isTaskForDaman / me 端规则一致）。
    if (m.kind === "task") {
      if (m.who === WHO) return false;
      if (m.assignee) return m.assignee === WHO;
      return MY_RE.test(String(m.text || ""));
    }
    if (m.who === "you") {
      const t = String(m.text || "");
      const toMe = MY_RE.test(t);
      const toPeer = PEER_RE.test(t);
      if (toPeer && !toMe) return false;   // 只点对方：对方接（契约 §2.5 @me 只 me 接、@daman 只 daman 接）
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

  // 附件落盘（附件 v1，2026-11-03）：把消息里的图片/文档下载到本地，注入里给绝对路径。
  // 下载失败不阻塞文字投递，只补一行失败说明并记 attach-error。
  const ATTACH_DIR = ROOT + "\\data\\bridge-files";
  const safeName = (n) => String(n || "file").replace(/[\\/:*?"<>|\x00-\x1f]/g, "_").trim().slice(0, 80) || "file";
  const fmtSize = (n) => (n >= 1048576 ? (n / 1048576).toFixed(1) + " MB" : n >= 1024 ? (n / 1024).toFixed(1) + " KB" : n + " B");
  const fetchAttachments = async (m) => {
    // 两种落库形态都认：kind=file 的单文件 `file`，和将来多附件的 `attachments` 数组。
    const list = (m.file ? [m.file] : Array.isArray(m.attachments) ? m.attachments : []).slice(0, 5);
    if (!list.length) return [];
    const dir = ATTACH_DIR + "\\" + safeName(m.roomId);
    try { fs.mkdirSync(dir, { recursive: true }); } catch {}
    const key = platKey();
    const lines = [];
    for (const a of list) {
      const name = safeName(a && a.name);
      const local = String((a && a.path) || "");
      const url = String((a && a.url) || "");
      // 同机部署时直接复用服务端已落盘的文件，省一次下载；跨机才走 HTTP。
      if (local && fs.existsSync(local)) {
        const fp = dir + "\\" + m.seq + "-" + name;
        try {
          fs.copyFileSync(local, fp);
          lines.push(`[附件] ${name} (${fmtSize(fs.statSync(fp).size)}) → ${fp}`);
          evt({ action: "attach", seq: m.seq, room: m.roomId, name, bytes: fs.statSync(fp).size, via: "copy" });
          continue;
        } catch {}
      }
      try {
        if (!url.startsWith("/api/files/")) throw new Error("bad url");
        const r = await fetch(base() + url + (key ? "?k=" + encodeURIComponent(key) : ""), { signal: AbortSignal.timeout(15000) });
        if (!r.ok) throw new Error("HTTP " + r.status);
        const buf = Buffer.from(await r.arrayBuffer());
        const fp = dir + "\\" + m.seq + "-" + name;
        fs.writeFileSync(fp, buf);
        lines.push(`[附件] ${name} (${fmtSize(buf.length)}) → ${fp}`);
        evt({ action: "attach", seq: m.seq, room: m.roomId, name, bytes: buf.length });
      } catch (e) {
        const why = String((e && e.message) ?? e).slice(0, 120);
        lines.push(`[附件] ${name} 下载失败（${why}）`);
        evt({ action: "attach-error", seq: m.seq, room: m.roomId, name, message: why });
      }
    }
    return lines;
  };

  // 2026-11-03 16:41 纯确认防刷屏：房间照常显示，只是连续两条纯 ack 不再互相唤醒。
  // 前一条本身就是 ack 时跳过本条的注入（仍算已处理并 ack），避免「收到/好的」ping-pong。
  const ACK_RE = /^[好的收到明白了了解行嗯嘞ok，,。.!！~～\s]{1,12}$/i;
  const ackState = new Map();
  const isAckMsg = (m) => { const t = String(m.text || "").trim(); return t.length > 0 && t.length <= 12 && ACK_RE.test(t); };

  // 歧义判定（内联 v2 claim-lib）：只有用户(you)发的、指向不明（两人都点名或都不点名）的普通聊天才走 claim。
  const isAmbiguousMsg = (m) => {
    // 2026-11-05：以前只认 who==="you"，受邀成员的「不点名」消息不进入 claim，
    // 两个 agent 都投就会双回。现在任何非 agent 的真人说话都走 claim。
    if (!m || m.who === WHO || m.who === PEER) return false;
    if (m.kind === "task") return false;   // 任务卡不参与 claim
    const t = String(m.text || "");
    return MY_RE.test(t) === PEER_RE.test(t);   // 都点名 或 都不点名
  };

  // stall 判定（内联 v2 stall-lib）：注入后 180s 该房仍没有 who=daman 且 seq 更大的回话 → 报警。
  const needsStall = (msgs, roomId, afterSeq, myWho = "daman") =>
    !(Array.isArray(msgs) ? msgs : []).some((x) => x && x.roomId === roomId && x.who === myWho && Number(x.seq) > Number(afterSeq));

  const claimedSeq = new Set();
  // ack 结果统一处理：消息已处理则游标照推（避免重投）；ack 失败（5xx/网络）记 pendingAck 下 tick 补，
  // 4xx（服务端拒绝）不再重试（重试必再被拒）。
  const ackSeq = async (seq, roomId) => {
    const a = await ackPost(seq, roomId);
    if (a.ok) { pendingAcks.delete(PA(roomId)); return; }
    if (a.status >= 400 && a.status < 500) {
      pendingAcks.delete(PA(roomId));
      evtOnce("ack-rej:" + seq, { action: "error", where: "ack", seq, status: a.status });
      return;
    }
    pendingAcks.set(PA(roomId), { seq, roomId: roomId || null });   // 累积语义：下 tick 重试推到这个 seq 即覆盖更早的失败
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
    // ① 纯确认 ping-pong 抑制（v2 保留）：连续两条纯 ack 不互相唤醒；算已处理（上层 ack）。
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
    // ② 已有 claim 裁决（可能是我 hold 期间对方接管了，契约 §2.5/§3.6）：对方赢 → skip(claimed-peer) 并 ack。
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
    // ③ agent 存在性（hold）。P2 灰度：这间房配了专属会话就用它，否则仍用共用会话。
    const sid = sessionFor(m.roomId);
    const ag = await liveAgent(sid);
    log(`agent seq=${m.seq} -> ${ag ? (typeof ag.followup) : "none"} phase=${ag ? String(ag.phase?.kind ?? ag.phase) : "-"}`);
    if (!ag || typeof ag.followup !== "function") { log("no agent"); return false; }
    // ④ busy / ⑤ cooldown（hold，不 ack）。注意：claim POST 必须在这两步之后（v2 顺序保留）——
    //    busy 方不占 claim，否则 token 卡住会让 60s 接管机制失效。
    //    BUG#2：hold 同时记账（哪个会话、哪条 seq、卡在什么 phase），卡死自愈靠这份账判断「有真 work 在等」。
    const phKind = phaseKind(ag);
    if (isBusyKind(phKind)) {
      noteHold(holdBook, sid, m.seq, phKind);
      holdClock.mark("busy:" + m.seq);
      evtOnce("hold:" + m.seq + ":busy:" + phKind, { action: "hold", seq: m.seq, room: m.roomId, kind: m.kind, reason: "busy:" + phKind, sessionId: sid });
      // A11（2026-11-08）：忙太久就把这条先放下（消息还在房历史里，不会丢），ack 推游标；
      // 否则一个长任务期间的每条消息都要冻到任务结束，恢复后一口气全灌进来。
      if (holdClock.expired("busy:" + m.seq)) {
        const waitedMin = Math.max(1, Math.round((Date.now() - holdClock.at("busy:" + m.seq)) / 60000));
        evt({ action: "hold-give-up", seq: m.seq, room: m.roomId, reason: "busy:" + phKind, waitedMin });
        log(`busy(${phKind}) give up #${m.seq} after ${waitedMin}min`);
        await postSay(m.roomId, `我这边还在忙（${phKind}），消息 #${m.seq} 等了 ${waitedMin} 分钟，先放下了；忙完我用 read 回看房间历史。`);
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
    // ⑥ claim 声明（用户聊天，契约 §2.5/§3.5）：歧义（都点名/都不点名）或接管时 POST；直接点名我不占声明。
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
        // 赢：也写 claim 事件（v3 两端对称，契约 §3.5）。
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
    // ⑧ 组装并注入。
    const head = `[ds-teams · ${m.roomId}${room?.title ? " · " + room.title : ""}] ${m.who} · ${m.kind} · #${m.seq}`;
    const floodNote = opts.floodSkipped > 0 ? `（该房间本次共 ${opts.floodSkipped + 1} 条新消息，已跳过前 ${opts.floodSkipped} 条，只送最新一条；需要细节用 read 看房间）` : "";
    const takeoverNote = opts.takeover ? `（接管：该消息被 ${PEER_NAME} hold 超过 60 秒，由 ${MY_NAME} 接手）` : "";
    const hint = `（回复：node "<repo>\\say.mjs" say ${m.roomId} "..." --who daman；你是大满）`;
    const isTask = m.kind === "task";
    const taskId = String(m.taskId || "").trim() || ("auto-" + m.seq);
    const directive = isTask
      ? [`【任务卡】taskId=${taskId} assignee=${m.assignee || "daman"} deadline=${m.deadlineAt || "未写"}`,
         `① 先 claim：node "<repo>\\dispatch.mjs" claim ${m.roomId} ${taskId} --seq ${m.seq}`,
         "② claim 返回 duplicate=true（退出码 3）就无视，别再拉人。",
         `③ 成功则回一句「收到 ${taskId}，拆 s1/s2」，再 spawn_teammate（prompt 带 roomId/taskId/验收标准/子 taskId=${taskId}.s1），并 set spawned --sub ${taskId}.s1 ...`,
         "④ 子任务 progress/report 用子 taskId，结束时 review 并按需关房。"]
      : [];
    // 会议室（docs/meeting-room.md §4.4）：report 卡注入时加评审任务头——只提实质问题，没意见就沉默。
    const reviewDirective = m.kind === "report" && m.who === PEER
      ? [`【评审任务】${PEER_NAME} 交了一份报告，你是指定评审方。`,
         "评审规则：只提实质问题（事实错/逻辑断/证据缺/风险漏）；没有实质问题就不要输出任何内容（沉默即通过，系统会自动收敛）。",
         `有意见时交卡：node "<repo>\\say.mjs" review ${m.roomId} --verdict issues "结论/证据/改法" --who daman`,
         "评审通过时交卡：node \"<repo>\\say.mjs\" review " + m.roomId + " --verdict pass \"评审通过\" --who daman",
         "禁止客套、禁止复述报告内容。报告全文："]
      : [];
    // 投递失败也要有证据（D3 复核①）：以前只进宿主 logger，bridge-daman.log 里看不到。
    // 契约 §6：followup 抛错 → settle ERROR（保留原 error 事件）。
    const failEvt = (e) => {
      evt({ action: "error", seq: m.seq, room: m.roomId, kind: m.kind, message: String((e && e.message) ?? e).slice(0, 300) });
      if (wakeId) settleWake(wakeId, "ERROR");
    };
    const attachLines = await fetchAttachments(m);
    const quoteLine = m.quote ? `↪ 引用 #${m.quote.seq} ${m.quote.who}：${String(m.quote.text || "").slice(0, 200)}` : "";
    try {
      const p = ag.followup(createUserMessage({
        content: [{ type: "text", text: [head, floodNote, takeoverNote, ...directive, ...reviewDirective, quoteLine, String(m.text || "").slice(0, 1200), ...attachLines, hint].filter(Boolean).join("\n") }],
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
    // 2026-11-03 19:28 静默卡死报警：注入后 3 分钟我在该房间仍没回过任何话，
    // 就在房间里留一句「我卡住了」，让对方别一直等（归档/排队/回合卡住时的兜底）。
    const stallSeq = m.seq, stallRoom = m.roomId;
    later(run("stall", async () => {
      if (disposed || globalThis[GEN_KEY] !== gen) return;
      const st = await pullState(stallSeq);
      if (!st.ok) return;
      if (!needsStall(st.s.messages || [], stallRoom, stallSeq, "daman")) return;
      await postSay(stallRoom, `我 #${stallSeq} 注入后 3 分钟没回过话，可能卡在排队或归档了。别一直等，先接或换个时刻再喊。`);
      evt({ action: "stall", seq: stallSeq, room: stallRoom });
      log(`stall reported #${stallSeq}`);
    }), 180000);
    if (wakeId) startWakeWatch(wakeId, m.seq, m.roomId);   // 契约 §6：注入成功 → 启动 outcome watcher
    return true;
  };

  // promote 之后叫醒我：这一步必须在回合里做 spawn，桥接只负责提醒，不自己拉人。
  // A2（2026-11-08）：返回值区分「送到了 / 可以马上重试 / 被闸门拦下」——
  // 以前送没送到都吞掉，而 nudgedAt 在取出时已经打上，提醒一丢就要干等 5 分钟 TTL，
  // claimed 卡到 15 分钟看门狗上直接变 timeout。现在 retry 时调用方立刻 clearNudge，15 秒后重试。
  const nudgePromoted = async (keys) => {
    try {
      const ag = await liveAgent(meSessionId());
      if (!ag || typeof ag.followup !== "function") { log("promote nudge: no agent"); return "retry"; }
      const ph = ag.phase;
      const kind = typeof ph === "object" && ph ? String(ph.kind ?? "") : String(ph ?? "");
      if (kind && kind !== "idle" && kind !== "waiting" && kind !== "completed") { log("promote nudge hold phase=" + kind); return "retry"; }
      // 契约 §6：promote nudge 也是唤醒（一次 followup = 一个 turn），先过闸再注入（seq:-1、kind:"nudge"）。
      const adm = await wakeAdmit(-1, "", "nudge");
      if (!adm.ok) {
        evtOnce("wake-admit-fail:nudge", { action: "wake-admit-fail", seq: -1, kind: "nudge", status: adm.status, message: adm.error });
        log("promote nudge admit failed" + (adm.status ? ` status=${adm.status}` : " (net)"));
        return adm.status ? "blocked" : "retry";
      }
      const am = adm.adm || {};
      if (am.ok === false || am.allow === false) {
        const reason = String(am.reason || (am.ok === false ? "REJECTED" : "COUNT"));
        evt({ action: "wake-blocked", reason, seq: -1, room: "", kind: "nudge", who: WHO });
        log("promote nudge blocked reason=" + reason);
        return "blocked";
      }
      if (!am.allow) {
        evtOnce("wake-admit-fail:nudge", { action: "wake-admit-fail", seq: -1, kind: "nudge", message: "bad admit body" });
        log("promote nudge admit bad body");
        return "blocked";
      }
      const wakeId = String(am.wakeId || "");
      const failSettle = (e) => {
        log("promote nudge followup failed " + String((e && e.message) ?? e));
        if (wakeId) settleWake(wakeId, "ERROR");
      };
      try {
        const p = ag.followup(createUserMessage({
          content: [{ type: "text", text: `【dispatch】名额已空出，以下排队任务已 promote 为 claimed：${keys.join(", ")}。给它们各 spawn 一个 teammate，成功后 dispatch.mjs set <roomId> <taskId> spawned --sub <taskId>.s1 --mate <mate名>。` }],
          source: { kind: "plugin:ds-teams" },
        }));
        if (p && typeof p.catch === "function") p.catch(failSettle);
      } catch (e) { failSettle(e); return "retry"; }
      evt({ action: "nudge", count: keys.length, wakeId, seq: -1 });
      if (wakeId) startWakeWatch(wakeId, -1, "");
      log("promote nudge -> " + keys.length);
      return "sent";
    } catch (e) { log("promote nudge failed " + String((e && e.message) ?? e)); return "retry"; }
  };

  // 接管扫描（契约 §2.5/§3.6）：每 15s 一次，找「只点对方、>60s、对方没 ack 也没回话、无 claim 裁决」的 say，
  // 由我 claim 接手注入。每轮最多接管一条。被 hold 的消息我方当时按 not-relevant 已 ack，游标不受影响。
  const takeoverScan = async (rooms) => {
    if (Date.now() - lastTakeoverScan < 15000) return;
    lastTakeoverScan = Date.now();
    try {
      const st = await pullState(0);
      if (!st.ok) return;
      const s = st.s;
      const all = Array.isArray(s.messages) ? s.messages : [];
      const acks = serverAcks(s);
      const recent = all.slice(-200);   // 与 claim 保留数一致，避免扫到天边
      for (const m of recent) {
        if (!m || m.recalled || !isSayKind(m) || m.who !== "you") continue;
        const t = String(m.text || "");
        if (!(PEER_RE.test(t) && !MY_RE.test(t))) continue;   // 只点对方（我这边按 not-relevant ack 过的）
        const age = Date.now() - Date.parse(m.ts || 0);
        if (!(age > TAKEOVER_HOLD_MS && age < TAKEOVER_MAX_AGE_MS)) continue;
        if (acks && typeof acks[PEER] === "number" && Number(acks[PEER]) >= Number(m.seq)) continue;   // 对方已 ack=已处理
        const replied = all.some((x) => x.roomId === m.roomId && x.who === PEER && Number(x.seq) > Number(m.seq) && !x.recalled);
        if (replied) continue;   // 对方已回话
        const stc = await claimGet(m.roomId, m.seq);
        if (stc?.winner) continue;   // 已有裁决（谁赢都别动，token 机制不变）
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
  // 本进程里所有活着的 agent（两端桥同进程：daman 主会话 / 各房专属会话 / me 会话都在这里）。
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
  const stuckGuard = async (rooms0) => {
    if (stuckBusy) return;
    if (Date.now() - lastStuckResetAt < STUCK_RETRY_MS) return;
    try {
      const act = liveAgents();
      const busyIds = new Set(act.filter((x) => isBusyKind(x.kind)).map((x) => x.id));
      for (const sid of [...holdBook.keys()]) if (!busyIds.has(sid)) holdBook.delete(sid);
      if (!busyIds.size) return;
      const logT = lastSeqTimeFromLog(EVENT_LOG);
      const openIds = new Set((rooms0 || []).filter((r) => !r.status || r.status === "open").map((r) => String(r.id)));
      const rooms = [];
      for (const [rid, sid] of roomSessions().byRoom) if (busyIds.has(sid) && openIds.has(String(rid))) rooms.push({ rid, sid });
      stuckBusy = true;
      const r = decideStuck({
        now: Date.now(),
        holds: holdBook,
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
    // busy 看门狗（契约 §3.8）：resume/fetch 偶发挂住时 busy 永远为 true，轮询从此静默（23:40 事故）。
    // 15s 强制复位；不取消 in-flight，其 finally 再置 busy=false 无害（与 v2 语义一致）。
    if (busy) {
      if (Date.now() - busySince < 15000) return;
      log("busy watchdog reset after " + Math.round((Date.now() - busySince) / 1000) + "s");
      evtOnce("watchdog", { action: "watchdog", afterMs: Date.now() - busySince });
      busy = false;
    }
    // 退避门控（契约 §3.6）：固定 2s interval 驱动 + nextAllowedAt 门控实现 2→4→8…30s。
    if (Date.now() < nextAllowedAt) return;
    busy = true;
    busySince = Date.now();
    try {
      // ① 心跳（契约 §3.1：每 tick 先发）。
      const hb = await heartbeat();
      if (!hb.ok) evtOnce("hb:" + (hb.status || "net"), { action: "error", where: "heartbeat", status: hb.status, message: hb.error });
      // ② 拉状态（契约 §3.2）。
      const st = await pullState(cursor ?? 0);
      if (!st.ok) evtOnce("state:" + (st.status || "net"), { action: "error", where: "state", status: st.status, message: st.error });
      // ③ 退避记账（契约 §3.6）：心跳失败/5xx 连续 3 tick → 指数退避；恢复回 2s。
      if (hb.ok && st.ok) {
        if (failStreak) log("poll recovered, back to 2s");
        failStreak = 0;
        pollDelay = 2000;
      } else {
        failStreak += 1;
        if (failStreak >= 3) {
          pollDelay = Math.min(30000, 2000 * Math.pow(2, failStreak - 2));   // 3→4s, 4→8s, 5→16s, 6→32s→30s 封顶
          evtOnce("backoff:" + failStreak, { action: "error", where: "backoff", streak: failStreak, pollDelayMs: pollDelay, hbOk: hb.ok, stOk: st.ok });
        }
      }
      if (!st.ok || !st.s) return;
      const s = st.s;
      const msgs = Array.isArray(s.messages) ? s.messages : [];
      const rooms = Array.isArray(s.rooms) ? s.rooms : [];
      // ④ 补 ack（上次 ack 5xx/网络失败的重试；按房分别记，累积语义，推至该房最新 seq 即覆盖更早的）。
      for (const [k, p] of [...pendingAcks]) {
        const a = await ackPost(p.seq, p.roomId);
        if (a.ok || (a.status >= 400 && a.status < 500)) pendingAcks.delete(k);
      }
      // ⑤ 游标初始化（首次）：文件 → 服务端 ack 游标（防御性，本地文件可能因崩溃落后）→ 冷启动采用最新并 ack 一次。
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
          const a = await ackPost(cursor);   // 声明历史已处理（幂等累积）
          if (!a.ok && !(a.status >= 400 && a.status < 500)) pendingAcks.set("-", { seq: cursor, roomId: null });
        } else {
          evt({ action: "cursor-restore", cursor, serverSeq: Number(s.seq || 0) });
        }
        saveCursor(cursor);
        return;
      }
      // ⑥ 台账维护（大满端，每 15s）：超时标 timeout；名额空出把 queued 提升为 claimed 并提醒我 spawn。
      if (Date.now() - lastMaint > 15000) {
        lastMaint = Date.now();
        try {
          const wd = watchdog();
          if (wd.length) log("watchdog timeout " + wd.map((x) => x.key + "(" + x.reason + ")").join(", "));
          const promoted = promote();
          // T2（2026-11-07）：promote 只管队列状态；已 claimed 但没人去 spawn 的同样要提醒，
          // 否则那条提醒一旦丢了，任务就一直挂着直到看门狗判 timeout。
          // A12（2026-11-08）：带上本端名，接管给对端的卡不提醒我（否则两端各跑一遍）。
          const waiting = claimedAwaitingSpawn(WHO);
          const nudges = Array.from(new Set([...promoted, ...waiting]));
          if (nudges.length) {
            log("promote " + nudges.join(", ") + (waiting.length ? " (await-spawn " + waiting.length + ")" : ""));
            const r = await nudgePromoted(nudges);
            // A2（2026-11-08）：提醒没送到就立刻还账，下一轮重试；闸门明确拒绝才按 TTL 等。
            if (r !== "sent") {
              if (r === "retry") for (const k of nudges) clearNudge(k);
              log("nudge " + r + ": " + nudges.join(", "));
            }
          }
        } catch (e) { log("maint failed " + String((e && e.message) ?? e)); }
      }
      // ⑦ 逐条处理 pending：flood(只 say) → relevant → ack-kind → 白名单 → deliver。
      //    since 取所有房低水位（多取几条无害，逐房 seq 过滤会挡掉重复）。
      //    注入成功/判定跳过 → 推该房游标 + ack；hold → 只冻结该房（heldRooms），别的房继续。
      const pending = msgs.filter((m) => Number(m.seq ?? -1) > cursor).sort((a, b) => Number(a.seq) - Number(b.seq));
      if (pending.length) {
        // flood 统计（契约 §3.3：只对 say；卡片永不折叠）。
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
            // 契约 §3.4：kind=ack 一律不注入，只写日志并 ack。
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
          // BUG#2：只有「真的了结」才销账。hold（done=false）说明这条还在等，账必须留着——卡死判据要靠它。
          if (!done) { heldRooms.add(m.roomId); continue; }   // hold：只冻结这一个房，别的房照走
          settleHold(holdBook, sessionFor(m.roomId), seq);
          roomCursors.set(m.roomId, seq);
          cursor = cursorFloor();
          saveCursor(cursor);
          await ackSeq(seq, m.roomId);   // C4：按房推进
        }
      }
      // ⑧ B1：自动建会话 + 归档房感知；⑨ 接管扫描（每 15s，契约 §2.5）。
      await ensureRoomSessions(rooms);
      await archiveGoneRooms(rooms);
      await takeoverScan(rooms);
      // ⑩ BUG#2 自愈：会话 phase 卡死在非 idle ≥180s → 强停那轮、下一条消息自然重投。
      await stuckGuard(rooms);
    } catch (e) {
      // D3 复核②：fetch 超时、state 解析失败这类没有具体 seq，也要留一条线索。
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
  // 卸载探针：effect 清理函数在 fiber 卸载时执行（这才是本版 cordis 的 dispose 语义）
  try {
    ctx.effect(() => {
      log("mounted g" + gen);
      return () => log("unloaded g" + gen + " (effect cleanup ran)");
    }, "ds-teams-bridge.lifecycle");
  } catch {}
  later(run("warm", () => keepWarm()), 2000);
  every(run("warm", () => keepWarm()), 15000);
  later(run("poll", tick), 1000);
  every(run("poll", tick), 2000);
  // 心跳：确认插件真的在被调度（每 60 秒一行，便于在不看消息时判断是否加载）
  every(run("beat", () => { log("alive g" + gen + " seq=" + (cursor ?? "?") + " agents=" + (agents() ? "yes" : "no")); }), 60000);
  // 被新一代实例取代后自行熄火（双保险：即使 effect 清理没跑到也不会重复轮询/重复投递）
  every(() => {
    if (disposed || globalThis[GEN_KEY] === gen) return;
    disposed = true;
    try { finishWake("NO_REPLY"); } catch {}   // 熄火前把未回填的 wake 收口
    for (const t of timers) { try { clearTimeout(t); clearInterval(t); } catch {} }
    timers.clear();
    log("superseded by g" + globalThis[GEN_KEY] + ", timers cleared");
  }, 5000);

  // 本版 cordis 不 emit 用户可见的 dispose 事件；这里保留监听只为兼容有该事件的构建。
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

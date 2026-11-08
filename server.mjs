// ds-teams · 三人管理员多房间服务（零依赖，127.0.0.1:8792）
// 一个 JSON 文件存全部数据：房间、消息、报告。删除=标记 closed，不删数据。
import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createProjectSpace } from "./projects.mjs";
import { acceptTask, controlCounts, counts as dispatchCounts, createTask, listTasks, promote as dispatchPromote, readLedger as dispatchReadLedger, rejectTask, watchdog as dispatchWatchdog, writeLedger as dispatchWriteLedger } from "./dispatch-lib.mjs";
import { buildHealth } from "./scripts/health-lib.mjs";
import { applyTakeover, decideTakeovers } from "./takeover-lib.mjs";
import { ACTIVE, AUTH, BRIDGE_ENDS, CAP, CLOSED, LIMITS, MEETING, MESSAGE_KINDS, MESSAGE_KIND_SET, MEETING_VERDICT_SET, OWNER, ROLES, SUBSTANTIVE_KINDS, WAKE_OUTCOMES, WAKE_OUTCOME_SET } from "./protocol.mjs";
import { admit as wakeAdmit, bridgeWake, getState as wakeGetState, readState as wakeReadState, settle as wakeSettle, sweepStale as wakeSweepStale } from "./wake-lib.mjs";
import { abortRoom as meetingAbortRoom, list as meetingList, onSilence as meetingOnSilence, open as meetingOpen, respond as meetingRespond } from "./meeting-lib.mjs";
import { patchSettings, readSettings } from "./settings-lib.mjs";
import { tail as opsTail, warn as opsWarn } from "./ops-lib.mjs";
import { CAPS, can, canActOn, capabilitiesOf, describe as describeAccess, effectiveRole, needsSuperConfirm } from "./access-lib.mjs";
import { STATUS as INVITE_STATUS, bindInvite, blacklistEntry, canClaim, isBlacklisted, killInvitesOf, newKey, newToken, normalizeName, peopleToPurge, releaseInvite, resolveSeat, scrubRoomForArchive } from "./invites-lib.mjs";
// 会话与密码：浏览器不再持有成员 key，改成登录一次拿不透明 cookie（见 session-lib.mjs 顶部口径）。
import * as SESSION from "./session-lib.mjs";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
// 临时台账实例（DS_TEAMS_LEDGER，测试/演练用）默认自己开一份数据目录：
// 以前它们和主实例共用 data/，后写盘的那个会把 sessions.json / rooms.json 覆盖回去，
// 表现就是「刚刚还是群主，刷新一下说我不是群主了」「消息发出去不见了」。
const DATA = process.env.DS_TEAMS_DATA_DIR
  || (process.env.DS_TEAMS_LEDGER
    ? path.join(process.env.TEMP || process.env.TMP || ROOT, `ds-teams-ledger-${process.pid}`)
    : path.join(ROOT, "data"));
const DB = path.join(DATA, "rooms.json");
const ARCHIVE = path.join(DATA, "archive");
const FILES = path.join(DATA, "files");
const MEMORY = path.join(DATA, "memory");                    // v3 房间记忆
const MEMORY_ARCHIVE = path.join(ARCHIVE, "memory");          // v3 归档记忆
const PERSONAS_FILE = path.join(DATA, "personas.json");       // v3 人设
const KEYFILE = path.join(DATA, "key.txt");
const TUNNEL_META = path.join(DATA, "tunnel.json");   // scripts/tunnel.mjs 写的现状快照
const UI = path.join(ROOT, "ui.html");
const CONTROL_UI = path.join(ROOT, "control.html");
const PORT = Number(process.env.DS_TEAMS_PORT || 8792);

fs.mkdirSync(DATA, { recursive: true });
fs.mkdirSync(ARCHIVE, { recursive: true });
fs.mkdirSync(FILES, { recursive: true });
fs.mkdirSync(MEMORY, { recursive: true });
fs.mkdirSync(MEMORY_ARCHIVE, { recursive: true });
if (!fs.existsSync(KEYFILE)) fs.writeFileSync(KEYFILE, crypto.randomBytes(4).toString("hex"));
const KEY = fs.readFileSync(KEYFILE, "utf8").trim();
const FLOOR_MS = 90_000;	// 话筒租约：90 秒无发言自动回收
const MAX_FILE = 25 * 1024 * 1024; // 单个附件上限 25MB
const ALLOW_EXT = new Set([".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp",
  ".docx", ".xlsx", ".pptx", ".pdf", ".txt", ".md", ".csv", ".json", ".zip"]);
const DECLARED_BY_ALLOWED = ROLES.filter((id) => id !== OWNER);
// 写消息类 action 允许显式声明的 kind；缺省时沿用各 action 的历史默认。
// file/claim/close 也纳入统一校验：file 只接受 file，claim/close 不接受任何消息 kind。
const ACTION_KINDS = Object.freeze({
  say: Object.freeze(["text", "ack"]),
  task: Object.freeze(["task"]),
  progress: Object.freeze(["progress"]),
  report: Object.freeze(["report"]),
  review: Object.freeze(["review"]),
  confirm: Object.freeze(["confirm"]),
  file: Object.freeze(["file"]),
  claim: Object.freeze([]),
  close: Object.freeze([]),
});

// 隧道现状：URL + 启动时间 + 进程是否还活着。读失败算「没有隧道」，不抛错。
function pidAlive(pid) { try { process.kill(Number(pid), 0); return true; } catch { return false; } }
function tunnelSnapshot() {
  let m = null;
  try { m = JSON.parse(fs.readFileSync(TUNNEL_META, "utf8")); } catch {}
  if (!m || !m.url) {
    // 兼容旧数据：只有裸 URL 文件、没有快照时，照样显示，但标成「可能已失效」。
    let legacy = "";
    try { legacy = fs.readFileSync(path.join(DATA, "tunnel-url.txt"), "utf8").trim(); } catch {}
    if (/^https?:\/\//.test(legacy)) return { ok: true, hasTunnel: true, url: legacy, link: KEY ? `${legacy}/?k=${KEY}` : legacy, pid: 0, alive: false, startedAt: 0, ageMs: 0, hint: "这是旧 URL 文件里的记录，没有进程/时间；重启过就要重跑 tunnel.mjs" };
    return { ok: true, hasTunnel: false, alive: false, hint: "跑 node scripts/tunnel.mjs 起隧道" };
  }
  return {
    ok: true, hasTunnel: true, url: m.url, link: m.link || m.url,
    pid: m.pid || 0, alive: !!(m.pid && pidAlive(m.pid)),
    startedAt: m.startedAt || 0, ageMs: m.startedAt ? Date.now() - m.startedAt : 0,
    hint: m.pid && !pidAlive(m.pid) ? "记录还在，但进程已退出：进度条那条 URL 已经失效" : "",
  };
}

const ENTRY_HTML = `<!doctype html><meta charset=utf-8><title>ds-teams 入口</title>
<style>
:root{color-scheme:dark}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0b0e14;color:#e6edf3;font:15px/1.6 system-ui,"Microsoft YaHei",sans-serif}
.card{width:min(560px,92vw);background:#121722;border:1px solid #1f2937;border-radius:14px;padding:26px 28px}
h1{font-size:17px;margin:0 0 4px;font-weight:600}
.sub{color:#8b98a9;font-size:13px;margin-bottom:18px}
.row{background:#0d121b;border:1px solid #1f2937;border-radius:10px;padding:12px 14px;margin-bottom:12px;word-break:break-all}
a{color:#6cb6ff;text-decoration:none}
.pill{display:inline-flex;align-items:center;gap:6px;font-size:12px;padding:2px 9px;border-radius:999px;background:#1b2432;color:#9fb0c6}
.dot{width:7px;height:7px;border-radius:50%;background:#3fb950}
.dot.bad{background:#f85149}
button{font:inherit;color:#dbe6f3;background:#1b2432;border:1px solid #2a3646;border-radius:8px;padding:6px 12px;cursor:pointer}
.mut{color:#8b98a9;font-size:12px;margin-top:14px}
</style>
<div class="card">
  <h1>ds-teams 入口</h1>
  <div class="sub">隧道 URL 重启会变，这个页面永远指向当前那条。</div>
  <div id="status" class="row">读状态中…</div>
  <div id="link" class="row" hidden></div>
  <div><button id="copy">复制链接</button> <button id="reload">重新读</button></div>
  <div class="mut">本机固定地址：<code>127.0.0.1:8792/entry</code>；刚重启隧道就打 <code>node scripts/tunnel.mjs</code>。</div>
</div>
<script>
const $=s=>document.querySelector(s);
function ago(ms){ if(!ms) return "未知"; const s=Math.round(ms/1000); const h=Math.floor(s/3600), m=Math.floor(s%3600/60); return h?h+" 小时 "+m+" 分":m?m+" 分钟":s+" 秒"; }
async function load(){
  try{
    const r=await fetch("/api/tunnel",{cache:"no-store"}); const j=await r.json();
    if(!j.hasTunnel){ $("#status").innerHTML='<span class="pill"><span class="dot bad"></span>没有隧道记录</span><br>'+(j.hint||""); $("#link").hidden=true; return; }
    $("#status").innerHTML='<span class="pill"><span class="dot '+(j.alive?"":"bad")+'"></span>'+(j.alive?"隧道在跑":"进程已退出")+
      '</span> <span class="mut">pid '+j.pid+' · 已跑 '+ago(j.ageMs)+'</span>'+(j.hint?'<br><span class="mut">'+j.hint+'</span>':"");
    $("#link").hidden=false; $("#link").innerHTML='<a href="'+j.link+'" target="_blank">'+j.link+'</a>';
  }catch(e){ $("#status").textContent="读状态失败："+e.message; }
}
$("#copy").onclick=()=>{ const a=$("#link a"); if(!a) return; navigator.clipboard.writeText(a.href).then(()=>{$("#copy").textContent="已复制"; setTimeout(()=>$("#copy").textContent="复制链接",1200)}); };
$("#reload").onclick=load;
load(); setInterval(load, 5000);
</script>`;


function kindError(action, b) {
  const raw = b && b.kind;
  if (raw === undefined || raw === null || !String(raw).trim()) return null;
  const given = String(raw).trim();
  if (!MESSAGE_KIND_SET.has(given)) {
    return `unknown kind: ${given}; allowed: ${MESSAGE_KINDS.join(", ")}`;
  }
  const allowed = ACTION_KINDS[action] || [];
  if (!allowed.includes(given)) {
    return `kind ${given} not allowed for ${action}; allowed: ${allowed.join(", ") || "none"}`;
  }
  return null;
}

// 成员注册表：人（human）和 agent 一视同仁，各有独立 key。
// 老数据在 data/members.json（{you,me,daman} → key），首次启动自动迁移进来。
const PEOPLE_FILE = path.join(DATA, "people.json");
const MEMBER_FILE = path.join(DATA, "members.json");
let PEOPLE = {};
try { PEOPLE = JSON.parse(fs.readFileSync(PEOPLE_FILE, "utf8")); } catch { PEOPLE = {}; }
if (!Object.keys(PEOPLE).length) {
  let MKEYS = {};
  try { MKEYS = JSON.parse(fs.readFileSync(MEMBER_FILE, "utf8")); } catch { MKEYS = {}; }
  const seed = {
    you: { name: "群主", kind: "human", role: "admin" },
    me: { name: "小满", kind: "agent", role: "admin", note: "QQ 端" },
    daman: { name: "大满", kind: "agent", role: "admin", note: "桌面端" },
  };
  for (const id of Object.keys(seed)) {
    PEOPLE[id] = { id, ...seed[id], key: MKEYS[id] || crypto.randomBytes(6).toString("hex"), status: "active", joinedAt: new Date().toISOString() };
  }
}
function savePeople() { writeJsonAtomic(PEOPLE_FILE, JSON.stringify(PEOPLE, null, 2)); }
savePeople();
function personOf(id) { return PEOPLE[id] || null; }
function personKey(id) { const p = personOf(id); return p && p.status === "active" ? p.key : ""; }

/* ---------- 系统级身份：谁是超级群主 ---------- */
// 权限 v4（2026-11-05 群主定）：系统只有一个超级群主，第一位注册的真人。
// 老世界没有这个概念，群主恒为 you —— 迁移时把 you 认成超管，行为与旧版一致。
const SYSTEM_FILE = path.join(DATA, "system.json");
function loadSystem() {
  try { const j = JSON.parse(fs.readFileSync(SYSTEM_FILE, "utf8")); return j && typeof j === "object" ? j : {}; }
  catch { return {}; }
}
let SYSTEM = loadSystem();
function saveSystem() { writeJsonAtomic(SYSTEM_FILE, JSON.stringify(SYSTEM, null, 2)); }
if (!SYSTEM.superId) {
  SYSTEM.superId = OWNER;
  SYSTEM.claimedAt = new Date().toISOString();
  SYSTEM.note = "迁移：权限 v4 上线前的唯一群主（you）";
  saveSystem();
}
function superId() { return SYSTEM.superId; }
function sysRoleOf(id) { return id && id === SYSTEM.superId ? "super" : "normal"; }
/** 把 people.json 的扁平记录包成 access-lib 认的人：额外带 sysRole。 */
function actorOf(id) {
  const p = personOf(String(id || ""));
  if (!p || p.status !== "active") return null;
  return { ...p, sysRole: sysRoleOf(p.id) };
}

/* ---------- 会话（浏览器登录态）与初始注册码 ----------
 * 2026-11-05 群主拍板：堵掉「谁拿到隧道链接谁就是群主」——浏览器不再拿成员 key，
 * 改成 super 用初始注册码注册一次（码只在主机 data/setup-code.txt 与启动日志里），
 * 之后每个人用自己的密码登录，拿到 HttpOnly 的不透明会话 cookie。
 * 成员 key 继续保留给 CLI / 桥接 / 测试，不发给浏览器。
 */
const SESSIONS_FILE = path.join(DATA, "sessions.json");
const SETUP_FILE = path.join(DATA, "setup-code.txt");
let SESSIONS = {};
try {
  const j = JSON.parse(fs.readFileSync(SESSIONS_FILE, "utf8"));
  if (j && typeof j === "object") SESSIONS = j;
} catch {}
let sessionsDirty = false;
let sessionsSavedAt = 0;
function saveSessions(force = false) {
  const now = Date.now();
  if (!force && !sessionsDirty) return;
  if (!force && now - sessionsSavedAt < 5000) return; // 每条请求都 touch，攒 5 秒写一次盘
  try { writeJsonAtomic(SESSIONS_FILE, JSON.stringify(SESSIONS, null, 2)); sessionsDirty = false; sessionsSavedAt = now; } catch {}
}
function pruneSessions() {
  const now = Date.now();
  let changed = false;
  for (const [h, s] of Object.entries(SESSIONS)) {
    if (!SESSION.sessionAlive(s, now)) { delete SESSIONS[h]; changed = true; }
  }
  if (changed) { sessionsDirty = true; saveSessions(true); }
  return changed;
}
/** 踢人 / 改密码 / 停用身份：直接吊销这个人所有会话。 */
function revokeSessionsOf(id, why = "") {
  let n = 0;
  for (const [h, s] of Object.entries(SESSIONS)) {
    if (s && s.who === id) { delete SESSIONS[h]; n++; }
  }
  if (n) { sessionsDirty = true; saveSessions(true); opsWarn("session-revoke", { who: id, count: n, why }); }
  return n;
}
function createSession(who, req) {
  const token = SESSION.newToken();
  SESSIONS[SESSION.tokenHash(token)] = {
    who,
    at: Date.now(),
    seen: Date.now(),
    ua: String((req && req.headers["user-agent"]) || "").slice(0, 120),
  };
  sessionsDirty = true;
  saveSessions(true);
  return token;
}
/** 从 cookie 认人：只读，不改库；session 失效即当没登录。 */
function sessionOf(req) {
  if (!req) return null;
  const token = SESSION.parseCookies(req.headers.cookie || "")[SESSION.COOKIE_NAME];
  if (!token) return null;
  const hash = SESSION.tokenHash(token);
  const s = SESSIONS[hash];
  if (!s) return null;
  if (!SESSION.sessionAlive(s)) { delete SESSIONS[hash]; sessionsDirty = true; saveSessions(true); return null; }
  const p = personOf(s.who);
  if (!p || p.status !== "active") { delete SESSIONS[hash]; sessionsDirty = true; saveSessions(true); return null; }
  s.seen = Date.now();
  sessionsDirty = true;
  saveSessions();
  return { ...s, id: s.who, hash };
}
const sessionWho = (req) => { const s = sessionOf(req); return s ? s.who : ""; };
/** 请求里到底是谁：先看 session（浏览器），再看 body.who（CLI 显式声明）。 */
function whoOf(body, req) {
  const declared = String((body && (body.who || body.id)) || "").trim();
  return declared || sessionWho(req);
}
if (!fs.existsSync(SETUP_FILE)) {
  fs.writeFileSync(SETUP_FILE, SESSION.newSetupCode() + "\n", { encoding: "utf8", mode: 0o600 });
}
const SETUP_CODE = fs.readFileSync(SETUP_FILE, "utf8").trim();
function setupCodeOk(given) {
  const a = SESSION.normalizeSetupCode(given);
  const b = SESSION.normalizeSetupCode(SETUP_CODE);
  return a.length > 0 && a.length === b.length && SESSION.safeEqual(a, b);
}
pruneSessions();

// 项目空间：data/projects/<id>/，见 docs/project-space-设计.md
const projectSpace = createProjectSpace({
  dataDir: DATA, personOf, personKey,
  // 双真源（DP-7）：任务状态只读 dispatch 台账，项目空间不写 task。
  readLedger: () => JSON.parse(fs.readFileSync(path.join(DATA, "dispatch-state.json"), "utf8")),
});

// 兼容旧代码：成员显示名
const MEMBERS = new Proxy({}, {
  get(_, id) {
    const p = personOf(String(id));
    if (!p || p.status !== "active") return undefined;
    return { name: p.name, role: id === OWNER ? "群主" : (p.role === "admin" ? "管理员" : "成员"), kind: p.kind };
  },
});

// 房间里的身份牌（2026-11-05）：以前成员列表直接拿全局 p.role，
// 于是「在房间里被授予管理员」的人在成员条上仍显示「成员」，等于授权没生效。
// 现在按房间算：超管 > 群主 > 房间 roles.admin > 成员。
function roomRoleOf(id, r) {
  const p = personOf(String(id)) || {};
  if (id === superId() || p.sysRole === "super") return "超管";
  if (r && id === r.owner) return "群主";
  if (r && r.roles && r.roles[id] === "admin") return "管理员";
  return "成员";
}
function roomMembers(r) {
  const list = (r && r.members) || [];
  return list.map((id) => {
    const p = personOf(String(id)) || {};
    // muted 也一起带出去：禁言是「状态变了」，成员条上得看得见，不能只落一条系统卡。
    const muted = Array.isArray(r.muted) && r.muted.map(String).includes(String(id));
    return { id, name: p.name || id, kind: p.kind, role: roomRoleOf(id, r), muted };
  });
}

/**
 * 房间可见性：超管与两个 AI 看全部；真人只能看自己在名单里的房间。
 * 以前 GET /api/rooms 什么都不看身份，谁拿到平台 key 就能翻遍所有房间 —— 受邀者
 * 因此能看到并切换到跟他无关的房间（只挡住了发言）。规格要求：没被拉进来就不该看见。
 */
function canSeeRoom(person, r) {
  if (!person || !r) return false;
  if (person.sysRole === "super" || person.kind === "agent") return true;
  return Array.isArray(r.members) && r.members.includes(person.id);
}

/**
 * 读接口的「看的人是谁」：?who=&key= 或会话 cookie。都不给就是匿名（只能看见空列表）。
 * 给了 key 就必须对得上，免得拿别人的 id 冒充。
 */
function viewerOf(req, url) {
  const q = url && url.searchParams;
  const id = String((q && q.get("who")) || "") || sessionWho(req) || "";
  const person = id ? personOf(id) : null;
  if (!person || person.status !== "active") return null;
  const given = String((q && (q.get("mk") || q.get("key"))) || (req && req.headers["x-member-key"]) || "");
  if (given && person.key && !SESSION.safeEqual(given, person.key)) return null;
  return person;
}

/**
 * 「这个 who 是不是他本人」——两条路，任一成立即可：
 *   1) 浏览器：HttpOnly 会话 cookie 指向同一身份（现在页面只走这条）；
 *   2) CLI / 桥接 / 测试：body.key 或 x-member-key 对得上成员 key。
 * 比较一律走等长 timingSafeEqual，不再用 === 比密钥。
 */
function whoAllowed(who, b, req) {
  const p = personOf(String(who || ""));
  if (!p || p.status !== "active") return false;
  const given = String(b[AUTH.member.body] || (req && req.headers[AUTH.member.header]) || "");
  if (given.length > 0) return SESSION.safeEqual(given, p.key);
  const s = sessionOf(req);
  return !!(s && s.who === p.id);
}

/**
 * 统一鉴权闸门：先认人（key 对得上），再看这个人在这个房间有没有这个能力。
 * 所有写接口都该走这里，不要再各自写 `if (who !== OWNER)`——那是权限漏洞的来源。
 * @returns {{person:object, role:string}|null} null 表示已经回过错误响应
 */
function permit(res, req, body, cap, room) {
  const who = whoOf(body, req);
  const person = actorOf(who);
  if (!person) {
    json(res, 403, { ok: false, error: "unknown or inactive identity" });
    return null;
  }
  if (!whoAllowed(who, body, req)) {
    json(res, 403, { ok: false, error: "identity key mismatch" });
    return null;
  }
  const role = effectiveRole(person, room);
  if (!can(role, cap)) {
    json(res, 403, { ok: false, error: "no permission: " + cap, role, cap });
    return null;
  }
  return { person, role };
}

/** 只管「认人」不管权限的场景（发言、读）用这个。 */
function identify(res, req, body) {
  const who = whoOf(body, req);
  const person = actorOf(who);
  if (!person) {
    json(res, 403, { ok: false, error: "unknown or inactive identity" });
    return null;
  }
  if (!whoAllowed(who, body, req)) {
    json(res, 403, { ok: false, error: "identity key mismatch" });
    return null;
  }
  return { person, role: "owner" };
}

/* ---------- 解散确认闸（规格二.1：小群主解散需超管 5 秒内可否决） ----------
 * 竞态处理（调研 D 段教的）：
 *   1) 同一间房同时只允许一条 pending，第二条直接 409，不排队；
 *   2) 超时和否决同时到达 → 只做一次 CAS（pending → expired/denied/approved），先写者赢；
 *   3) 超时默认「放行」是规格要求的 fail-open，但只在超管确实在线（5 秒内有心跳）时才放；
 *      超管不在线就等于没人能拦，这种情况改成拒绝，别让断线变成自动批准。
 * 实现细节：pending 挂在 room.pendingClose 上，跟房间一起落盘；定时器只在进程内，
 *   重启后 load 时按 deadline 重新结算（见 recoverPendingCloses）。
 */
const CLOSE_CONFIRM_MS = 5000;
const SUPER_HEARTBEAT_MS = 10_000;
const _closeTimers = new Map();

function superOnline() {
  // 两种在线证据：页面在轮询 /api/me（presence），或者桥在打心跳（heartbeat）。
  // 页面不轮询时 presence 会过期，这时退回 heartbeat；两个都没有就认为超管不在线。
  const now = Date.now();
  const pr = db.presence && db.presence[SYSTEM.superId];
  if (pr && now - Number(pr) < SUPER_HEARTBEAT_MS) return true;
  const hb = db.heartbeat && db.heartbeat[SYSTEM.superId];
  return !!hb && (now - Date.parse(hb)) < SUPER_HEARTBEAT_MS;
}

function settleClose(roomId, verdict, by) {
  const r = room(roomId);
  const p = r && r.pendingClose;
  if (!p || p.status !== "pending") return null;
  // 单次 CAS：只有还处于 pending 的那一次调用能把状态改掉。
  p.status = verdict;
  p.settledAt = new Date().toISOString();
  p.settledBy = by || "";
  if (verdict === "denied") {
    push(r.id, "system", "system", `超级群主否决了「${p.byName || p.by}」的解散请求，房间继续`);
  } else {
    if (r.status === "open") {
      r.status = "closed";
      r.closedAt = new Date().toISOString();
      r.closedBy = p.by;
    }
    push(r.id, "system", "system", verdict === "expired"
      ? `解散确认超时，按规则放行：房间已解散（发起人 ${p.byName || p.by}）`
      : `超级群主同意解散，房间已解散（发起人 ${p.byName || p.by}）`);
  }
  flush();
  // 解散/否决都改了房间状态和待确认横幅，页面必须立刻看到（不等 3 秒轮询）。
  metaRoom(r.id, "close-" + verdict);
  metaRooms("close-" + verdict);
  const t = _closeTimers.get(r.id);
  if (t) { clearTimeout(t); _closeTimers.delete(r.id); }
  return p;
}

function requestRoomClose(roomObj, actor) {
  if (roomObj.pendingClose && roomObj.pendingClose.status === "pending") return null; // 已有待确认 → 409
  const req = {
    id: "cc-" + crypto.randomBytes(4).toString("hex"),
    roomId: roomObj.id,
    by: actor.id,
    byName: actor.name || actor.id,
    status: "pending",
    at: new Date().toISOString(),
    deadline: new Date(Date.now() + CLOSE_CONFIRM_MS).toISOString(),
  };
  roomObj.pendingClose = req;
  push(roomObj.id, "system", "system", `${actor.name || actor.id} 请求解散房间，等待超级群主 ${CLOSE_CONFIRM_MS / 1000} 秒内确认（不响应即放行）`);
  save();
  const timer = setTimeout(() => {
    _closeTimers.delete(roomObj.id);
    const p = roomObj.pendingClose;
    if (!p || p.status !== "pending") return;
    if (!superOnline()) {
      settleClose(roomObj.id, "denied", "system:super-offline");
      return;
    }
    settleClose(roomObj.id, "expired", "system:timeout");
  }, CLOSE_CONFIRM_MS);
  if (typeof timer.unref === "function") timer.unref();
  _closeTimers.set(roomObj.id, timer);
  flush();
  return req;
}

// 进程重启后，上次没结算完的 pending 按 deadline 补结算，别让房间永久卡在「等确认」。
function recoverPendingCloses() {
  for (const r of db.rooms) {
    const p = r.pendingClose;
    if (!p || p.status !== "pending") continue;
    if (Date.parse(p.deadline) <= Date.now()) {
      settleClose(r.id, "expired", "system:recovered");
    } else {
      const wait = Math.max(0, Date.parse(p.deadline) - Date.now());
      const timer = setTimeout(() => settleClose(r.id, superOnline() ? "expired" : "denied", "system:timeout"), wait);
      if (typeof timer.unref === "function") timer.unref();
      _closeTimers.set(r.id, timer);
    }
  }
}

/* ---------- v3 人设：data/personas.json（缺失时用内联默认并自动创建） ---------- */
const PERSONAS_DEFAULT = Object.freeze({
  me: { name: "小满", tone: "活泼、直接、短句", channel: "QQ" },
  daman: { name: "大满", tone: "稳、负责、短句", channel: "桌面" },
});
function loadPersonas() {
  try {
    const j = JSON.parse(fs.readFileSync(PERSONAS_FILE, "utf8"));
    if (!j || typeof j !== "object") throw new Error("bad personas");
    const out = {};
    for (const id of BRIDGE_ENDS) out[id] = { ...PERSONAS_DEFAULT[id], ...(j[id] && typeof j[id] === "object" ? j[id] : {}) };
    return out;
  } catch {
    try { fs.writeFileSync(PERSONAS_FILE, JSON.stringify(PERSONAS_DEFAULT, null, 2) + "\n"); } catch {}
    return { me: { ...PERSONAS_DEFAULT.me }, daman: { ...PERSONAS_DEFAULT.daman } };
  }
}
const PERSONAS = loadPersonas();

/* ---------- v3 房间记忆：data/memory/<roomId>.jsonl，每行 {ts,by,text} ---------- */
function memoryPath(id) { return path.join(MEMORY, id + ".jsonl"); }
function memoryArchivePath(id) { return path.join(MEMORY_ARCHIVE, id + ".jsonl"); }
function readMemory(id) {
  for (const fp of [memoryPath(id), memoryArchivePath(id)]) {
    try {
      const lines = fs.readFileSync(fp, "utf8").split(/\r?\n/).filter(Boolean);
      if (!lines.length) continue;
      return lines.map((l) => JSON.parse(l));
    } catch { /* 不存在或坏行：试另一个位置 */ }
  }
  return [];
}
function appendMemory(id, who, text) {
  fs.mkdirSync(MEMORY, { recursive: true });
  fs.appendFileSync(memoryPath(id), JSON.stringify({ ts: new Date().toISOString(), by: who, text: String(text) }) + "\n");
  let lines = [];
  try { lines = fs.readFileSync(memoryPath(id), "utf8").split(/\r?\n/).filter(Boolean); } catch {}
  if (lines.length > LIMITS.memoryMaxEntries) {
    fs.writeFileSync(memoryPath(id), lines.slice(-LIMITS.memoryMaxEntries).join("\n") + "\n");
  }
}
function memorySummary(id) {
  const entries = readMemory(id);
  return { roomId: id, count: entries.length, entries: [...entries].reverse() };
}

function load() {
  try { return JSON.parse(fs.readFileSync(DB, "utf8")); } catch { return { seq: 0, rooms: [], messages: [] }; }
}
let db = load();
// 声明（claim）用独立自增 token：老数据没有这个字段，补 0 起步。
db.claimSeq = Number(db.claimSeq || 0);
// v3：桥接游标（每端已确认的最大 seq）与心跳（每端最近一次 ISO 时间戳）
db.cursors = (db.cursors && typeof db.cursors === "object") ? db.cursors : {};
// C4（2026-11-04）：每房游标。只有一个全局 who 游标时，A 房一条 hold 消息会把游标顶住，
// B 房的消息永远排在它后面不发（head-of-line）。桥在 ack 时带上 roomId，这里按房记；
// 对外 cursor 仍返回"所有 open 房的最小值"，语义是"这个 who 已经追平到哪"，只前进不倒退由桥端保证。
db.roomCursors = (db.roomCursors && typeof db.roomCursors === "object") ? db.roomCursors : {};
db.heartbeat = (db.heartbeat && typeof db.heartbeat === "object") ? db.heartbeat : {};
// 在线状态：老库没有 presence 字段时补空表，注册/登录/轮询都要往里写。
db.presence = (db.presence && typeof db.presence === "object") ? db.presence : {};

/* ---------- 房间元信息实时推送（2026-11-06） ----------
 * 成员进出、设/撤管理员、转让群主、禁言、解散待确认……这些以前只落一条系统卡，
 * 页面全靠 3~6 秒轮询去追，成员条常停在旧快照上。现在改成服务端主动推：
 *   event: meta   { roomId, reason, seq, at }   只推给订了这间房的页面
 *   event: rooms  { reason, seq, at }           推给所有页面（新建/解散/归档/换名单 → 左栏也要变）
 * reason 只给日志和调试看，前端一律「收到就重拉」，不自己猜语义。
 * 谁负责调：每个改动房间的 action 在 flush() 之后调一次 metaRoom(id, reason) 或 metaRooms(reason)。
 * 不挂在 flush() 上——flush 在启动/心跳/游标等处也会调，那是无关的变化，推了就是白刷页面。
 * 同一轮请求里可能连着几次元信息调用（落系统卡 + 改字段），60ms 内压成一帧。 */
const META_DEBOUNCE_MS = 60;
const META_FALLBACK_ROOM = "__pending__";
const metaPending = new Map();          // roomId -> Set(reason)
let metaRoomsPending = new Set();       // 左栏原因集合
let metaTimer = null;
function metaRoom(roomId, reason) {
  const id = String(roomId || "");
  if (!id) return;
  if (!metaPending.has(id)) metaPending.set(id, new Set());
  metaPending.get(id).add(String(reason || ""));
  scheduleMetaPush();
}
function metaRooms(reason) { metaRoomsPending.add(String(reason || "")); scheduleMetaPush(); }
function scheduleMetaPush() {
  if (metaTimer) return;
  metaTimer = setTimeout(() => {
    metaTimer = null;
    const rooms = metaRoomsPending;
    const per = new Map(metaPending);
    metaRoomsPending = new Set();
    metaPending.clear();
    // 名单可能变了（被踢/自己退了）：左栏也要重拉，所以任何 meta 都顺带带一帧 rooms。
    broadcastRooms(Array.from(rooms).join(",") || "meta");
    for (const [id, reasons] of per) broadcastMeta(id, Array.from(reasons).join(","));
  }, META_DEBOUNCE_MS);
}
function broadcastMeta(roomId, reason) {
  const frame = `event: meta\ndata: ${JSON.stringify({ roomId, reason: String(reason || ""), seq: db.seq, at: new Date().toISOString() })}\n\n`;
  for (const c of clients) {
    if (c.room === roomId || c.room === "*") { try { c.res.write(frame); } catch {} }
  }
}
function broadcastRooms(reason) {
  const frame = `event: rooms\ndata: ${JSON.stringify({ reason: String(reason || ""), seq: db.seq, at: new Date().toISOString() })}\n\n`;
  for (const c of clients) { try { c.res.write(frame); } catch {} }
}

let saveTimer = null;

// 原子落盘：writeFileSync 直接写目标文件是「先截断再写」，写到一半被别人读到就是空文件/半截 JSON。
// 测试并行跑（node --test 每个文件一个进程）时真出现过 JSON.parse of 空串的偶发失败。
// 改成「写临时文件 → rename 覆盖」，rename 在同一分区上是原子的，读到的永远是完整版本。
function writeJsonAtomic(file, value) {
  const tmp = file + ".tmp-" + process.pid;
  try {
    fs.writeFileSync(tmp, value);
    fs.renameSync(tmp, file);
  } catch {
    try { fs.unlinkSync(tmp); } catch {}
  }
}

function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    writeJsonAtomic(DB, JSON.stringify(db));
  }, 200);
}
function flush() { if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; } writeJsonAtomic(DB, JSON.stringify(db)); }

// C4：本端低水位。每个 open 房各自有游标（桥 ack 时带 roomId），对外回的 cursor 取所有 open 房的
// 最小值；没有 per-room 记录的房回落老的全局游标（老桥/冷启动仍兼容）。
function cursorFloor(who) {
  const legacy = Number(db.cursors[who] || 0);
  const rc = db.roomCursors[who];
  if (!rc || typeof rc !== "object") return legacy;
  const live = db.rooms.filter((r) => r.status === "open").map((r) => r.id);
  if (!live.length) return legacy;
  let low = Infinity;
  for (const id of live) {
    const v = Number(rc[id]);
    low = Math.min(low, Number.isFinite(v) && v > 0 ? v : legacy);
  }
  return Number.isFinite(low) ? low : legacy;
}
function roomCursorsSnapshot() {
  const out = {};
  for (const [who, m] of Object.entries(db.roomCursors || {})) {
    if (!m || typeof m !== "object") continue;
    out[who] = { ...m };
  }
  return out;
}

// 迁移（权限 v4 之前）：owner 恒为「群主」。
// v4 之后小群主＝房间创建者，所以这里只补「缺 owner」的老房间，不再强行改写已有 owner。
// 超管仍然强制进成员表：任意房间满血是规格第零条。
let ownerMigrated = false;
for (const r of db.rooms) {
  if (!r.owner) { r.owner = SYSTEM.superId || OWNER; ownerMigrated = true; }
  if (!r.roles || typeof r.roles !== "object") { r.roles = {}; ownerMigrated = true; }
  // v4 迁移：老房间没有角色表，两个 AI 补成管理员（否则它们接不了活），其余人一律普通成员。
  for (const id of BRIDGE_ENDS) {
    if (PEOPLE[id]?.status === "active" && id !== r.owner && !r.roles[id]) { r.roles[id] = "admin"; ownerMigrated = true; }
  }
  if (Array.isArray(r.members) && !r.members.includes(r.owner)) { r.members.unshift(r.owner); ownerMigrated = true; }
  if (Array.isArray(r.members) && !r.members.includes(SYSTEM.superId || OWNER)) { r.members.unshift(SYSTEM.superId || OWNER); ownerMigrated = true; }
}
if (ownerMigrated) flush();
recoverPendingCloses();

/* ---------- 真归档：把房间连同消息搬出 rooms.json，落成 data/archive/<id>.json ---------- */
function archivePath(id) { return path.join(ARCHIVE, id + ".json"); }
// 2026-11-05: 归档目录可能被搬走/删掉（曾经 data 落在桌面旧路径），写前必须补建，
// 否则 writeFileSync 抛 ENOENT 直接把整个 8792 进程带走（watchdog 每 2 分钟重启一次的死循环）。
function ensureArchiveDir() {
  try { fs.mkdirSync(ARCHIVE, { recursive: true }); } catch {}
}
function archiveList() {
  ensureArchiveDir();
  const out = [];
  let files = [];
  try { files = fs.readdirSync(ARCHIVE).filter((f) => f.endsWith(".json") && !f.startsWith("_")); } catch { files = []; }
  for (const f of files) {
    try {
      const d = JSON.parse(fs.readFileSync(path.join(ARCHIVE, f), "utf8"));
      if (!d || !d.room) continue;
      out.push({
        id: d.room.id,
        title: d.room.title,
        status: "archived",
        createdAt: d.room.createdAt,
        archivedAt: d.archivedAt,
        archivedBy: d.archivedBy || "",
        closedAt: d.room.closedAt || null,
        messageCount: (d.messages || []).length,
        reportCount: (d.messages || []).filter((m) => m.kind === "report" && !m.recalled).length,
        last: (d.messages || []).length
          ? (() => { const m = d.messages[d.messages.length - 1]; return { seq: m.seq, who: m.who, kind: m.kind, text: String(m.text || "").slice(0, 120) }; })()
          : null,
      });
    } catch {}
  }
  return out.sort((a, b) => String(b.archivedAt || "").localeCompare(String(a.archivedAt || "")));
}
function archiveMove(r, who) {
  const msgs = db.messages.filter((m) => m.roomId === r.id);
  // 用户 2026-11-05：归档就该把人名、id、角色、邀请有效性这些「非消息记录」删掉，只留消息做审计。
  // 所以先算「哪些人被带走了」，再 scrub 掉房间里的身份字段，落盘后从 people.json 里清人。
  const former = [...(r.members || [])];
  const liveRooms = db.rooms.filter((x) => x.id !== r.id);
  const purge = peopleToPurge(PEOPLE, { id: r.id, invites: r.invites || {}, __formerMembers: former }, liveRooms);
  const payload = {
    room: scrubRoomForArchive(r),
    messages: msgs,
    archivedAt: new Date().toISOString(),
    archivedBy: who,
    formerMemberCount: former.length,
    purgedPeople: purge.length,
    schema: 2,
  };
  ensureArchiveDir();
  fs.writeFileSync(archivePath(r.id), JSON.stringify(payload, null, 1));
  // 邀请进的临时身份跟着房一起清（you/me/daman 这类长期身份不动）。
  for (const id of purge) delete PEOPLE[id];
  if (purge.length) savePeople();
  db.rooms = db.rooms.filter((x) => x.id !== r.id);
  db.messages = db.messages.filter((m) => m.roomId !== r.id);
  meetingAbortRoom(r.id); // 会议室：归档 → 该房 open 议题全部 abort
  try { // v3：房间记忆随房归档，移入 data/archive/memory/
    if (fs.existsSync(memoryPath(r.id))) {
      fs.mkdirSync(MEMORY_ARCHIVE, { recursive: true });
      fs.renameSync(memoryPath(r.id), memoryArchivePath(r.id));
    }
  } catch {}
  flush();
  return payload;
}

/* ---------- 邀请链接（规格三.1）：一人一链、用完即废、认名不认人、被踢即死 ---------- */
/** token → {room, invite}。token 全局唯一，扫一遍活房即可（房间数是个位数）。 */
function findInvite(token) {
  const t = String(token || "");
  if (!t) return null;
  for (const r of db.rooms) {
    const inv = (r.invites || {})[t];
    if (inv) return { room: r, invite: inv };
  }
  return null;
}

/** 入场页：只问昵称，不泄露房间内部任何东西。 */
function invitePageHTML(token) {
  return `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>加入房间</title>
<style>
  :root{color-scheme:light}
  body{margin:0;min-height:100vh;display:grid;place-items:center;font:15px/1.6 -apple-system,"Segoe UI",system-ui,sans-serif;background:#f5f6f8;color:#1f2329}
  .card{width:min(92vw,380px);background:#fff;border:1px solid #e3e5e9;border-radius:14px;padding:26px 22px;box-shadow:0 8px 28px rgba(20,26,38,.06)}
  h1{margin:0 0 6px;font-size:19px}
  .sub{margin:0 0 18px;color:#6b7280;font-size:13px}
  input{width:100%;box-sizing:border-box;padding:11px 12px;border:1px solid #d6d9de;border-radius:9px;font:inherit}
  input:focus{outline:2px solid #3b82f6;outline-offset:1px;border-color:#3b82f6}
  button{width:100%;margin-top:12px;padding:11px;border:0;border-radius:9px;background:#1f2937;color:#fff;font:inherit;font-weight:600;cursor:pointer}
  button[disabled]{opacity:.5;cursor:default}
  .err{color:#d64545;font-size:13px;margin-top:10px;min-height:1em}
  .ok{color:#15803d;font-size:13px;margin-top:10px}
  .kbd{color:#9aa1ab;font-size:12px;margin-top:14px}
</style>
<div class="card">
  <h1 id="h1">加入房间</h1>
  <p class="sub" id="sub">请输入你的昵称。这个名字就是你在房间里的身份，退出后用同一名字可以回来。</p>
  <input id="name" maxlength="20" placeholder="你的昵称" autocomplete="off" autofocus>
  <input id="pw" type="password" placeholder="设个密码（以后用这个名字登录）" autocomplete="new-password" style="margin-top:8px">
  <button id="go" disabled>进入</button>
  <div class="err" id="err"></div>
  <div class="ok" id="ok"></div>
  <div class="kbd">一人一链 · 用完即废 · 被移出即失效</div>
</div>
<script>
const TOKEN = ${JSON.stringify(token)};
const $ = (id) => document.getElementById(id);
const params = new URLSearchParams(location.search);
const k = params.get('k') || '';
const api = (p, opt) => fetch(p + (p.includes('?') ? '&' : '?') + 'k=' + encodeURIComponent(k), Object.assign({ headers: { 'content-type': 'application/json' } }, opt || {}));
$('name').addEventListener('input', () => { $('go').disabled = !$('name').value.trim(); });
$('name').addEventListener('keydown', (e) => { if (e.key === 'Enter' && !$('go').disabled) $('go').click(); });
(async () => {
  try {
    const r = await api('/api/invite/' + encodeURIComponent(TOKEN));
    const d = await r.json();
    if (!d.ok) { $('h1').textContent = '这个邀请用不了了'; $('sub').textContent = d.error || '链接无效'; $('go').disabled = true; $('name').disabled = true; return; }
    $('h1').textContent = '加入「' + (d.roomTitle || '房间') + '」';
    if (d.seatName) $('sub').textContent = '这个座位原来是「' + d.seatName + '」，输一样的名字可以直接继承。';
    document.title = '加入「' + (d.roomTitle || '房间') + '」';
  } catch (e) { $('err').textContent = '网络错误：' + e.message; }
})();
$('go').addEventListener('click', async () => {
  $('go').disabled = true; $('err').textContent = ''; $('ok').textContent = '';
  const name = $('name').value.trim();
  const password = $('pw').value;
  try {
    const r = await api('/api/invite/' + encodeURIComponent(TOKEN) + '/claim', { method: 'POST', body: JSON.stringify({ name, password }) });
    const d = await r.json();
    if (!d.ok) { $('err').textContent = d.error || '进不去'; $('go').disabled = false; return; }
    // 会话 cookie 已经由 claim 响应种好了（HttpOnly），这里只记「上次看的房间」。
    // 不再往 localStorage 塞成员 key —— 浏览器从此不持有任何明文密钥。
    try { localStorage.setItem('ds-teams:room', d.roomId); } catch {}
    $('ok').textContent = '欢迎，' + d.name + '。正在进入…';
    location.replace('/?k=' + encodeURIComponent(k));
  } catch (e) { $('err').textContent = '网络错误：' + e.message; $('go').disabled = false; }
});
</script>`;
}
function archiveRestore(id) {
  const fp = archivePath(id);
  if (!fs.existsSync(fp)) return null;
  const d = JSON.parse(fs.readFileSync(fp, "utf8"));
  const r = { ...d.room, id, status: "closed" };
  // schema 2 起归档文件是 scrub 过的：人名/id/邀请全被清掉，还原回来只剩超管一个人，身份靠重新邀请。
  if (!r.owner) r.owner = SYSTEM.superId || OWNER;
  r.members = Array.isArray(r.members) && r.members.length ? r.members : [r.owner];
  r.roles = r.roles && typeof r.roles === "object" ? r.roles : {};
  r.invites = {};
  r.muted = [];
  db.rooms.push(r);
  const seen = new Set(db.messages.map((m) => m.seq));
  for (const m of d.messages || []) if (!seen.has(m.seq)) db.messages.push(m);
  db.messages.sort((a, b) => a.seq - b.seq);
  fs.renameSync(fp, fp + ".restored-" + Date.now());
  try { // v3：还原时把记忆从 data/archive/memory/ 移回 data/memory/
    if (fs.existsSync(memoryArchivePath(id))) {
      fs.mkdirSync(MEMORY, { recursive: true });
      fs.renameSync(memoryArchivePath(id), memoryPath(id));
    }
  } catch {}
  // 还原入口只允许群主（who 已校验），文案固定即可
  push(id, "system", "system", "已从归档还原（群主操作）");
  flush();
  return r;
}

// 引用快照：只存被引消息的 seq/who/文本前 200 字；被撤回的消息不能再被引用
function quoteOf(roomId, seq) {
  const n = Number(seq || 0);
  if (!n) return null;
  const q = db.messages.find((x) => x.roomId === roomId && x.seq === n && !x.recalled);
  return q ? { seq: q.seq, who: q.who, kind: q.kind, text: String(q.text || "").slice(0, 200), ts: q.ts } : null;
}

function push(roomId, who, kind, text, extra = {}) {
  // 幂等：同一 msgId 在同一房间只落一条（重发/重投不产生重复消息）
  if (extra.msgId) {
    const dup = db.messages.find((x) => x.roomId === roomId && x.msgId === extra.msgId);
    if (dup) return dup;
  }
  const m = { seq: ++db.seq, roomId, ts: new Date().toISOString(), who, kind, text: String(text || "").slice(0, 8000), ...extra };
  db.messages.push(m);
  // 有人真干活了，巡检升级计数与冷却一起归零（实质消息集合见 health-lib）
  if (SUBSTANTIVE_KINDS.has(kind)) {
    const r = room(roomId);
    if (r) { r.patrolLevel = 0; r.lastPatrolAt = null; }
  }
  save();
  broadcast(roomId, m);
  // 名单/角色/规则/解散这类系统卡，本质就是「房间元信息变了」。挂在这里而不是挂在 flush() 上：
  // push 是唯一会落卡的地方，心跳、游标、启动读库都不会走到这儿，所以不会白刷。
  // 漏调过的 action 也因此自动补上，不再需要逐处手写 metaRoom。
  if (kind === "system") { metaRoom(roomId, "system"); metaRooms("system"); }
  else if (kind === "report" || kind === "review") metaRooms(kind);
  return m;
}

function room(id) { return db.rooms.find((r) => r.id === id); }

/* ---------- 评审端延迟观测（用户 2026-11-04 要求：超阈值就记 warn，且 UI 能开关） ---------- */
// 口径：从「报告卡落库时刻」到「评审动作落库时刻」的墙钟差。评审动作 = review 卡（pass/issues）
// 或沉默收敛（wake NO_REPLY → onSilence）。这是人真正能感知的等待，含唤醒、排队、思考。
// 只记 warn，不改任何业务行为；开关与阈值在 data/settings.json，UI 房间头有按钮。
// 起算点：首轮从报告卡落库算；返工轮（rounds>=2）从上一轮 review 落库算，
// 否则第 2 轮会把上一轮整段等待也算进来，数值虚高。
function reviewLatencyMs(issue) {
  if (!issue || !issue.reportSeq) return null;
  const rounds = Array.isArray(issue.rounds) ? issue.rounds : [];
  const prevAt = rounds.length >= 2 ? rounds[rounds.length - 2].at : null;
  let from = prevAt ? Date.parse(prevAt) : NaN;
  if (!Number.isFinite(from)) {
    // 优先用开题时固化的 reportTs：归档会把消息清出 rooms.json，回查会落空。
    from = Date.parse(String(issue.reportTs || ""));
    if (!Number.isFinite(from)) {
      const rep = db.messages.find((x) => x.roomId === issue.roomId && Number(x.seq) === Number(issue.reportSeq));
      if (rep) from = Date.parse(rep.ts || "");
    }
    // C1（11-04）：老议题没有 reportTs，报告卡又已被归档清出 rooms.json 时，退到开题时间
    // （meetingOpen 与报告落库同一瞬间，误差 <1s）。宁可算出一个近似值，也不要静默漏报。
    if (!Number.isFinite(from)) from = Date.parse(String(rounds[0] && rounds[0].at || issue.createdAt || ""));
    if (!Number.isFinite(from)) return null;
  }
  const to = Date.parse(issue.updatedAt || "");
  if (!Number.isFinite(from) || !Number.isFinite(to)) return null;
  const ms = to - from;
  return ms >= 0 ? ms : null;
}

// A3 定档（11-04 用户拍板「30s 黄、90s 红推人、15 分钟节流聚合」）：
//   黄线 warnMs：超了记 REVIEW_LATENCY warn（UI chip 染黄计数）。
//   红线 criticalMs：超了额外记 REVIEW_LATENCY_CRITICAL，并推一条消息进群主常看的房（推人）。
//   节流 quietWindowMs：红线推人的静默窗，窗内只聚合计数（quietCount），过窗合并成一条再推。
// 沉默收敛（closedBy:"silent"）恒在 ~90s 判定=设计行为：黄线跳过（不刷屏），红线照推（90s 沉默也值得知道）。
let lastCriticalPushAt = 0;
let quietCount = 0;
function noteReviewLatency(issue, closedBy) {
  const st = readSettings();
  if (!st.reviewLatency.enabled) return;
  const ms = reviewLatencyMs(issue);
  if (ms === null) return;
  const silent = closedBy === "silent";
  const { warnMs, criticalMs, quietWindowMs } = st.reviewLatency;
  const fields = {
    roomId: String(issue.roomId || ""),
    issueId: String(issue.id || ""),
    reviewer: String(issue.reviewer || ""),
    reportSeq: Number(issue.reportSeq || 0),
    round: Array.isArray(issue.rounds) ? issue.rounds.length : 1,
    latencyMs: ms,
    closedBy: String(closedBy || ""),
    title: String(issue.title || "").slice(0, 60),
  };
  if (!silent && ms > warnMs) {
    opsWarn("REVIEW_LATENCY", { ...fields, warnMs });
  }
  if (ms > criticalMs) {
    opsWarn("REVIEW_LATENCY_CRITICAL", { ...fields, criticalMs });
    // 推人（节流聚合）：窗内静默累计，过窗一次性合并推
    quietCount++;
    if (Date.now() - lastCriticalPushAt >= quietWindowMs) {
      const n = quietCount;
      quietCount = 0;
      lastCriticalPushAt = Date.now();
      const secs = Math.round(ms / 1000);
      // 推进所有 open 房（群主看的房不固定；一条系统卡，不@人不触发 agent 注入——
      // system 卡只有含 ⚠️/掉线/转给 才注入，这里故意不含，只给人看）
      for (const r of db.rooms.filter((x) => x.status === "open")) {
        push(r.id, "system", "system", `评审延迟红线：${secs}s 超过 ${Math.round(criticalMs / 1000)}s${n > 1 ? `（近一窗内 ${n} 条）` : ""}，评审端 ${fields.reviewer || "?"}，议题「${fields.title}」`);
      }
    }
  }
}
const clients = new Set();
function broadcast(roomId, m) {
  const frame = `event: msg\ndata: ${JSON.stringify(m)}\n\n`;
  for (const c of clients) {
    if (c.room === roomId || c.room === "*") { try { c.res.write(frame); } catch {} }
  }
}

/* ---------- 零依赖热加载：改文件 → 页面自己刷新（2026-11-05） ----------
 * 前端：fs.watch 盯 ui.html（以及同目录 .css/.js），变了就往所有 SSE 连接推 event: reload。
 * 后端：--watch（或 DS_TEAMS_WATCH=1）时盯 *.mjs，变了就 process.exit(0)，
 *       由 run-server.cmd 的守护循环 3 秒后拉起新进程（不引 nodemon / chokidar）。 */
function broadcastReload(payload) {
  const frame = `event: reload\ndata: ${JSON.stringify(payload)}\n\n`;
  let n = 0;
  for (const c of clients) {
    try { c.res.write(frame); n += 1; } catch {}
  }
  return n;
}

function watchFrontend() {
  if (process.env.DS_TEAMS_HOT === "0") return;
  let timer = null;
  try {
    fs.watch(ROOT, { persistent: false }, (_, filename) => {
      const name = String(filename || "");
      if (!/\.(html|css|js)$/i.test(name)) return; // 数据目录的 json 不算，别把页面刷爆
      clearTimeout(timer);
      timer = setTimeout(() => {
        let mtime = 0;
        try { mtime = fs.statSync(path.join(ROOT, name)).mtimeMs; } catch { return; }
        const n = broadcastReload({ file: name, at: new Date().toISOString(), mtime: Math.round(mtime) });
        console.log(`[hot] ${name} 变了 → 通知 ${n} 个页面刷新`);
      }, 200);
    });
  } catch (error) {
    console.error("[hot] 前端监听失败，热刷新不可用：", error.message);
  }
}

function watchBackend() {
  const on = process.env.DS_TEAMS_WATCH === "0"
    ? false
    : (process.argv.includes("--watch") || process.env.DS_TEAMS_WATCH === "1");
  if (!on) return;
  console.log("[hot] --watch 已开：改 *.mjs 会自动重启（run-server.cmd 守护）");
  let timer = null;
  // 基线：启动时各 .mjs 的 mtime。Windows 上 fs.watch 会送幻影事件（读文件、杀毒扫描、
  // 同步盘回写都会触发），mtime 没真变就不重启——否则跑测试时会把自己的服务重启掉。
  const base = new Map();
  try {
    for (const f of fs.readdirSync(ROOT)) {
      if (!/\.mjs$/i.test(f)) continue;
      try { base.set(f, fs.statSync(path.join(ROOT, f)).mtimeMs); } catch {}
    }
  } catch (error) {
    console.error("[hot] 基线扫描失败：", error.message);
  }
  try {
    fs.watch(ROOT, { persistent: false }, (_, filename) => {
      const name = String(filename || "");
      if (!/\.mjs$/i.test(name)) return;
      let mtime = 0;
      try { mtime = fs.statSync(path.join(ROOT, name)).mtimeMs; } catch { return; } // 删掉的文件不重启
      if (base.get(name) === mtime) return; // 幻影事件：mtime 没变
      base.set(name, mtime);
      clearTimeout(timer);
      // 先把 reload 推给页面再退，省得它等重连；守护脚本会重新拉起进程。
      timer = setTimeout(() => {
        console.log(`[hot] ${name} 变了 → 退出等守护重启`);
        try { broadcastReload({ file: name, at: new Date().toISOString(), restart: true }); } catch {}
        // 给 SSE 一点时间把帧写出去
        setTimeout(() => process.exit(0), 120);
      }, 250);
    });
  } catch (error) {
    console.error("[hot] 后端监听失败：", error.message);
  }
}

function authed(req, url) {
  const cookies = String(req.headers.cookie || "").split(/;\s*/);
  if (cookies.includes(`${AUTH.platform.cookie}=${KEY}`)) return true;
  return url.searchParams.get(AUTH.platform.query) === KEY;
}
function json(res, code, obj, extraHeaders) {
  const body = JSON.stringify(obj);
  const headers = { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" };
  if (extraHeaders) Object.assign(headers, extraHeaders);
  res.writeHead(code, headers);
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve) => {
    let raw = "";
    req.on("data", (c) => { raw += c; if (raw.length > 40_000_000) req.destroy(); });
    req.on("end", () => { try { resolve(JSON.parse(raw || "{}")); } catch { resolve({}); } });
  });
}

const f_ts = (f) => Date.parse(f.since || 0);
function roomSummary(r) {
  const msgs = db.messages.filter((m) => m.roomId === r.id);
  const last = msgs[msgs.length - 1];
  const reports = msgs.filter((m) => m.kind === "report" && !m.recalled);
  const lastTs = last ? Date.parse(last.ts) : Date.parse(r.createdAt || 0);
  // whoName：房间列表的「最后一条是谁说的」要用真名。
  // 受邀成员是 h-xxxx 这种 id，前端 PEOPLE 里没有，只给 id 就只能显示一串哈希。
  const lastName = last ? ((personOf(last.who) || {}).name || (last.who === "system" ? "系统" : last.who)) : "";
  return { ...r, floor: r.floor || null, memberCount: Array.isArray(r.members) ? r.members.length : 0,
    // 待确认解散：房间列表要能看见「有人要拆这间房，等超管裁决」，否则只有开着的页面知道。
    pendingCloseAt: r.pendingClose && r.pendingClose.status === "pending" ? (r.pendingClose.at || "") : null,
    idleSeconds: Math.max(0, Math.round((Date.now() - (lastTs || Date.now())) / 1000)), messageCount: msgs.length, last: last ? { seq: last.seq, who: last.who, whoName: lastName, kind: last.kind, text: last.text.slice(0, 120) } : null, reportCount: reports.length };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  const p = url.pathname;

  if (p === "/api/member" && req.method === "GET") {
    // 2026-11-05 群主拍板：这里**不再下发任何成员 key**。
    // 之前（哪怕收窄后）它还把 you/me/daman 三把长期 key 发给任何拿到隧道链接的人，
    // 等于谁点开链接谁就是群主。浏览器现在走 /api/auth/login 的会话 cookie；
    // CLI / 桥接 / 测试直接从 data/people.json 读 key，不需要这个接口。
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    return json(res, 200, { ok: true, keys: {}, deprecated: true, hint: "改用 /api/auth/login 登录拿会话 cookie" });
  }

  /* ---------- 登录 / 注册 / 登出（浏览器身份的唯一来源） ---------- */
  if (p === "/api/auth/whoami" && req.method === "GET") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    const s = sessionOf(req);
    const sup = personOf(superId());
    const setupNeeded = !sup || !sup.pw;
    if (!s) return json(res, 200, { ok: false, loginRequired: true, setupNeeded, superName: sup ? sup.name : "" });
    const person = personOf(s.who);
    return json(res, 200, {
      ok: true,
      id: person.id,
      name: person.name,
      kind: person.kind,
      isSuper: sysRoleOf(person.id) === "super",
      hasPassword: !!person.pw,
    });
  }

  // 超管注册：只在超管还没设密码时可用，必须带主机上的初始注册码。
  // 规格第零条「第一个打开初始链接并注册账号的真人」——注册码就是「主机本地」这道墙。
  if (p === "/api/auth/register" && req.method === "POST") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    const b = await readBody(req);
    if (!setupCodeOk(b.setupCode)) {
      return json(res, 403, { ok: false, error: "初始注册码不对。它在服务器那台机器的 data/setup-code.txt 里（启动日志也打印过）。" });
    }
    const sup = personOf(superId());
    if (sup && sup.pw) return json(res, 409, { ok: false, error: "超级群主已经注册过了，直接登录" });
    const named = normalizeName(b.name);
    if (!named.ok) return json(res, 400, { ok: false, error: named.error });
    const pw = SESSION.checkPassword(b.password);
    if (!pw.ok) return json(res, 400, { ok: false, error: pw.error });
    let person = sup;
    if (!person) {
      const id = "u-" + crypto.randomBytes(4).toString("hex");
      person = { id, name: named.name, kind: "human", role: "admin", key: newKey(), status: "active", joinedAt: new Date().toISOString(), createdVia: "register" };
      PEOPLE[id] = person;
      SYSTEM.superId = id;
      saveSystem();
    } else {
      person.name = named.name;
      person.createdVia = person.createdVia || "register";
    }
    person.pw = SESSION.hashPassword(b.password);
    person.registeredAt = new Date().toISOString();
    savePeople();
    revokeSessionsOf(person.id, "register");
    db.presence[person.id] = Date.now();
    const token = createSession(person.id, req);
    // 只有脚本（带 x-ds-client 头）才回 key；浏览器永远拿不到明文成员 key。
    const body = { ok: true, id: person.id, name: person.name, isSuper: true };
    if (String(req.headers["x-ds-client"] || "")) body.key = person.key;
    return json(res, 200, body, { "set-cookie": SESSION.setCookieHeader(token) });
  }

  if (p === "/api/auth/login" && req.method === "POST") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    const b = await readBody(req);
    const wanted = String(b.id || b.who || "").trim();
    const name = String(b.name || "").trim();
    let person = wanted ? personOf(wanted) : Object.values(PEOPLE).find((x) => x.status === "active" && x.name === name);
    if (!person || person.status !== "active") return json(res, 401, { ok: false, error: "没有这个身份" });
    if (!person.pw) return json(res, 409, { ok: false, error: "这个身份还没设密码，请超管在「成员」里给他设一个" });
    if (!SESSION.verifyPassword(b.password, person.pw)) {
      await new Promise((r) => setTimeout(r, 400)); // 削一下暴力猜
      return json(res, 401, { ok: false, error: "密码不对" });
    }
    revokeSessionsOf(person.id, "relogin"); // 一个身份只保留最新一条会话，换设备即踢掉旧的
    const token = createSession(person.id, req);
    db.presence[person.id] = Date.now();
    return json(res, 200, { ok: true, id: person.id, name: person.name, isSuper: sysRoleOf(person.id) === "super" }, { "set-cookie": SESSION.setCookieHeader(token) });
  }

  if (p === "/api/auth/logout" && req.method === "POST") {
    const s = sessionOf(req);
    if (s) { delete SESSIONS[s.hash]; sessionsDirty = true; saveSessions(true); }
    res.setHeader("Set-Cookie", SESSION.clearCookieHeader());
    return json(res, 200, { ok: true });
  }

  // 设/改密码：本人（有会话）改自己的；超管可以给任何人设（body.who）。
  if (p === "/api/auth/password" && req.method === "POST") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    const b = await readBody(req);
    const s = sessionOf(req);
    if (!s) return json(res, 401, { ok: false, error: "先登录" });
    const target = String(b.who || "").trim() || s.who;
    const isSuper = sysRoleOf(s.who) === "super";
    if (target !== s.who && !isSuper) return json(res, 403, { ok: false, error: "no permission: member.setPassword" });
    const person = personOf(target);
    if (!person || person.status !== "active") return json(res, 404, { ok: false, error: "没有这个身份" });
    if (target === s.who && person.pw && !SESSION.verifyPassword(b.oldPassword, person.pw)) {
      return json(res, 403, { ok: false, error: "原密码不对" });
    }
    const pw = SESSION.checkPassword(b.password);
    if (!pw.ok) return json(res, 400, { ok: false, error: pw.error });
    person.pw = SESSION.hashPassword(b.password);
    person.passwordSetAt = new Date().toISOString();
    savePeople();
    revokeSessionsOf(target, target === s.who ? "password-change" : "password-set-by-super");
    if (target !== s.who) return json(res, 200, { ok: true });
    // 自己改密码：旧会话已吊销，顺手发一条新的，别把用户踢下线。
    const token = createSession(target, req);
    return json(res, 200, { ok: true }, { "set-cookie": SESSION.setCookieHeader(token) });
  }

  // 「我是谁、我在这间房能干什么」——UI 只信这个接口的答复来渲染按钮。
  // 规格五：所有 UI 与身份标签必须由服务端真实角色 + 是否超管双重判断得出，
  // 前端不许再自己写 `who === 'you'`（上一轮「我明明可以解散，却被藏了按钮」就是这么来的）。
  if (p === "/api/me" && req.method === "GET") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    // who 可以从 query 显式给（CLI/测试），也可以完全不给——浏览器只靠会话 cookie。
    const who = String(url.searchParams.get("who") || "").trim() || sessionWho(req);
    const roomId = String(url.searchParams.get("roomId") || "").trim();
    const givenKey = String(url.searchParams.get("key") || "");
    const person = actorOf(who);
    if (!person) return json(res, 403, { ok: false, error: "unknown or inactive identity" });
    const proven = givenKey ? SESSION.safeEqual(givenKey, person.key) : (sessionWho(req) === person.id);
    if (!proven) return json(res, 403, { ok: false, error: "identity key mismatch" });
    // 页面每 2 秒轮询这个接口刷新 presence：解散确认闸、在线状态都靠它判断人在不在。
    db.presence = db.presence && typeof db.presence === "object" ? db.presence : {};
    db.presence[person.id] = Date.now();
    const r = roomId ? room(roomId) : null;
    const d = describeAccess(person, r);
    return json(res, 200, {
      ok: true,
      id: person.id,
      name: person.name,
      kind: person.kind,
      isSuper: person.sysRole === "super",
      roomId: r ? r.id : null,
      role: d.role,
      caps: d.caps,
      canActOn: d.canActOn,
      // 是否在这间房里（超管不需要）
      isMember: r ? (r.members || []).includes(person.id) || person.sysRole === "super" : false,
      roomOwner: r ? r.owner : null,
      roomStatus: r ? r.status : null,
      pendingClose: r && r.pendingClose && r.pendingClose.status === "pending" ? r.pendingClose : null,
    });
  }

  if (p === "/api/people" && req.method === "GET") {
    // 身份列表含 key 的间接泄露面，必须先鉴权（此前放在 authed 之前是 bug）
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    const list = Object.values(PEOPLE).map((x) => ({ id: x.id, name: x.name, kind: x.kind, role: x.role, status: x.status, joinedAt: x.joinedAt, note: x.note || "" }));
    return json(res, 200, { ok: true, people: list });
  }
  if (p === "/api/people" && req.method === "POST") {
    // 群主开通新身份：真人（human）或外部 agent；key 只返回这一次
    const pb = await readBody(req);
    if (!permit(res, req, pb, "people.create", null)) return;
    const pwho = String(pb.who || "");
    const name = String(pb.name || "").trim();
    const kind = pb.kind === "agent" ? "agent" : "human";
    if (!name) return json(res, 400, { ok: false, error: "name required" });
    const id = (kind === "human" ? "h-" : "a-") + crypto.randomBytes(3).toString("hex");
    PEOPLE[id] = { id, name, kind, role: "member", key: crypto.randomBytes(6).toString("hex"), status: "active", joinedAt: new Date().toISOString(), note: String(pb.note || "") };
    savePeople();
    return json(res, 200, { ok: true, person: { id, name, kind, role: "member" }, key: PEOPLE[id].key });
  }

  // 项目空间：自带 person key 鉴权，故放在平台 key 的 authed 之前
  if (p.startsWith("/api/projects")) {
    try {
      const handled = await projectSpace.handle(req, res, url, readBody, json);
      if (handled !== false) return;
    } catch (e) {
      return json(res, 500, { ok: false, error: String((e && e.message) || e) });
    }
  }

  // 只读健康快照：供 healthcheck 巡检用。必须鉴权，但 401 也回 JSON（排在通用 401 HTML 之前）
  if (p === "/api/health" && req.method === "GET") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    try {
      return json(res, 200, buildHealth({ db, dataDir: DATA, ledger: readLedger() }));
    } catch (e) {
      return json(res, 500, { ok: false, error: String((e && e.message) || e) });
    }
  }

  if (!authed(req, url)) {
    // API 面统一回 JSON，页面才回 HTML：脚本/测试不该为了解析一句中文去猜 401 的格式。
    if (p.startsWith("/api/")) return json(res, 401, { ok: false, error: "unauthorized" });
    res.writeHead(401, { "content-type": "text/html; charset=utf-8" });
    res.end("<meta charset=utf-8><body style='font:14px sans-serif;padding:30px'>需要钥匙：<code>?k=...</code></body>");
    return;
  }

  if (p === "/" || p === "/index.html") {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "set-cookie": `tk=${KEY}; Path=/; Max-Age=604800; SameSite=Lax` });
    res.end(fs.existsSync(UI) ? fs.readFileSync(UI) : "<h1>ui.html 还没写</h1>");
    return;
  }

  // 入口页：隧道 URL 每次重启都会变，这里给一个固定地址（本机 8792/entry）随时能拿到现链接。
  if (p === "/entry" && req.method === "GET") {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "set-cookie": `tk=${KEY}; Path=/; Max-Age=604800; SameSite=Lax` });
    res.end(ENTRY_HTML);
    return;
  }

  if (p === "/api/tunnel" && req.method === "GET") {
    if (!authed(req, url)) return json(res, 401, { ok: false, error: "unauthorized" });
    return json(res, 200, tunnelSnapshot());
  }

  /* ---------- 邀请链接（规格三.1）---------- */
  // 落地页 /i/<token>：发给真人的就是这一条链接，页面只问昵称。
  const iPage = p.match(/^\/i\/([A-Za-z0-9_-]{8,64})$/);
  if (iPage && req.method === "GET") {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "set-cookie": `tk=${KEY}; Path=/; Max-Age=604800; SameSite=Lax` });
    res.end(invitePageHTML(iPage[1]));
    return;
  }
  const iInfo = p.match(/^\/api\/invite\/([A-Za-z0-9_-]{8,64})$/);
  if (iInfo && req.method === "GET") {
    const found = findInvite(iInfo[1]);
    if (!found) return json(res, 404, { ok: false, error: "邀请链接不存在" });
    const chk = canClaim(found.invite, found.room);
    // 只回「能不能进 + 座位名」，不回成员名单、不回消息。
    return json(res, chk.ok ? 200 : chk.code, {
      ok: chk.ok, error: chk.error || null,
      roomId: found.room.id, roomTitle: found.room.title,
      status: found.invite.status, seatName: found.invite.seatName || "",
    });
  }
  const iClaim = p.match(/^\/api\/invite\/([A-Za-z0-9_-]{8,64})\/claim$/);
  if (iClaim && req.method === "POST") {
    const cb = await readBody(req);
    const found = findInvite(iClaim[1]);
    if (!found) return json(res, 404, { ok: false, error: "邀请链接不存在" });
    const { room: ir, invite: inv } = found;
    const chk = canClaim(inv, ir);
    if (!chk.ok) return json(res, chk.code, { ok: false, error: chk.error });
    const nm = normalizeName(cb.name);
    if (!nm.ok) return json(res, 400, { ok: false, error: nm.error });
    if (isBlacklisted(ir, nm.name)) {
      return json(res, 403, { ok: false, error: "这个名字已被移出过，只能由群主重新邀请" });
    }
    const seat = resolveSeat({ people: PEOPLE, room: ir, name: nm.name, token: inv.token });
    if (seat.kind === "busy") return json(res, 409, { ok: false, error: "ID 被占用，必须重新取名字" });
    let person;
    let reclaimed = false;
    if (seat.kind === "inherit") {
      person = PEOPLE[seat.person.id];
      // 重签底层 secret：只把链接「空出来」而不换 key，等于把身份白送。
      person.key = newKey();
      person.status = "active";
      person.leftAt = null;
      person.reclaimedAt = new Date().toISOString();
      reclaimed = true;
    } else {
      let id = "h-" + crypto.randomBytes(4).toString("hex");
      while (PEOPLE[id]) id = "h-" + crypto.randomBytes(4).toString("hex");
      person = {
        id, name: nm.name, kind: "human", role: "member", key: newKey(),
        status: "active", joinedAt: new Date().toISOString(), createdVia: "invite",
      };
      PEOPLE[id] = person;
    }
    // 落地页可以顺手设密码：设了以后就能用「昵称 + 密码」在任何设备登录，
    // 不用非得再走一次邀请链接。不设也行，当前这条会话 7 天内有效。
    if (cb.password !== undefined && String(cb.password) !== "") {
      const pw = SESSION.checkPassword(cb.password);
      if (!pw.ok) return json(res, 400, { ok: false, error: pw.error });
      person.pw = SESSION.hashPassword(cb.password);
      person.passwordSetAt = new Date().toISOString();
    }
    savePeople();
    bindInvite(inv, person);
    if (!ir.members.includes(person.id)) ir.members.push(person.id);
    // 规格三.1 身份防错乱：受邀者强制普通成员，绝不继承任何管理身份。
    if (ir.roles) delete ir.roles[person.id];
    push(ir.id, "system", "system", `${person.name} 通过邀请链接进入房间${reclaimed ? "（继承原座位）" : ""}`);
    metaRoom(ir.id, "claim");
    metaRooms("claim");
    flush();
    // 落地页拿到会话 cookie 后直接跳回主页面，全程不接触明文成员 key。
    revokeSessionsOf(person.id, "invite-claim");
    const token = createSession(person.id, req);
    db.presence[person.id] = Date.now();
    const claimBody = { ok: true, id: person.id, name: person.name, roomId: ir.id, role: "member", reclaimed };
    if (String(req.headers["x-ds-client"] || "")) claimBody.key = person.key; // 老脚本兼容
    return json(res, 200, claimBody, { "set-cookie": SESSION.setCookieHeader(token) });
  }
  // 邀请链接清单：只有能拉人的人才看得到（链接本身就是入场凭证）。
  const iList = p.match(/^\/api\/rooms\/([^/]+)\/invites$/);
  if (iList && req.method === "GET") {
    const lb = { who: String(url.searchParams.get("who") || ""), key: url.searchParams.get("mk") || req.headers["x-member-key"] || "" };
    const lr = room(iList[1]);
    if (!lr) return json(res, 404, { ok: false, error: "no such room" });
    const gate = permit(res, req, lb, "member.invite", lr);
    if (!gate) return;
    const base = String(url.searchParams.get("base") || "").trim().replace(/\/+$/, "") || "http://127.0.0.1:" + PORT;
    const items = Object.values(lr.invites || {})
      .filter((inv) => inv && inv.token)
      .sort((a, b) => String(b.at).localeCompare(String(a.at)))
      .map((inv) => ({
        token: inv.token,
        url: base + "/i/" + inv.token + "?k=" + KEY,
        status: inv.status,
        seatName: inv.seatName || "",
        by: inv.byName || inv.by || "",
        at: inv.at || null,
      }));
    return json(res, 200, { ok: true, invites: items });
  }
  // 主动退出：链接空出，座位名保留，后来者输同样昵称即可继承。
  const iLeave = p.match(/^\/api\/rooms\/([^/]+)\/leave$/);
  if (iLeave && req.method === "POST") {
    const lb = await readBody(req);
    const who = whoOf(lb, req);
    const me = actorOf(who);
    if (!me) return json(res, 403, { ok: false, error: "unknown or inactive identity" });
    if (!whoAllowed(who, lb, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    const lr = room(iLeave[1]);
    if (!lr) return json(res, 404, { ok: false, error: "no such room" });
    if (lr.status !== "open") return json(res, 409, { ok: false, error: "房间不是进行中" });
    if (me.id === lr.owner) return json(res, 409, { ok: false, error: "小群主要么转让他，要么解散房间" });
    if (!lr.members.includes(me.id)) return json(res, 409, { ok: false, error: "不在这间房里" });
    lr.members = lr.members.filter((x) => x !== me.id);
    if (lr.roles) delete lr.roles[me.id];
    if (Array.isArray(lr.muted)) lr.muted = lr.muted.filter((x) => x !== me.id);
    if (lr.floor && lr.floor.holder === me.id) lr.floor = null;
    for (const inv of Object.values(lr.invites || {})) if (inv.holder === me.id) releaseInvite(inv);
    me.leftAt = new Date().toISOString();
    savePeople();
    push(lr.id, "system", "system", `${me.name} 退出了房间（邀请链接已空出，同名可以继承）`);
    metaRoom(lr.id, "leave");
    metaRooms("leave");
    flush();
    return json(res, 200, { ok: true, room: lr });
  }

  // 控制面 v2：人类看板。房间 UI 仍是 /，这里只读台账 + 人审动作。
  if (p === "/control" || p === "/control.html") {
    // 与 / 一样下发 tk cookie：否则首次用 ?k= 打开看板后，页面里的 fetch 没有 key 会 401。
    res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", "set-cookie": `tk=${KEY}; Path=/; Max-Age=604800; SameSite=Lax` });
    res.end(fs.existsSync(CONTROL_UI) ? fs.readFileSync(CONTROL_UI) : "<meta charset=utf-8><body style='font:14px sans-serif;padding:30px'>control.html 还没写</body>");
    return;
  }

  if (p === "/api/control/state" && req.method === "GET") {
    const project = String(url.searchParams.get("project") || "").trim();
    const tasks = listTasks({ projectId: project, all: true });
    const projects = [...new Set(tasks.map((t) => t.projectId).filter(Boolean))]
      .map((id) => ({ id, title: id }));
    // cap / capacity：并发名额的「上限」与「用了多少」，房间与项目分开算（T6）。
    return json(res, 200, { ok: true, now: new Date().toISOString(), projects, counts: controlCounts(), cap: CAP, capacity: dispatchCounts(), tasks });
  }

  if (p === "/api/control/task" && req.method === "POST") {
    const b = await readBody(req);
    const projectId = String(b.projectId || "").trim();
    if (!projectId) return json(res, 400, { ok: false, error: "projectId required" });
    const taskId = String(b.taskId || "").trim() || ("T-" + new Date().toISOString().replace(/\D/g, "").slice(0, 14) + "-" + crypto.randomBytes(2).toString("hex"));
    const r = createTask({
      projectId,
      taskId,
      title: b.title,
      acceptance: Array.isArray(b.acceptance) ? b.acceptance : [],
      writeScope: Array.isArray(b.writeScope) ? b.writeScope : [],
      gate: b.gate,
      owner: "you",
      assignee: b.assignee || "you",
    });
    return json(res, r.duplicate ? 409 : r.ok ? 200 : 400, r);
  }

  const controlAction = p.match(/^\/api\/control\/task\/(.+)\/(accept|reject)$/);
  if (controlAction && req.method === "POST") {
    const key = decodeURIComponent(controlAction[1]);
    const b = await readBody(req);
    const r = controlAction[2] === "accept"
      ? acceptTask(key, { by: "you", note: b.note, force: b.force === true })
      : rejectTask(key, { by: "you", note: b.note });
    const code = r.ok ? 200 : /unknown key/.test(r.error || "") ? 404 : 400;
    return json(res, code, r);
  }

  if (p === "/api/rooms" && req.method === "GET") {
    const viewer = viewerOf(req, url);
    const rooms = db.rooms.filter((r) => canSeeRoom(viewer, r));
    return json(res, 200, {
      ok: true,
      viewer: viewer ? { id: viewer.id, name: viewer.name } : null,
      rooms: rooms.map(roomSummary).sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1)),
    });
  }

  if (p === "/api/rooms" && req.method === "POST") {
    const b = await readBody(req);
    const title = String(b.title || "").trim() || "未命名任务";
    // 建群与命令绑定：必须声明建群方；默认小满，大满仅在指定时建；同题 10 分钟内不许重复建
    const declaredBy = String(b.declaredBy || b.by || b.who || "").trim() || sessionWho(req);
    if (!declaredBy) {
      return json(res, 400, { ok: false, error: "declaredBy required: me|daman|you（建群方必须声明）" });
    }
    // 建群方声明必须是本人：会话 cookie 或成员 key 任一证明。避免拿着全局 ?k 冒名声明「本次由小满建群」
    if (!whoAllowed(declaredBy, b, req)) {
      return json(res, 403, { ok: false, error: "identity key mismatch: declaredBy=" + declaredBy });
    }
    // 规格第零条：只有超级群主和两个 AI 测试账号能建房，普通真人一律无入口（后端也拦）。
    const creator = actorOf(declaredBy);
    if (!creator) return json(res, 403, { ok: false, error: "unknown or inactive identity" });
    const mayCreate = creator.sysRole === "super" || creator.kind === "agent";
    if (!mayCreate) return json(res, 403, { ok: false, error: "no permission: room.create" });
    const dupe = db.rooms.find((x) => x.status === "open" && x.title === title && (Date.now() - Date.parse(x.createdAt)) < 10 * 60_000);
    if (dupe) return json(res, 409, { ok: false, error: "duplicate room", room: dupe });
    // 小群主＝房间创建者。AI 代建时房间归超级群主（代建语义），真人建房则自己就是小群主。
    const ownerId = String(b.ownerId || "").trim()
      || (creator.sysRole === "super" ? creator.id : superId());
    const r = {
      id: "room-" + crypto.randomBytes(3).toString("hex"),
      title,
      status: "open",
      createdAt: new Date().toISOString(),
      // 默认成员＝管理员（群主+小满+大满），不是全部 active 身份；开通的真人靠邀请入场
      owner: ownerId,
      // 默认角色表：两个 AI（小满/大满）在房间里默认是管理员（规格四：AI 要听小群主/管理员，
      // 也需要 task.write 才接得了活）；小群主本人由 owner 字段决定，不写进这张表。
      roles: (() => {
        const base = { ...(b.roles && typeof b.roles === "object" ? b.roles : {}) };
        for (const id of BRIDGE_ENDS) if (PEOPLE[id]?.status === "active" && id !== ownerId) base[id] = base[id] || "admin";
        return base;
      })(),
      members: (() => {
        const m = Array.isArray(b.members) && b.members.length ? b.members.slice() : Object.keys(PEOPLE).filter((id) => PEOPLE[id]?.role === "admin");
        if (!m.includes(ownerId)) m.unshift(ownerId);
        if (!m.includes(superId())) m.unshift(superId());
        return [...new Set(m)];
      })(),
      createdBy: declaredBy,
    };
    db.rooms.push(r);
    const note = String(b.note || "").trim();
    // v3 建群卡：成员（带人设）、三句规则、该房记忆尾 3 条摘要。
    const byName = (PERSONAS[declaredBy] && PERSONAS[declaredBy].name) || declaredBy;
    const memberLine = r.members
      .map((id) => (PERSONAS[id] ? `${PERSONAS[id].name}（${PERSONAS[id].channel}）` : ((MEMBERS[id] && MEMBERS[id].name) || id)))
      .join("、");
    const memTail = readMemory(r.id).slice(-3);
    const memLine = memTail.length
      ? memTail.map((e) => `${e.by}：${String(e.text || "").replace(/\s+/g, " ").slice(0, 60)}`).join("；")
      : "暂无历史记忆";
    push(r.id, "system", "system", [
      `建群：${title}（本次由${byName}代建，群主：${(MEMBERS[r.owner] && MEMBERS[r.owner].name) || r.owner || "群主"}）${note}`,
      `成员：${memberLine}`,
      `规则：@谁谁接、不抢答、闲聊可以；机器话只进日志；交付带证据。`,
      `记忆：${memLine}`,
    ].join("\n"));
    metaRoom(r.id, "create");
    metaRooms("create");
    flush();
    return json(res, 200, { ok: true, room: r });
  }

  // v3 状态：GET 只读，不推进游标；游标由 POST /api/state/ack 显式推进。
  if (p === "/api/state" && req.method === "GET") {
    const since = Number(url.searchParams.get("since") || 0);
    const who = String(url.searchParams.get("who") || "").trim();
    const resp = { seq: db.seq, messages: db.messages.filter((m) => m.seq > since), rooms: db.rooms.map(roomSummary) };
    if (who) resp.cursor = cursorFloor(who);
    resp.acks = { ...(db.cursors || {}) };
    resp.roomCursors = roomCursorsSnapshot();
    // 台账 mtime（ms）：桥端 roomSessions() 有 5s 缓存，UI 靠这个时间差显示「配置生效倒计时」。
    // 文件不存在/读不到一律 0（UI 视为「没信息」，不提示），绝不因为这点装饰功能 500。
    try { resp.roomSessionsAt = fs.statSync(path.join(DATA, "room-sessions.json")).mtimeMs; }
    catch { resp.roomSessionsAt = 0; }
    return json(res, 200, resp);
  }
  if (p === "/api/state/ack" && req.method === "POST") {
    const b = await readBody(req);
    const who = String(b.who || "");
    if (!BRIDGE_ENDS.includes(who)) return json(res, 400, { ok: false, error: "who required: me|daman" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    const seq = Number(b.seq);
    if (!Number.isInteger(seq) || seq < 0) return json(res, 400, { ok: false, error: "seq must be a non-negative integer" });
    // C4：桥可以带 roomId，按房推进；不带则沿用老的全局游标（老桥/冷启动仍能跑）。
    const roomId = String(b.roomId || "").trim();
    if (roomId) {
      if (!db.rooms.some((r) => r.id === roomId)) return json(res, 404, { ok: false, error: "room not found" });
      const per = (db.roomCursors[who] && typeof db.roomCursors[who] === "object") ? db.roomCursors[who] : {};
      const cur = Number(per[roomId] || 0);
      if (seq < cur) return json(res, 400, { ok: false, error: `seq must not go backward (cursor=${cur})` });
      const advanced = seq > cur;
      if (advanced) { per[roomId] = seq; db.roomCursors[who] = per; save(); }
      return json(res, 200, { ok: true, who, roomId, cursor: per[roomId], floor: cursorFloor(who), advanced });
    }
    const cur = Number(db.cursors[who] || 0);
    // 游标只前进：小于当前值拒绝（防桥接重启倒退重放），等于当前值幂等放行。
    if (seq < cur) return json(res, 400, { ok: false, error: `seq must not go backward (cursor=${cur})` });
    const advanced = seq > cur;
    if (advanced) { db.cursors[who] = seq; save(); }
    return json(res, 200, { ok: true, who, cursor: db.cursors[who], advanced });
  }
  if (p === "/api/bridge/heartbeat" && req.method === "POST") {
    const b = await readBody(req);
    const who = String(b.who || "");
    if (!BRIDGE_ENDS.includes(who)) return json(res, 400, { ok: false, error: "who required: me|daman" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    // at 可选：正常心跳不传（写当前时间）；掉线演练/测试可注入历史时间戳伪造失联。
    let at = new Date().toISOString();
    const given = String(b.at || "").trim();
    if (given) {
      const t = Date.parse(given);
      if (!Number.isFinite(t)) return json(res, 400, { ok: false, error: "bad at: not an ISO date-time" });
      at = new Date(t).toISOString();
    }
    db.heartbeat[who] = at;
    // 心跳每 2 秒一次，全量 save() 会重写整份 rooms.json（约 160KB）。
    // 内存值每次更新（/status 与 patrol 读内存，实时性不变），落盘压到 5 秒一次。
    const hbSaveMs = typeof db._hbSaveMs === "number" ? db._hbSaveMs : 0;
    if (Date.now() - hbSaveMs >= 5000) { db._hbSaveMs = Date.now(); save(); }
    return json(res, 200, { ok: true, who, at });
  }
  if (p === "/api/bridge/status" && req.method === "GET") {
    const now = Date.now();
    const heartbeat = {};
    const down = {};
    for (const who of BRIDGE_ENDS) {
      const hb = db.heartbeat[who] ? Date.parse(db.heartbeat[who]) : 0;
      heartbeat[who] = db.heartbeat[who] || null;
      // 见过至少一次心跳、且距上次超过窗口才算 down；从没见过不算 down。
      down[who] = Boolean(hb) && now - hb > LIMITS.downAfterMs;
    }
    return json(res, 200, { heartbeat, down, downAfterMs: LIMITS.downAfterMs, takeoverAfterMs: LIMITS.takeoverAfterMs, wake: bridgeWake() });
  }

  // 唤醒治理（docs/wake-governance.md §4）：注入前闸门 + outcome 回填 + 台账查询。
  // 判定路径零模型调用：这里全是规则计数。鉴权与现有端点一致：平台 ?k= + 成员 body.key。
  if (p === "/api/wake/admit" && req.method === "POST") {
    const b = await readBody(req);
    const who = String(b.who || "");
    if (!BRIDGE_ENDS.includes(who)) return json(res, 400, { ok: false, error: "who required: me|daman" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    const seq = Number(b.seq);
    if (!Number.isInteger(seq)) return json(res, 400, { ok: false, error: "seq must be an integer" });
    const kind = String(b.kind || "").trim() || "text";
    if (kind !== "nudge" && !MESSAGE_KIND_SET.has(kind)) {
      return json(res, 400, { ok: false, error: "unknown kind: " + kind + "; allowed: " + MESSAGE_KINDS.join(", ") + ", nudge" });
    }
    return json(res, 200, wakeAdmit({ who, seq, roomId: String(b.roomId || ""), kind }));
  }
  if (p === "/api/wake/settle" && req.method === "POST") {
    const b = await readBody(req);
    const who = String(b.who || "");
    if (!BRIDGE_ENDS.includes(who)) return json(res, 400, { ok: false, error: "who required: me|daman" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    const wakeId = String(b.wakeId || "").trim();
    const outcome = String(b.outcome || "");
    if (!wakeId) return json(res, 400, { ok: false, error: "wakeId required" });
    if (!WAKE_OUTCOME_SET.has(outcome)) {
      return json(res, 400, { ok: false, error: "outcome must be one of " + WAKE_OUTCOMES.join("|") });
    }
    const r = wakeSettle({ who, wakeId, outcome });
    if (!r.ok) return json(res, r.code || 400, { ok: false, error: r.error });
    // 会议室联动（docs/meeting-room.md §5）：评审方对 report 卡沉默 → 议题按无意见收敛。
    if (outcome === "NO_REPLY") {
      const we = (wakeReadState().wakes || {})[wakeId];
      if (we && we.kind === "report") {
        const ms = meetingOnSilence({ roomId: we.roomId, reportSeq: we.seq, reviewer: who });
        if (ms.ok && ms.issue) {
          noteReviewLatency(ms.issue, "silent");
          const rr = room(we.roomId);
          const byName = (id) => (MEMBERS[id] && MEMBERS[id].name) || id;
          if (rr && rr.status === "open") {
            push(we.roomId, "system", "system", `评审收敛：${byName(who)} 对「${ms.issue.title || "报告"}」未提意见（沉默通过）`);
          }
        }
      }
    }
    return json(res, 200, { ok: true });
  }
  if (p === "/api/wake/state" && req.method === "GET") {
    const date = String(url.searchParams.get("date") || "").trim();
    const s = wakeGetState({ date });
    if (!s.ok) return json(res, 400, s);
    return json(res, 200, s);
  }
  if (p === "/api/meeting/state" && req.method === "GET") {
    // 平台 ?k 鉴权即可回看（只读）；roomId 可选过滤。
    const roomId = String(url.searchParams.get("roomId") || "").trim();
    return json(res, 200, { ok: true, issues: meetingList(roomId) });
  }

  // 观测开关（用户 2026-11-04）：评审端延迟超阈值是否记 warn + 阈值。GET 只读、PATCH 局部更新。
  // 开关落在 data/settings.json，改完立即生效（每次判定现读，不缓存），不用重启服务。
  if (p === "/api/settings" && req.method === "GET") {
    return json(res, 200, { ok: true, settings: readSettings() });
  }
  if (p === "/api/settings" && (req.method === "PATCH" || req.method === "POST")) {
    const b = await readBody(req);
    // B3（11-04）：写开关只归群主/超管。GET 仍对任何持平台钥匙的人只读开放。
    if (!permit(res, req, b, "room.settings", null)) return;
    const r = patchSettings(b);
    if (!r.ok) return json(res, r.code || 400, { ok: false, error: r.error });
    return json(res, 200, { ok: true, settings: r.settings });
  }
  // 最近 warn 事件（倒序）。?n=50&code=REVIEW_LATENCY 可选过滤。
  if (p === "/api/ops/events" && req.method === "GET") {
    const n = Math.min(200, Math.max(1, Number(url.searchParams.get("n") || 50) || 50));
    const code = String(url.searchParams.get("code") || "").trim();
    return json(res, 200, { ok: true, events: opsTail(n, { code }) });
  }

  // 归档：列表 / 单间读取 / 还原。真归档＝房间和消息搬出 rooms.json，落成独立文件。
  if (p === "/api/archive" && req.method === "GET") {
    const viewer = viewerOf(req, url);
    // 归档是 scrub 过的（members 已清空），所以只有超管和 AI 能看归档列表；
    // 受邀者的历史房间不在这里展示，避免「所有人都能翻归档」。
    const privileged = !!viewer && (viewer.sysRole === "super" || viewer.kind === "agent");
    return json(res, 200, { ok: true, rooms: privileged ? archiveList() : [] });
  }
  const a1 = p.match(/^\/api\/archive\/([^/]+)$/);
  if (a1 && req.method === "GET") {
    const fp = archivePath(a1[1]);
    if (!fs.existsSync(fp)) return json(res, 404, { ok: false, error: "not archived" });
    const d = JSON.parse(fs.readFileSync(fp, "utf8"));
    const viewer = viewerOf(req, url);
    const former = (d.room && d.room.__formerMembers) || [];
    if (!canSeeRoom(viewer, d.room) && !(viewer && former.includes(viewer.id))) {
      return json(res, 403, { ok: false, error: "not a member of this room" });
    }
    const msgs = (d.messages || []).slice().sort((a, b) => a.seq - b.seq);
    return json(res, 200, {
      room: { ...d.room, status: "archived" },
      archivedAt: d.archivedAt,
      archivedBy: d.archivedBy || "",
      members: roomMembers(d.room),
      messages: msgs,
      reports: msgs.filter((m) => m.kind === "report" && !m.recalled),
      progress: msgs.filter((m) => m.kind === "progress" && !m.recalled),
      reviews: msgs.filter((m) => m.kind === "review" && !m.recalled),
    });
  }
  const a2 = p.match(/^\/api\/archive\/([^/]+)\/restore$/);
  if (a2 && req.method === "POST") {
    const b = await readBody(req);
    if (!permit(res, req, b, "room.restore", null)) return;
    const who = whoOf(b, req);
    const r = archiveRestore(a2[1]);
    if (!r) return json(res, 404, { ok: false, error: "not archived" });
    return json(res, 200, { ok: true, room: r });
  }

  const m1 = p.match(/^\/api\/rooms\/([^/]+)$/);
  if (m1 && req.method === "GET") {
    const r = room(m1[1]);
    if (!r) return json(res, 404, { ok: false, error: "no such room" });
    if (!canSeeRoom(viewerOf(req, url), r)) return json(res, 403, { ok: false, error: "not a member of this room" });
    // since：只回比它新的消息。前端 3 秒一次拉增量（房间元信息+成员+新消息），不重复搬全量。
    const since = Number(url.searchParams.get("since") || 0) || 0;
    const msgs = db.messages.filter((m) => m.roomId === r.id && m.seq > since);
    return json(res, 200, {
      room: r,
      members: roomMembers(r),
      messages: msgs,
      reports: msgs.filter((m) => m.kind === "report" && !m.recalled),
      progress: msgs.filter((m) => m.kind === "progress" && !m.recalled),
      reviews: msgs.filter((m) => m.kind === "review" && !m.recalled),
    });
  }

  // v3 房间记忆：GET 平台鉴权即可回看（倒序）；POST 走成员 key，字数/条数受限。
  const mm = p.match(/^\/api\/rooms\/([^/]+)\/memory$/);
  if (mm) {
    const r = room(mm[1]);
    if (!r) return json(res, 404, { ok: false, error: "no such room" });
    if (req.method === "GET") {
      if (!canSeeRoom(viewerOf(req, url), r)) return json(res, 403, { ok: false, error: "not a member of this room" });
      return json(res, 200, memorySummary(r.id));
    }
    if (req.method !== "POST") return json(res, 405, { ok: false, error: "method not allowed" });
    const b = await readBody(req);
    const who = whoOf(b, req);
    if (!MEMBERS[who]) return json(res, 400, { ok: false, error: "unknown who" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    if (!(r.members || []).includes(who)) return json(res, 403, { ok: false, error: "not a member of this room" });
    if (r.status !== "open") return json(res, 409, { ok: false, error: "room is not open: " + r.status });
    const text = String(b.text || "");
    if (!text.trim()) return json(res, 400, { ok: false, error: "text required" });
    if (text.length > LIMITS.memoryMaxText) return json(res, 400, { ok: false, error: `text too long (max ${LIMITS.memoryMaxText})` });
    appendMemory(r.id, who, text);
    return json(res, 200, memorySummary(r.id));
  }

  // 附件下载/预览：/api/files/<roomId>/<stored>，需要平台 key（cookie 或 ?k=）
  const fm = p.match(/^\/api\/files\/([^/]+)\/([^/]+)$/);
  if (fm && req.method === "GET") {
    // 附件也是房间内容：只有在房间名单里的人（或超管/AI）能下。归档房间查不到 room，只放行特权身份。
    const fviewer = viewerOf(req, url);
    const froom = room(decodeURIComponent(fm[1]));
    const privileged = !!fviewer && (fviewer.sysRole === "super" || fviewer.kind === "agent");
    if (!privileged && (!froom || !canSeeRoom(fviewer, froom))) {
      return json(res, 403, { ok: false, error: "not a member of this room" });
    }
    const rdir = path.join(FILES, decodeURIComponent(fm[1]));
    const fpath = path.join(rdir, path.basename(decodeURIComponent(fm[2])));
    if (!fpath.startsWith(rdir) || !fs.existsSync(fpath)) return json(res, 404, { ok: false, error: "no such file" });
    const extf = path.extname(fpath).toLowerCase();
    const type = ({ ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp", ".gif": "image/gif", ".bmp": "image/bmp", ".pdf": "application/pdf", ".txt": "text/plain; charset=utf-8", ".md": "text/plain; charset=utf-8", ".csv": "text/csv; charset=utf-8", ".json": "application/json; charset=utf-8", ".zip": "application/zip" })[extf] || "application/octet-stream";
    res.writeHead(200, { "content-type": type, "cache-control": "private, max-age=3600" });
    fs.createReadStream(fpath).pipe(res);
    return;
  }

  const m2 = p.match(/^\/api\/rooms\/([^/]+)\/(messages|close|close-deny|close-approve|say|task|progress|report|review|floor|confirm|archive|invite|kick|join|leave|rules|file|recall|claim|setRole|transfer|mute)$/);
  if (m2) {
    const r = room(m2[1]);
    if (!r) return json(res, 404, { ok: false, error: "no such room" });
    const action = m2[2];

    // 已解散/已归档的房间只允许回看与归档操作，管理动作和发言一律拒绝
    if (["invite", "kick", "join", "rules", "task", "floor", "say", "progress", "report", "review", "confirm", "recall", "claim", "setRole", "transfer", "mute"].includes(action) && r.status !== "open") {
      return json(res, 409, { ok: false, error: "room is not open: " + r.status });
    }

    // 公共 body 只解析一次：POST 动作先统一校验显式 kind，再进入各分支。
    let parsedBody;
    const bodyOf = async () => {
      if (parsedBody === undefined) parsedBody = await readBody(req);
      return parsedBody;
    };
    if (req.method === "POST") {
      const ke = kindError(action, await bodyOf());
      if (ke) return json(res, 400, { ok: false, error: ke });
    }

    if (action === "messages" && req.method === "GET") {
      if (!canSeeRoom(viewerOf(req, url), r)) return json(res, 403, { ok: false, error: "not a member of this room" });
      const since = Number(url.searchParams.get("since") || 0);
      return json(res, 200, { seq: db.seq, messages: db.messages.filter((m) => m.roomId === r.id && m.seq > since) });
    }
    // 声明（claim）走带外通道，不发房间消息：群里不再出现「我接 #seq」。
    // 平局按服务端分配的自增 token 决定，先到先得，确定性且无需客户端时钟。
    if (action === "claim" && req.method === "POST") {
      const cb = await bodyOf();
      const cwho = whoOf(cb, req);
      if (!r.members.includes(cwho)) return json(res, 403, { ok: false, error: "not a member of this room" });
      if (!whoAllowed(cwho, cb, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
      const target = Number(cb.targetSeq || 0);
      if (!target) return json(res, 400, { ok: false, error: "targetSeq required" });
      r.claims = r.claims || {};
      const list = Array.isArray(r.claims[target]) ? r.claims[target] : [];
      const mine = list.find((c) => c.who === cwho);
      if (mine) return json(res, 200, { ok: true, claim: mine, duplicate: true, winner: list[0]?.who || cwho });
      const claim = { who: cwho, token: ++db.claimSeq, at: new Date().toISOString() };
      list.push(claim);
      r.claims[target] = list;
      // 只留最近 200 条目标的声明，避免 rooms.json 无限膨胀
      const keys = Object.keys(r.claims);
      const keep = LIMITS.claimRetention;
      if (keys.length > keep) for (const k of keys.slice(0, keys.length - keep)) delete r.claims[k];
      flush();
      const winner = [...list].sort((a, b) => a.token - b.token)[0]?.who || cwho;
      return json(res, 200, { ok: true, claim, winner, count: list.length });
    }
    if (action === "claim" && req.method === "GET") {
      const target = Number(url.searchParams.get("targetSeq") || 0);
      const list = (r.claims && r.claims[target]) || [];
      const sorted = [...list].sort((a, b) => a.token - b.token);
      return json(res, 200, { ok: true, targetSeq: target, claims: sorted, winner: sorted[0]?.who || null });
    }
    if (action === "close" && req.method === "POST") {
      // 解散：小群主可以发起，但必须过超级群主 5 秒确认闸；超级群主自己解散直接放行。
      const cb = await bodyOf();
      const gate = permit(res, req, cb, "room.close", r);
      if (!gate) return;
      const cwho = gate.person.id;
      if (r.status === "closed") return json(res, 200, { ok: true, room: r, already: true });
      // 需要超管确认时先挂起，由 /close-deny、/close-approve 或超时决定。
      // 注意：requestRoomClose 返回 null 表示「已经有一条在等」，这时必须 409 退出，
      // 不能往下走到「直接解散」——那等于把确认闸整个绕过。
      if (needsSuperConfirm(gate.person, r)) {
        const pendingReq = requestRoomClose(r, gate.person);
        if (!pendingReq) return json(res, 409, { ok: false, error: "已有一条待确认的解散请求", pending: r.pendingClose });
        r.pendingClose.status = "pending";
        // 这条分支不落系统卡（等超管裁决），所以 push 上的自动通知够不着——必须手写，
        // 否则左栏「待确认解散」要等 6 秒轮询才出现，看着像没反应。
        metaRoom(r.id, "close-pending");
        metaRooms("close-pending");
        flush();
        return json(res, 202, { ok: true, pending: true, request: pendingReq, room: r });
      }
      r.status = "closed"; r.closedAt = new Date().toISOString(); r.closedBy = cwho;
      if (r.pendingClose && r.pendingClose.status === "pending") {
        r.pendingClose.status = "approved"; r.pendingClose.settledBy = cwho; r.pendingClose.settledAt = r.closedAt;
      }
      meetingAbortRoom(r.id); // 会议室：房间解散 → 该房 open 议题全部 abort
      push(r.id, "system", "system", "房间已解散（数据保留，可回看）");
      metaRoom(r.id, "close");
      metaRooms("close");
      flush();
      return json(res, 200, { ok: true, room: r });
    }

    // 超管对挂起的解散请求表态：deny 保留房间，approve 立即解散。
    if ((action === "close-deny" || action === "close-approve") && req.method === "POST") {
      const db2 = await bodyOf();
      const gate = permit(res, req, db2, "room.close", r);
      if (!gate) return;
      if (effectiveRole(gate.person, r) !== "super") {
        return json(res, 403, { ok: false, error: "只有超级群主能裁决解散请求" });
      }
      const verdict = action === "close-deny" ? "denied" : "approved";
      const reqObj = r.pendingClose;
      if (!reqObj || reqObj.status !== "pending") return json(res, 409, { ok: false, error: "没有待裁决的解散请求" });
      if (verdict === "denied") {
        settleClose(r.id, "denied", gate.person.id);
      } else {
        settleClose(r.id, "approved", gate.person.id);
        meetingAbortRoom(r.id);
      }
      metaRoom(r.id, "close-" + verdict);
      metaRooms("close");
      return json(res, 200, { ok: true, verdict, room: r });
    }

    if (action === "file" && req.method === "POST") {
      const fb = await bodyOf();
      const fwho = whoOf(fb, req);
      if (!r.members.includes(fwho)) return json(res, 403, { ok: false, error: "not a member of this room" });
      if (!whoAllowed(fwho, fb, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
      const qseq = Number(fb.quoteSeq || 0);
      const quote = quoteOf(r.id, qseq);
      if (qseq && !quote) return json(res, 400, { ok: false, error: "quote target not found or recalled" });
      const name = path.basename(String(fb.name || "file"));
      const mime = String(fb.mime || "application/octet-stream").slice(0, 120).replace(/[\r\n]/g, "");
      const ext = path.extname(name).toLowerCase();
      if (!ALLOW_EXT.has(ext)) return json(res, 415, { ok: false, error: "file type not allowed: " + (ext || "none") });
      if (/^video\//i.test(mime)) return json(res, 415, { ok: false, error: "video not supported" });
      let buf;
      try { buf = Buffer.from(String(fb.data || ""), "base64"); } catch { return json(res, 400, { ok: false, error: "bad base64" }); }
      if (!buf.length) return json(res, 400, { ok: false, error: "data (base64) required" });
      if (buf.length > MAX_FILE) return json(res, 413, { ok: false, error: "file too large (max " + Math.round(MAX_FILE / 1048576) + "MB)" });
      const sha = crypto.createHash("sha256").update(buf).digest("hex");
      const dir = path.join(FILES, r.id);
      fs.mkdirSync(dir, { recursive: true });
      const stored = sha.slice(0, 16) + "-" + name.replace(/[^\w.\-\u4e00-\u9fa5]+/g, "_").slice(0, 80);
      fs.writeFileSync(path.join(dir, stored), buf);
      const meta = { name, size: buf.length, mime, sha256: sha, path: path.join(dir, stored), url: "/api/files/" + encodeURIComponent(r.id) + "/" + encodeURIComponent(stored) };
      const m = push(r.id, fwho, "file", String(fb.note || "").slice(0, 2000) || "发来文件：" + name, { file: meta, quote, msgId: String(fb.msgId || "") });
      return json(res, 200, { ok: true, message: m, file: meta });
    }

    if (action === "archive" && req.method === "POST") {
      const ab = await bodyOf();
      const gate = permit(res, req, ab, "room.archive", r);
      if (!gate) return;
      const awho = gate.person.id;
      if (r.status !== "closed") return json(res, 409, { ok: false, error: "先解散才能归档" });
      const payload = archiveMove(r, awho);
      metaRooms("archive");
      return json(res, 200, { ok: true, archived: payload.room.id, file: path.basename(archivePath(payload.room.id)) });
    }

    if (action === "invite" && req.method === "POST") {
      const ib = await bodyOf();
      const gate = permit(res, req, ib, "member.invite", r);
      if (!gate) return;
      // 规格三.1：一人一链。点「拉人」只生成链接，先不认人——谁来、叫什么名字，由点链接的人自己填。
      // 老调用（带 memberId）仍按旧语义直接把人拉进房，保留兼容。
      if (ib.memberId) {
        const target = personOf(String(ib.memberId || ""));
        if (!target || target.status !== "active") return json(res, 404, { ok: false, error: "no such member" });
        if (isBlacklisted(r, target.name)) return json(res, 403, { ok: false, error: "该身份已被移出过，需要换名字重新邀请" });
        if (r.members.includes(target.id)) return json(res, 409, { ok: false, error: "already in room" });
        r.members.push(target.id);
        if (r.roles) delete r.roles[target.id];
        push(r.id, "system", "system", `群主把 ${target.name} 拉进房间`);
        metaRoom(r.id, "invite");
        metaRooms("invite");
        flush();
        return json(res, 200, { ok: true, room: r, code: null });
      }
      r.invites = r.invites || {};
      const token = newToken();
      r.invites[token] = {
        token,
        by: gate.person.id,
        byName: gate.person.name,
        at: new Date().toISOString(),
        status: INVITE_STATUS.OPEN,
        holder: null,
        seatName: "",
        usedAt: null,
        leftAt: null,
      };
      push(r.id, "system", "system", `${gate.person.name} 生成了一条一次性邀请链接`);
      flush();
      const base = String(ib.base || "").trim().replace(/\/+$/, "");
      const url2 = (base || "http://127.0.0.1:" + PORT) + "/i/" + token + "?k=" + KEY;
      return json(res, 200, { ok: true, token, url: url2, roomId: r.id });
    }

    if (action === "kick" && req.method === "POST") {
      const kb = await bodyOf();
      const gate = permit(res, req, kb, "member.kick", r);
      if (!gate) return;
      const target = String(kb.memberId || "");
      if (!r.members.includes(target)) return json(res, 404, { ok: false, error: "not in room" });
      // 等级铁律（Discord/Matrix 都是这条）：动不了 rank ≥ 自己的人。
      // 管理员之间不能互踢，只有群主/超管能动管理员；超管谁都动不了。
      const targetRole = effectiveRole(actorOf(target) || { id: target }, r);
      if (!canActOn(gate.role, targetRole)) {
        return json(res, 403, { ok: false, error: "不能踢同级或更高身份的人", actorRole: gate.role, targetRole });
      }
      r.members = r.members.filter((x) => x !== target);
      // 规格三.1：被踢 → 他的邀请链接直接报废 + 名字进本房黑名单，要进人只能重新生成。
      killInvitesOf(r, target, "kicked");
      if (r.roles) delete r.roles[target];
      const tname = (personOf(target) || {}).name || target;
      r.blacklist = Array.isArray(r.blacklist) ? r.blacklist : [];
      if (!isBlacklisted(r, tname)) r.blacklist.push(blacklistEntry(tname, r.id, gate.person.id));
      // 被踢的人可能正持话筒，不清就会卡住整间房 90 秒
      if (r.floor && r.floor.holder === target) r.floor = null;
      // 会话也要当场吊销：否则他刷新页面还顶着自己的身份看这间房。
      revokeSessionsOf(target, "kicked:" + r.id);
      push(r.id, "system", "system", `${gate.person.name} 把 ${tname} 移出房间（邀请链接作废）`);
      metaRoom(r.id, "kick");
      metaRooms("kick");
      flush();
      return json(res, 200, { ok: true, room: r });
    }

    // 设/撤管理员（规格二.1/二.2：只有小群主与超管能授权，模型无主动授权资格）
    if (action === "setRole" && req.method === "POST") {
      const sb = await bodyOf();
      const gate = permit(res, req, sb, "member.setRole", r);
      if (!gate) return;
      const target = String(sb.memberId || "");
      const role = String(sb.role || ""); // "admin" | "member"
      if (!["admin", "member"].includes(role)) return json(res, 400, { ok: false, error: "role must be admin|member" });
      if (!r.members.includes(target)) return json(res, 404, { ok: false, error: "not in room" });
      if (target === r.owner) return json(res, 400, { ok: false, error: "小群主的身份只能走转让，不能直接改" });
      const tRole = effectiveRole(actorOf(target) || { id: target }, r);
      if (!canActOn(gate.role, tRole)) return json(res, 403, { ok: false, error: "不能改动同级或更高身份的人", actorRole: gate.role, targetRole: tRole });
      const tn = (personOf(target) || {}).name || target;
      if (role === "admin") {
        r.roles = r.roles || {};
        r.roles[target] = "admin";
        push(r.id, "system", "system", `${gate.person.name} 把 ${tn} 设为管理员`);
      } else {
        if (r.roles) delete r.roles[target];
        push(r.id, "system", "system", `${gate.person.name} 撤销了 ${tn} 的管理员身份`);
      }
      metaRoom(r.id, "setRole");
      flush();
      return json(res, 200, { ok: true, room: r, roles: r.roles || {} });
    }

    // 转让小群主（规格二.1：转让后原小群主降为管理员；超管发起仅字面变化不降权）
    if (action === "transfer" && req.method === "POST") {
      const tb = await bodyOf();
      const gate = permit(res, req, tb, "member.transfer", r);
      if (!gate) return;
      const target = String(tb.memberId || "");
      if (!r.members.includes(target)) return json(res, 404, { ok: false, error: "not in room" });
      if (target === r.owner) return json(res, 400, { ok: false, error: "他已经是小群主" });
      const tPerson = actorOf(target);
      if (!tPerson) return json(res, 404, { ok: false, error: "unknown target" });
      // 超管自己发起转让：只是把字面 owner 挪过去，超管降权是不可能的（自己仍是 super）。
      const prevOwner = r.owner;
      r.owner = target;
      r.roles = r.roles || {};
      r.roles[target] = "owner";
      if (prevOwner) r.roles[prevOwner] = "admin"; // 原小群主降为管理员
      push(r.id, "system", "system", `${gate.person.name} 把小群主转让给 ${tPerson.name}${prevOwner ? `，${(personOf(prevOwner) || {}).name || prevOwner} 转为管理员` : ""}`);
      metaRoom(r.id, "transfer");
      flush();
      return json(res, 200, { ok: true, room: r });
    }

    // 禁言（房间规则性质的软限制，规格二.2 管理员可用）
    if (action === "mute" && req.method === "POST") {
      const mb = await bodyOf();
      const gate = permit(res, req, mb, "member.mute", r);
      if (!gate) return;
      const target = String(mb.memberId || "");
      if (!r.members.includes(target)) return json(res, 404, { ok: false, error: "not in room" });
      const tRole = effectiveRole(actorOf(target) || { id: target }, r);
      if (!canActOn(gate.role, tRole)) return json(res, 403, { ok: false, error: "不能禁言同级或更高身份的人", actorRole: gate.role, targetRole: tRole });
      r.muted = Array.isArray(r.muted) ? r.muted : [];
      const on = mb.muted !== false;
      r.muted = on ? [...new Set([...r.muted, target])] : r.muted.filter((x) => x !== target);
      const tn = (personOf(target) || {}).name || target;
      push(r.id, "system", "system", `${gate.person.name} ${on ? "禁言" : "解除禁言"} ${tn}`);
      metaRoom(r.id, "mute");
      flush();
      return json(res, 200, { ok: true, room: r, muted: r.muted });
    }

    if (action === "join" && req.method === "POST") {
      // 真人持邀请码入场：校验码、过期、一次性，成功后自己就是成员
      const jb = await bodyOf();
      const code = String(jb.code || "");
      const inv = (r.invites || {})[code];
      if (!inv) return json(res, 404, { ok: false, error: "邀请码无效" });
      if (inv.used) return json(res, 409, { ok: false, error: "邀请码已用过" });
      if (Date.parse(inv.expiresAt) < Date.now()) return json(res, 410, { ok: false, error: "邀请码已过期" });
      const target = personOf(inv.memberId);
      if (!target || target.status !== "active") return json(res, 404, { ok: false, error: "成员已失效" });
      if (String(jb.name || "") && String(jb.name).trim() !== target.name) {
        return json(res, 409, { ok: false, error: "邀请码属于 " + target.name });
      }
      inv.used = true;
      if (!r.members.includes(target.id)) r.members.push(target.id);
      push(r.id, "system", "system", `${target.name} 通过邀请码加入房间`);
      metaRoom(r.id, "join");
      metaRooms("join");
      flush();
      return json(res, 200, { ok: true, room: r, memberId: target.id });
    }

    if (action === "rules" && req.method === "POST") {
      const rb = await bodyOf();
      const gate = permit(res, req, rb, "room.rules", r);
      if (!gate) return;
      const allowed = ["floorMs", "ackRequired", "quietMs", "maxParallelTasks", "note"];
      const next = Object.assign({}, r.rules || {});
      for (const k of allowed) {
        if (!(k in (rb.rules || {}))) continue;
        if (k === "note") next.note = String(rb.rules.note).slice(0, 500);
        else if (k === "ackRequired") next.ackRequired = !!rb.rules.ackRequired;
        else {
          const n = Number(rb.rules[k]);
          if (!Number.isFinite(n) || n < 0) return json(res, 400, { ok: false, error: "bad value: " + k });
          next[k] = Math.round(n);
        }
      }
      r.rules = next;
      push(r.id, "system", "system", `群主更新房间规则：${JSON.stringify(next)}`);
      metaRoom(r.id, "rules");
      flush();
      return json(res, 200, { ok: true, room: r });
    }

    const b = await bodyOf();
    const who = whoOf(b, req);
    if (!MEMBERS[who]) return json(res, 400, { ok: false, error: "unknown who" });
    if (!whoAllowed(who, b, req)) return json(res, 403, { ok: false, error: "identity key mismatch" });
    // 超管不受「必须在这间房里」限制：任意房间满血是规格第零条。
    if (!(r.members || []).includes(who) && sysRoleOf(who) !== "super") {
      return json(res, 403, { ok: false, error: "not a member of this room" });
    }
    if (r.status === "closed") return json(res, 409, { ok: false, error: "room closed" });

    if (action === "recall") {
      // 撤回：作者可撤自己的，群主可撤任何人的；系统消息不可撤，解散后不可撤
      const seq = Number(b.seq || 0);
      const m = db.messages.find((x) => x.roomId === r.id && x.seq === seq);
      if (!m) return json(res, 404, { ok: false, error: "no such message" });
      if (m.kind === "system") return json(res, 400, { ok: false, error: "system message can not be recalled" });
      if (m.who !== who && !can(effectiveRole(actorOf(who) || { id: who }, r), "message.recallAny")) {
        return json(res, 403, { ok: false, error: "只能撤回自己的消息（群主/管理员可撤回他人的）" });
      }
      if (m.recalled) return json(res, 200, { ok: true, message: m, already: true });
      m.recalled = true;
      m.recalledAt = new Date().toISOString();
      m.recalledBy = who;
      m.text = "";
      m.quote = null;
      m.attachments = [];
      m.file = null;
      flush();
      broadcast(r.id, m);
      return json(res, 200, { ok: true, message: m });
    }

    if (action === "say") {
      if (!String(b.text || "").trim()) return json(res, 400, { ok: false, error: "empty" });
      const qseq = Number(b.quoteSeq || 0);
      const quote = quoteOf(r.id, qseq);
      if (qseq && !quote) return json(res, 400, { ok: false, error: "quote target not found or recalled" });
      // 协作约定：行首 ACK <taskId> <收到|已派|卡住> 视为回执，单独一种 kind，便于统计与去重
      const ack = String(b.text).match(/^ACK\s+(\S+)\s+(收到|已派|卡住)\s*$/);
      if (ack) {
        const m = push(r.id, who, "ack", String(b.text).trim(), { taskId: ack[1], status: ack[2], quote, msgId: String(b.msgId || "") });
        return json(res, 200, { ok: true, message: m });
      }
      const m = push(r.id, who, "text", b.text, { quote, msgId: String(b.msgId || "") });
      // 滑动租约：持话筒的人发言即续租（与 floor-spec 的 90s 滑动窗口一致）
      if (r.floor && r.floor.holder === who) { r.floor.since = new Date().toISOString(); r.floor.expiresAt = new Date(Date.now() + FLOOR_MS).toISOString(); }
      return json(res, 200, { ok: true, message: m });
    }
    if (action === "floor") {
      // 话筒：同一时刻只有一个发言人。claim 抢占（无主/过期/自己），yield 显式交给指定人，release 归还。
      const now = Date.now();
      const f = (r.floor && r.floor.holder && now - f_ts(r.floor) < FLOOR_MS) ? r.floor : null;
      const act = String(b.action || "claim");
      if (act === "claim") {
        if (f && f.holder !== who) return json(res, 409, { ok: false, error: "floor held by " + f.holder, floor: f });
        r.floor = { holder: who, since: new Date().toISOString(), expiresAt: new Date(now + FLOOR_MS).toISOString() };
      } else if (act === "release") {
        if (f && f.holder !== who) return json(res, 409, { ok: false, error: "not holder" });
        r.floor = null;
      } else if (act === "yield") {
        const to = String(b.to || "");
        if (!MEMBERS[to]) return json(res, 400, { ok: false, error: "bad target" });
        if (f && f.holder !== who) return json(res, 409, { ok: false, error: "not holder" });
        r.floor = { holder: to, since: new Date().toISOString(), expiresAt: new Date(now + FLOOR_MS).toISOString(), yieldedBy: who };
      } else return json(res, 400, { ok: false, error: "bad action" });
      flush();
      return json(res, 200, { ok: true, floor: r.floor });
    }
    if (action === "confirm") {
      // 收工确认：done=我已完成；agree=同意汇总。只记账，不触发解散（解散权在群主）。
      const taskId = String(b.taskId || "").trim();
      const say2 = String(b.text || "").trim();
      if (!taskId || !["done", "agree"].includes(say2)) return json(res, 400, { ok: false, error: "taskId + text(done|agree) required" });
      const m = push(r.id, who, "confirm", say2, { taskId });
      return json(res, 200, { ok: true, message: m });
    }
    if (action === "task") {
      const taskId = String(b.taskId || "").trim();
      const text = String(b.text || "").trim();
      if (!taskId || !text) return json(res, 400, { ok: false, error: "taskId/text required" });
      const assignee = String(b.assignee || "").trim();
      if (assignee && !r.members.includes(assignee)) {
        return json(res, 400, { ok: false, error: `assignee not in room: ${assignee}` });
      }
      const deadlineAt = b.deadlineAt ? String(b.deadlineAt) : "";
      if (deadlineAt && Number.isNaN(Date.parse(deadlineAt))) {
        return json(res, 400, { ok: false, error: "deadlineAt must be ISO date-time" });
      }
      const m = push(r.id, who, "task", text, { taskId, assignee, deadlineAt });
      return json(res, 200, { ok: true, message: m });
    }
    if (action === "progress") {
      const m = push(r.id, who, "progress", String(b.text || ""), { taskId: String(b.taskId || ""), percent: Number(b.percent ?? -1) });
      return json(res, 200, { ok: true, message: m });
    }
    if (action === "report") {
      const m = push(r.id, who, "report", String(b.summary || ""), {
        taskId: String(b.taskId || ""),
        title: String(b.title || "报告"),
        status: String(b.status || "done"),
        links: Array.isArray(b.links) ? b.links.slice(0, 20) : [],
      });
      // 会议室（docs/meeting-room.md）：report 落库即开议题；--noreview 逃生口（只记台账不投评审）。
      if (who === "me" || who === "daman") {
        const mo = b.noreview === true ? null : meetingOpen({ roomId: r.id, author: who, reportSeq: m.seq, reportTs: String(m.ts || ""), title: String(b.title || ""), taskId: String(b.taskId || "") });
        if (mo && !mo.ok) return json(res, 400, { ok: false, error: mo.error });
        if (mo) m.meeting = mo.issue;
      }
      return json(res, 200, { ok: true, message: m });
    }
    if (action === "review") {
      const m = push(r.id, who, "review", String(b.text || ""), { target: String(b.target || "all") });
      // verdict 参数化收敛：pass→收敛；issues→等对方再交（roundCap 兜底）。
      const verdict = String(b.verdict || "").trim();
      if (verdict) {
        if (!MEETING_VERDICT_SET.has(verdict)) {
          return json(res, 400, { ok: false, error: "bad verdict: " + verdict + "; allowed: pass|issues" });
        }
        const mr = meetingRespond({ roomId: r.id, reviewer: who, verdict, text: String(b.text || ""), });
        if (mr.ok && mr.issue) {
          m.meeting = mr.issue;
          noteReviewLatency(mr.issue, mr.issue.status === "converged" ? String(mr.issue.closedBy || "pass") : "round");
          if (mr.issue.status === "converged") {
            const byName = (id) => (MEMBERS[id] && MEMBERS[id].name) || id;
            const line = mr.issue.closedBy === "pass"
              ? `评审收敛：${byName(who)} 对「${mr.issue.title || "报告"}」评审通过`
              : `评审收敛：已达 ${MEETING.roundCap} 轮，请群主裁决「${mr.issue.title || "报告"}」`;
            push(r.id, "system", "system", line);
          }
        }
      }
      return json(res, 200, { ok: true, message: m });
    }
  }

  if (p === "/events") {
    const r = url.searchParams.get("room") || "*";
    res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-store", connection: "keep-alive" });
    res.write(`event: init\ndata: ${JSON.stringify({ seq: db.seq })}\n\n`);
    const c = { res, room: r };
    clients.add(c);
    const ping = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 20_000);
    req.on("close", () => { clearInterval(ping); clients.delete(c); });
    return;
  }

  json(res, 404, { ok: false, error: "not found" });
});

/* ---------- 静默巡检：服务自己盯，不依赖外部定时器 ---------- */
const PATROL_MS = Number(process.env.DS_TEAMS_PATROL_MS) || 60_000;
const PATROL_COOLDOWN_MS = 10 * 60_000;
// dispatch 台账：claim 后走台账收尾的任务不会在房间里留 confirm，巡检必须认它。
// v3 起改用 dispatch-lib 的 env 感知读法（DS_TEAMS_LEDGER 可指临时台账，测试/演练用）。
function readLedger() {
  try {
    const j = dispatchReadLedger();
    return (j && j.tasks) || {};
  } catch {
    return {};
  }
}

/* ---------- v3 掉线接管：心跳失联端的旧任务转给存活端 ---------- */
function bridgeDown(now) {
  const down = {};
  for (const who of BRIDGE_ENDS) {
    const hb = db.heartbeat[who] ? Date.parse(db.heartbeat[who]) : 0;
    down[who] = Boolean(hb) && now - hb > LIMITS.downAfterMs;
  }
  return down;
}
// A12（2026-11-08）：判据抽到 takeover-lib（纯函数，可测）。旧版只看 claimedAt 超 15 分钟，
// 忙端刚接的活也会被转走，那一端回来后两端各跑一遍。现在还要「近 5 分钟没进度」且「30 分钟内没转过」。
function takeoverPass(now) {
  const down = bridgeDown(now);
  const ledger = dispatchReadLedger();
  const tasks = (ledger && ledger.tasks) || {};
  const moves = decideTakeovers({ now, down, tasks, limits: LIMITS });
  if (!moves.length) return;
  for (const mv of moves) {
    const e = tasks[mv.key];
    applyTakeover(e, { ...mv, now });
    const r = room(e.roomId);
    if (r && r.status === "open") {
      const fromName = (PERSONAS[mv.from] && PERSONAS[mv.from].name) || (MEMBERS[mv.from] && MEMBERS[mv.from].name) || mv.from;
      const toName = (PERSONAS[mv.to] && PERSONAS[mv.to].name) || (MEMBERS[mv.to] && MEMBERS[mv.to].name) || mv.to;
      push(r.id, "system", "system", `⚠️ ${fromName}掉线，任务「${mv.title}」转给${toName}`);
    }
  }
  try { dispatchWriteLedger(ledger); } catch (e) { console.error("takeover writeLedger", e.message); }
  save();
}
// wake 兜底回收用的消息视图：活房从 db 取，已归档房从 data/archive/<id>.json 取。
function roomMessagesForWake(roomId) {
  if (!roomId) return [];
  const live = db.messages.filter((m) => m.roomId === roomId);
  if (live.length) return live;
  try {
    const d = JSON.parse(fs.readFileSync(archivePath(roomId), "utf8"));
    return Array.isArray(d.messages) ? d.messages : [];
  } catch {
    return [];
  }
}
function patrolOnce(now = Date.now()) {
  takeoverPass(now); // v3：先做掉线接管，再做常规静默巡检
  // 断桥遗留 wake 兜底（C5）：桥重启/断线丢了 outcome watcher 时，按房间消息证据补 REPLIED/NO_REPLY。
  try {
    const reaped = wakeSweepStale({ now, lookup: roomMessagesForWake });
    if (reaped.length) console.log(`[ds-teams] wake sweep: ${reaped.map((x) => x.wakeId + "=" + x.outcome).join(", ")}`);
  } catch (e) {
    console.error("wake sweep", e.message);
  }
  for (const r of db.rooms) {
    if (r.status !== "open") continue;
    const quiet = Number(r.rules && r.rules.quietMs) || 180_000;
    const msgs = db.messages.filter((m) => m.roomId === r.id);
    if (!msgs.length) continue;
    // idle 只认实质消息：system/patrol 不算，否则巡检自己会刷新静默计时
    const real = msgs.filter((m) => SUBSTANTIVE_KINDS.has(m.kind));
    if (!real.length) continue;
    const last = real[real.length - 1];
    const idle = now - Date.parse(last.ts);
    if (!(idle >= quiet)) continue;
    const tasks = msgs.filter((m) => m.kind === "task");
    if (!tasks.length) continue;
    // 任务算「未完成」：房间内没有 confirm done / 报告 done，且 dispatch 台账里不是 done/aborted
    // 只看房间消息会漏掉「claim 后走台账收尾、房间没发 confirm」的任务，导致巡检一直点名已完工的节点
    const ledger = readLedger();
    const pending = tasks.filter((t) => {
      const entry = ledger[`${r.id}::${t.taskId}`];
      const closed = CLOSED.has(entry && entry.state);
      if (closed) return false;
      return !msgs.some((m) => m.taskId === t.taskId && ((m.kind === "confirm" && m.text === "done") || (m.kind === "report" && !m.recalled && String(m.status) === "done")));
    });
    if (!pending.length) continue;
    const lastPatrol = r.lastPatrolAt ? Date.parse(r.lastPatrolAt) : 0;
    if (now - lastPatrol < PATROL_COOLDOWN_MS) continue;
    r.lastPatrolAt = new Date(now).toISOString();
    // 三级升级：1 点名负责人 → 2 抄送群主 → 3 建议置 blocked（连续三轮没人动）
    r.patrolLevel = Number(r.patrolLevel || 0) + 1;
    const level = Math.min(r.patrolLevel, 3);
    const ids = pending.map((t) => t.taskId).join(" / ");
    const assignees = [...new Set(pending.map((t) => t.assignee).filter(Boolean))];
    const at = assignees.length ? assignees.map((id) => "@" + ((personOf(id) || {}).name || id)).join(" ") : "@" + (r.members || []).map((id) => (personOf(id) || {}).name || id).join(" @");
    const idleMin = Math.round(idle / 60_000);
    let text;
    if (level === 1) text = `静默巡检（1/3）：${idleMin} 分钟没有实质进展，${at} 请报进度或说明阻塞。未完成：${ids}`;
    else if (level === 2) text = `静默巡检（2/3）：${idleMin} 分钟仍无进展，抄送群主。${at} 未完成：${ids}。有事直接说卡在哪，不要空等。`;
    else text = `静默巡检（3/3）：${idleMin} 分钟无进展，已连续三轮无人响应。建议把这几个节点置为 blocked 并在群里说明原因：${ids}（${at}）`;
    push(r.id, "system", "patrol", text);
  }
  flush();
}
setInterval(() => { try { patrolOnce(); } catch (e) { console.error("patrol", e.message); } }, PATROL_MS).unref();

/* ---------- 台账维护（T1 2026-11-07）----------
 * 以前 promote/watchdog 只活在大满端桥的 15 秒 tick 里：桌面端不开，新任务永远 queued，
 * 房间看着「在干活」其实队列没动。这里让服务自己每 5 秒跑一次：
 *   watchdog 先把超时的标掉（名额随之放出），promote 再把 queued 提到 claimed。
 * 提醒 agent 去 spawn 仍然只能由桥做（服务端注入不了宿主会话），桥那边捞的是
 * claimedAwaitingSpawn()，所以「提升出来没人叫」的空窗不会超过桥的一轮。
 * DS_TEAMS_DISPATCH_TICK_MS 可调，0 关闭（演练/调试用）。 */
const DISPATCH_TICK_MS = Number(process.env.DS_TEAMS_DISPATCH_TICK_MS ?? 5000);
if (DISPATCH_TICK_MS > 0) {
  setInterval(() => {
    try {
      // A12（2026-11-08）：接管要排在 watchdog 前面。否则失联端名下的卡会先被判 timeout，
      // 接管看到非 ACTIVE 就跳过了——活明明还能接到另一端手里，却被记成了超时。
      takeoverPass(Date.now());
      const wd = dispatchWatchdog();
      if (wd.length) console.error("[dispatch] watchdog " + wd.map((x) => `${x.key}(${x.reason})`).join(", "));
      const pr = dispatchPromote();
      if (pr.length) console.error("[dispatch] promote " + pr.join(", "));
    } catch (e) { console.error("[dispatch] tick " + e.message); }
  }, DISPATCH_TICK_MS).unref();
}

/* ---------- 单实例锁（2026-11-05）----------
 * 两个实例共用同一份 data/*.json：后写盘的那个会把先写的 sessions.json / rooms.json 覆盖掉。
 * 表现出来就是「刚刚还是群主，突然说我不是群主了」「消息发出去不见了」——查了半天是双开。
 * 这里抢 data/server.lock：锁里记的 pid 还活着就带原因退出，别再悄悄起第二个。
 * 临时台账实例（DS_TEAMS_LEDGER）已经自己开一份数据目录，不会撞锁；
 * 只有显式 DS_TEAMS_ALLOW_MULTI=1 才跳过这道闸。 */
const LOCK_FILE = path.join(DATA, "server.lock");
function acquireLock() {
  if (process.env.DS_TEAMS_ALLOW_MULTI === "1") return;
  try {
    const old = JSON.parse(fs.readFileSync(LOCK_FILE, "utf8"));
    if (old && old.pid && old.pid !== process.pid && pidAlive(old.pid)) {
      console.error(`[ds-teams] 已经有一个实例在跑（pid ${old.pid}，端口 ${old.port}）。`);
      console.error("[ds-teams] 同一份 data/ 开两个会互相覆盖会话和房间，所以这里直接退出。要强行多开：DS_TEAMS_ALLOW_MULTI=1。");
      process.exit(1);
    }
  } catch {}
  const write = () => {
    try { fs.writeFileSync(LOCK_FILE, JSON.stringify({ pid: process.pid, port: PORT, at: Date.now() })); } catch {}
  };
  write();
  const beat = setInterval(write, 10_000);
  beat.unref();
  process.on("exit", () => {
    try {
      const cur = JSON.parse(fs.readFileSync(LOCK_FILE, "utf8"));
      if (cur && cur.pid === process.pid) fs.unlinkSync(LOCK_FILE);
    } catch {}
  });
}

function listen(port, tries = 0, requested = port) {
  server.once("error", (err) => {
    if (err.code === "EADDRINUSE") {
      // 默认不漂移：端口被占多半是旧实例没退，悄悄 +1 会让网页连着旧数据，看着像「重启没生效」。
      if (process.env.DS_TEAMS_ALLOW_PORT_SHIFT === "1" && tries < 10) return listen(port + 1, tries + 1, requested);
      console.error(`[ds-teams] 端口 ${port} 被占用（多半是旧实例没退）。先结束旧进程再启动；确需漂移用 DS_TEAMS_ALLOW_PORT_SHIFT=1，或换端口 DS_TEAMS_PORT=<port>。`);
      console.error(err);
      process.exit(1);
    }
    console.error(err); process.exit(1);
  });
  // 默认只绑回环；要手机/局域网访问用 DS_TEAMS_HOST=0.0.0.0 启动（防火墙需放行 node）。
  const host = process.env.DS_TEAMS_HOST || "127.0.0.1";
  server.listen(port, host, () => {
    const actual = server.address().port;
    // 临时台账实例（测试/演练）不写 port.txt / server.pid：
    // 否则 8793 这类测试实例会把主实例 8792 的指针覆盖掉，项目空间工具随后 fetch failed。
    const ephemeral = Boolean(process.env.DS_TEAMS_LEDGER);
    // 漂移出来的实例同样不写：否则脚本会读到备用端口，主实例反倒失联。
    if (actual === requested && !ephemeral) fs.writeFileSync(path.join(DATA, "port.txt"), String(actual));
    else if (ephemeral) console.log("[ds-teams] 临时台账实例不写 port.txt/server.pid");
    else console.log(`[ds-teams] 端口漂移 ${requested} → ${actual}，port.txt 保持指向主实例`);
    // 只有绑到请求端口的主实例才写 pid：漂移实例写 pid 会让重启脚本杀错进程。
    if (actual === requested && !ephemeral) fs.writeFileSync(path.join(DATA, "server.pid"), String(process.pid));
    console.log(`[ds-teams] http://${host}:${actual}/?k=${KEY}`);
    watchFrontend();
    watchBackend();
    // 没有超管密码时，只在启动日志里提示去主机上的 data/setup-code.txt 取初始注册码。
    const sup = personOf(superId());
    if (!sup || !sup.pw) {
      console.log(`[ds-teams] 超级群主还没注册：打开页面，用初始注册码 ${SETUP_CODE} 注册（同 data/setup-code.txt）。注册后这个码就不再管用了。`);
    }
  });
}
acquireLock();
listen(PORT);

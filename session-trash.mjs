// session-trash.mjs —— 房间专属会话目录的回收站搬运（两个桥 + 清理脚本共用）。
//
// 背景（用户 2026-11-10）：桥给每个 open 房自动建 DSH 会话，房归档后会话目录留在
// %USERPROFILE%\.dsh\sessions\<workspace>\ 里没人清，累积 578 个 / 40.5MB，拖慢 DSH 启动扫描。
//
// 规则（项目纪律）：不直接删除，一律移进 Desktop\trash\<YYYY-MM-DD>\room-sessions\；
// 同一天同名再移一次自动加 -2、-3 后缀，绝不覆盖已有回收物。
//
// 本模块只做「搬目录」，不碰台账；台账由调用方（桥/清理脚本）决定怎么改。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

// DSH 的会话根（与两个桥里的 SESS_ROOT 同源）。测试可覆盖：trashSession(id, { sessionsRoot })
export const SESSIONS_ROOT =
  process.env.DS_TEAMS_SESSIONS_DIR || path.join(os.homedir(), ".dsh", "sessions");

const SID_RE = /^session-[A-Za-z0-9._-]+$/;

export function sessionDir(sessionId, sessionsRoot = SESSIONS_ROOT) {
  return path.join(sessionsRoot, String(sessionId || ""));
}

// 本地日期（不是 UTC）：回收站按天分目录，跨零点要落在「人看的那一天」。
export function todayStamp(d = new Date()) {
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

export function trashDirFor(dateStamp = todayStamp(), home = os.homedir()) {
  return path.join(home, "Desktop", "trash", dateStamp, "room-sessions");
}

function dirBytes(p) {
  let total = 0;
  let st;
  try { st = fs.statSync(p); } catch { return 0; }
  if (!st.isDirectory()) return st.size;
  let names = [];
  try { names = fs.readdirSync(p); } catch { return 0; }
  for (const n of names) total += dirBytes(path.join(p, n));
  return total;
}

/**
 * 把一个会话目录移进回收站。
 * @returns {{ok:boolean, reason?:string, dest?:string, bytes?:number, dryRun?:boolean}}
 *   reason: bad-id | missing | not-a-dir | rename-failed
 */
export function trashSession(sessionId, opts = {}) {
  const sid = String(sessionId || "").trim();
  if (!SID_RE.test(sid)) return { ok: false, reason: "bad-id" };
  const sessionsRoot = opts.sessionsRoot || SESSIONS_ROOT;
  const home = opts.home || os.homedir();
  const src = sessionDir(sid, sessionsRoot);
  let st;
  try { st = fs.statSync(src); } catch { return { ok: false, reason: "missing" }; }
  if (!st.isDirectory()) return { ok: false, reason: "not-a-dir" };
  const root = opts.trashRoot || trashDirFor(opts.dateStamp || todayStamp(), home);
  let dest = path.join(root, sid);
  for (let i = 2; fs.existsSync(dest); i += 1) dest = path.join(root, `${sid}-${i}`);
  const bytes = dirBytes(src);
  if (opts.dryRun) return { ok: true, dest, bytes, dryRun: true };
  try {
    fs.mkdirSync(root, { recursive: true });
    fs.renameSync(src, dest);   // 同盘 C:，rename 原子；跨盘会抛，落回 rename-failed
  } catch (e) {
    return { ok: false, reason: "rename-failed: " + String((e && e.message) ?? e).slice(0, 120) };
  }
  return { ok: true, dest, bytes };
}
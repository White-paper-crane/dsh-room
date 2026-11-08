// ds-teams · /api/health 快照（零依赖，只读传入数据，不碰全局状态）
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { CAP, CLOSED as CLOSED_LEDGER_STATES, SUBSTANTIVE_KINDS } from "../protocol.mjs";
import { tally } from "../capacity-lib.mjs";

// 归档会话来源：默认 ~/.dsh/storages/workspace.json，可用环境变量覆盖（测试/换机时用）
export const DEFAULT_WORKSPACE_FILE = process.env.DS_TEAMS_WORKSPACE_FILE
  || path.join(os.homedir(), ".dsh", "storages", "workspace.json");

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}

// 桥接游标文件：{seq, at}；只取 seq，文件缺失、JSON 损坏、seq 非法都当 0
export function readBridgeCursor(dataDir, name) {
  const j = readJson(path.join(dataDir, `bridge-${name}-cursor.json`));
  const raw = Number(j && j.seq);
  return { cursor: Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0 };
}

// 最新 seq 减游标；游标比最新还大（换了数据源）时按 0 算，不出现负 lag
function bridgeHealth(latestSeq, dataDir) {
  const latest = Number.isFinite(Number(latestSeq)) ? Number(latestSeq) : 0;
  const one = (name) => {
    const c = readBridgeCursor(dataDir, name).cursor;
    return { cursor: c, lag: Math.max(0, latest - c) };
  };
  return { me: one("me"), daman: one("daman") };
}

// 读 global.archivedSessionIds；文件不存在/结构不对 → 空数组
export function readArchivedSessions(file) {
  const j = readJson(file);
  const ids = j && j.global && j.global.archivedSessionIds;
  return Array.isArray(ids) ? ids.filter((x) => typeof x === "string") : [];
}

// 任务消息去重后，confirm done / report done / 台账收尾任一即算完成
function roomTaskProgress(msgs, ledger, roomId, closedStates) {
  const ids = [];
  for (const m of msgs) {
    if (m.kind === "task" && m.taskId && !ids.includes(m.taskId)) ids.push(String(m.taskId));
  }
  const open = [];
  let done = 0;
  for (const id of ids) {
    const entry = ledger && ledger[`${roomId}::${id}`];
    const ledgerClosed = closedStates.has(entry && entry.state);
    const roomDone = msgs.some((m) => m.taskId === id && !m.recalled
      && ((m.kind === "confirm" && m.text === "done") || (m.kind === "report" && String(m.status) === "done")));
    if (ledgerClosed || roomDone) done++;
    else open.push(id);
  }
  return { total: ids.length, done, open };
}

function roomHealth(r, msgs, { ledger, closedStates, now }) {
  const last = msgs.length ? msgs[msgs.length - 1] : null;
  // idle 的锚点：最后一条实质消息（system/patrol 不算，否则巡检自己会把静默计时清零）；一条都没有就退回建群时间
  const real = msgs.filter((m) => SUBSTANTIVE_KINDS.has(m.kind));
  const anchorMsg = real.length ? real[real.length - 1] : null;
  const anchor = anchorMsg ? Date.parse(anchorMsg.ts) : Date.parse(r.createdAt || 0);
  const idleSeconds = Math.max(0, Math.round((now - (Number.isFinite(anchor) ? anchor : now)) / 1000));
  // 话筒租约过期即视为空闲，和发言抢占逻辑一致
  const floorLive = r.floor && r.floor.holder
    && (!r.floor.expiresAt || Date.parse(r.floor.expiresAt) > now);
  return {
    id: r.id,
    title: r.title,
    status: r.status,
    idleSeconds,
    floor: floorLive ? { holder: r.floor.holder, expiresAt: r.floor.expiresAt || null } : null,
    lastSeq: last ? last.seq : 0,
    lastWho: last ? last.who : null,
    lastKind: last ? last.kind : null,
    lastAt: last ? last.ts : null,
    // 额外字段：healthcheck 判断「静默且有未完成任务」要用
    tasks: roomTaskProgress(msgs, ledger, r.id, closedStates),
  };
}

// 组装 /api/health 的完整响应；db 传 server 内存里的房间库，保证读到未落盘的最新状态
export function buildHealth({
  db = { seq: 0, rooms: [], messages: [] },
  dataDir,
  workspaceFile = DEFAULT_WORKSPACE_FILE,
  ledger = {},
  closedStates = CLOSED_LEDGER_STATES,
  now = Date.now(),
} = {}) {
  const byRoom = new Map();
  for (const m of db.messages || []) {
    if (!byRoom.has(m.roomId)) byRoom.set(m.roomId, []);
    byRoom.get(m.roomId).push(m);
  }
  return {
    ok: true,
    at: new Date(now).toISOString(),
    rooms: (db.rooms || []).map((r) => roomHealth(r, byRoom.get(r.id) || [], { ledger, closedStates, now })),
    bridges: bridgeHealth(db.seq, dataDir),
    // 并发名额：cap 是上限（DS_TEAMS_CAP_* 可覆盖），used 是当前占用，房间/项目分桶。
    capacity: { cap: CAP, used: tally(ledger) },
    archived: { sessions: readArchivedSessions(workspaceFile) },
  };
}

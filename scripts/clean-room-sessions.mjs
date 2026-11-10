// clean-room-sessions.mjs —— 清理「房专属会话」占用的磁盘。
//
// 背景（用户 2026-11-10）：rooms.json 只剩 2 个 open 房，但 room-sessions.json 台账里
// 攒了 355 个房 / 578 个会话（桥给每个开过的房自动建的），会话目录合计约 40MB，
// 全在 %USERPROFILE%\.dsh\sessions\<workspace>\ 下，拖慢 DSH 启动扫描也让人误以为
// 「上下文里有几百个房」。
//
// 规则：
//   - 保留 = rooms.json 里 status=open 的房（两端会话都留）；
//   - 其余台账条目（已关闭/已归档/房已消失）→ 会话目录移进 Desktop\trash\<日期>\room-sessions\，
//     台账条目删除（先备份 room-sessions.json.bak-<ts>）；
//   - 默认 --dry-run，只有加 --apply 才真的动文件。
//
// 用法：
//   node scripts/clean-room-sessions.mjs            # 预演，只打印
//   node scripts/clean-room-sessions.mjs --apply    # 真清
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { trashSession, SESSIONS_ROOT, trashDirFor, todayStamp } from "../session-trash.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const TEAMS = path.resolve(HERE, "..");
const DATA = path.join(TEAMS, "data");
const ROOMS_FILE = path.join(DATA, "rooms.json");
const LEDGER_FILE = path.join(DATA, "room-sessions.json");

const readJson = (file, fallback) => {
  try { return JSON.parse(fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "")); } catch { return fallback; }
};

/**
 * 纯函数：给定 rooms.json 与台账，算出「保留谁、清谁」。
 * @returns {{openIds:string[], keep:Map<string, Array<{who:string,sid:string}>>,
 *            purge:Array<{rid:string, rows:Array<{who:string,sid:string}>}>}}
 */
export function planCleanup(rooms, ledger) {
  const list = Array.isArray(rooms) ? rooms : (rooms && Array.isArray(rooms.rooms) ? rooms.rooms : []);
  const openIds = list.filter((r) => r && r.status === "open").map((r) => String(r.id));
  const openSet = new Set(openIds);
  const ledgerRooms = (ledger && ledger.rooms) || {};
  const keep = new Map();
  const purge = [];
  for (const [rid0, info] of Object.entries(ledgerRooms)) {
    const rid = String(rid0);
    const ends = (info && info.ends) || {};
    const rows = Object.entries(ends)
      .map(([who, e]) => ({ who, sid: String((e && e.sessionId) || ""), status: String((e && e.status) || "active") }))
      .filter((r) => r.sid);
    if (openSet.has(rid)) keep.set(rid, rows);
    else purge.push({ rid, rows });
  }
  return { openIds, keep, purge };
}

function main() {
  const apply = process.argv.includes("--apply");
  const rooms = readJson(ROOMS_FILE, null);
  const ledger = readJson(LEDGER_FILE, null);
  if (!rooms) { console.log("✗ 读不到 data/rooms.json，先确认在 teams 目录跑"); process.exit(1); }
  if (!ledger) { console.log("✗ 读不到 data/room-sessions.json（台账），先确认在 teams 目录跑"); process.exit(1); }

  const { openIds, keep, purge } = planCleanup(rooms, ledger);
  const keepSids = new Set([...keep.values()].flat().map((r) => r.sid));

  let moved = 0, missing = 0, bytes = 0, skipped = 0;
  const movedList = [];
  for (const room of purge) {
    for (const row of room.rows) {
      if (keepSids.has(row.sid)) { skipped += 1; continue; }   // 防御：同一会话被两个房引用
      const r = trashSession(row.sid, { dryRun: !apply });
      if (r.ok) { moved += 1; bytes += r.bytes || 0; movedList.push({ sid: row.sid, room: room.rid, bytes: r.bytes || 0 }); }
      else if (r.reason === "missing") missing += 1;
      else { skipped += 1; console.log(`  ! ${row.sid} (${room.rid}) 没搬动: ${r.reason}`); }
    }
  }

  const mb = (bytes / 1048576).toFixed(2);
  if (apply) {
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const bak = `${LEDGER_FILE}.bak-${stamp}`;
    try { fs.copyFileSync(LEDGER_FILE, bak); } catch {}
    const next = JSON.parse(JSON.stringify(ledger));
    next.rooms = {};
    for (const [rid, rows] of keep) {
      next.rooms[rid] = ledger.rooms[rid];
      void rows;
    }
    const tmp = LEDGER_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(next, null, 2), "utf8");
    fs.renameSync(tmp, LEDGER_FILE);
    console.log(`✓ 清掉 ${purge.length} 个房 / 搬走 ${moved} 个会话目录（${mb}MB），台账只留 ${keep.size} 个 open 房`);
    console.log(`  回收站：${trashDirFor(todayStamp())}`);
    console.log(`  台账备份：${bak}`);
  } else {
    console.log(`[预演] 将清 ${purge.length} 个房 / 搬走 ${moved} 个会话目录（${mb}MB），台账保留 ${keep.size} 个 open 房`);
    console.log("  加 --apply 才真的执行。");
  }
  if (missing) console.log(`  另有 ${missing} 个台账会话目录本就不存在（跳过）`);
  if (skipped) console.log(`  跳过 ${skipped} 个（被多个房引用或搬迁失败）`);
  if (movedList.length) {
    const top = movedList.sort((a, b) => b.bytes - a.bytes).slice(0, 5);
    console.log("  最大的几个：" + top.map((x) => `${x.sid.slice(0, 16)}… ${(x.bytes / 1024).toFixed(0)}KB`).join(", "));
  }
  if (openIds.length) console.log(`  保留的 open 房：${openIds.join(", ")}`);
}

// 只有直接执行才跑 main（被测试 import 时不要动文件）
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
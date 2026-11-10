// session-trash + clean-room-sessions 单测（2026-11-10）。
// 纯函数/临时目录级：不碰真实 .dsh\sessions 与真实回收站。
// 覆盖：planCleanup 只保留 open 房；trashSession 搬目录 + 同名不覆盖 + 非法 id/缺失目录的返回码。
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { trashSession, trashDirFor } from "../session-trash.mjs";
import { planCleanup } from "../scripts/clean-room-sessions.mjs";

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; } else { fail++; console.error("FAIL: " + name); } };

// ---- planCleanup：台账 4 个房，rooms.json 只有 2 个 open ----
const rooms = [
  { id: "room-open1", status: "open" },
  { id: "room-open2", status: "open" },
  { id: "room-closed", status: "closed" },
];
const ledger = {
  version: 1,
  enabled: true,
  rooms: {
    "room-open1": { ends: { me: { sessionId: "session-a1", status: "active" }, daman: { sessionId: "session-a2", status: "active" } } },
    "room-open2": { ends: { me: { sessionId: "session-b1", status: "active" } } },
    "room-closed": { ends: { me: { sessionId: "session-c1", status: "archived" } } },
    "room-gone": { ends: { daman: { sessionId: "session-d1", status: "archived" } } },
    "room-empty": { ends: {} },
  },
};
const plan = planCleanup(rooms, ledger);
ok(plan.openIds.length === 2 && plan.openIds.includes("room-open1"), "openIds = 2 个 open 房");
ok(plan.keep.size === 2, "keep 只留 2 个 open 房");
ok(plan.keep.get("room-open1").length === 2, "open 房两端会话都留");
ok(plan.purge.length === 3, "purge = closed + gone + empty 共 3 个房");
ok(plan.purge.find((p) => p.rid === "room-gone").rows[0].sid === "session-d1", "purge 带出 sessionId");
ok(planCleanup([], ledger).purge.length === 5, "rooms 为空时全清");
ok(planCleanup(null, null).purge.length === 0, "空输入不炸");

// ---- trashSession：真搬目录（临时根）----
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "rs-trash-"));
const sessRoot = path.join(tmp, "sessions");
const trashRoot = path.join(tmp, "trash", "2026-11-10", "room-sessions");
const mkSession = (sid, bytes = 10) => {
  fs.mkdirSync(path.join(sessRoot, sid, "sub"), { recursive: true });
  fs.writeFileSync(path.join(sessRoot, sid, "session.v4.jsonl.zstd"), Buffer.alloc(bytes, 7));
  fs.writeFileSync(path.join(sessRoot, sid, "sub", "x.txt"), "hi");
};
process.on("exit", () => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {} });

mkSession("session-keep1", 100);
const r1 = trashSession("session-keep1", { sessionsRoot: sessRoot, trashRoot });
ok(r1.ok === true, "trashSession ok");
ok(fs.existsSync(path.join(trashRoot, "session-keep1", "session.v4.jsonl.zstd")), "目录已搬进回收站");
ok(!fs.existsSync(path.join(sessRoot, "session-keep1")), "原目录已不在");
ok(r1.bytes >= 100, "bytes 统计包含文件");

// 同名再搬一次 → -2 后缀，不覆盖
mkSession("session-keep1", 5);
const r2 = trashSession("session-keep1", { sessionsRoot: sessRoot, trashRoot });
ok(r2.ok === true && /-2$/.test(r2.dest), "同名第二次落到 -2，不覆盖");

// dryRun 不动物理文件
mkSession("session-dry", 3);
const r3 = trashSession("session-dry", { sessionsRoot: sessRoot, trashRoot, dryRun: true });
ok(r3.ok === true && r3.dryRun === true, "dryRun ok");
ok(fs.existsSync(path.join(sessRoot, "session-dry")), "dryRun 不搬");

ok(trashSession("../evil", { sessionsRoot: sessRoot, trashRoot }).reason === "bad-id", "路径穿越 id 被拒");
ok(trashSession("session-nope", { sessionsRoot: sessRoot, trashRoot }).reason === "missing", "目录不存在 -> missing");

// trashDirFor 按天分目录
ok(trashDirFor("2026-01-02", "C:\\Users\\x").endsWith(path.join("Desktop", "trash", "2026-01-02", "room-sessions")), "按天分目录");

console.log(`room-session-clean: ${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
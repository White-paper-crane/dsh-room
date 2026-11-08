#!/usr/bin/env node
// ds-teams · dispatch 台账 CLI（大满、小满的回合里都可用）
//   node dispatch.mjs claim <roomId> <taskId> [--seq N] [--assignee daman]
//   node dispatch.mjs set   <roomId> <taskId> <state> [--sub s1,s2] [--mate m1,m2] [--stall-min N]
//   node dispatch.mjs touch <roomId> <taskId> [--stall-min N] [--note "..."]
//   node dispatch.mjs get   <roomId> <taskId>
//   node dispatch.mjs list  [--all]
//   node dispatch.mjs resume            （进程启动/插件重载后：非终态 -> interrupted）
//   node dispatch.mjs promote           （名额空出后：queued 按 sourceSeq 提升为 claimed）
//   node dispatch.mjs watchdog [--claimed-min 5] [--run-min 45] [--queued-min 120]
//   node dispatch.mjs fail <roomId> <taskId> --reason "..."   （spawn 失败：未到上限回 queued，到了标 failed）
//   node dispatch.mjs review <roomId> <taskId> --verdict pass|fail [--by daman] [--note "..."]
//   node dispatch.mjs room-status <roomId>
//   node dispatch.mjs counts
// 退出码：0 成功；2 用法错；3 重复 claim（已存在，调用方应无视）。
import { claim, readLedger, setState, touch, markInterrupted, taskKey, promote, watchdog, fail, counts, addReview, roomStatus } from "./dispatch-lib.mjs";
import { CAP, TERMINAL } from "./protocol.mjs";

const argv = process.argv.slice(2);
const cmd = argv[0];
const flag = (name, d = "") => {
  const i = argv.indexOf("--" + name);
  return i >= 0 && argv[i + 1] !== undefined ? argv[i + 1] : d;
};
const pos = argv.slice(1).filter((a, i, arr) => !a.startsWith("--") && !(i > 0 && arr[i - 1].startsWith("--")));

const die = (msg) => { console.error(msg); process.exit(2); };

if (cmd === "claim") {
  const [roomId, taskId] = pos;
  if (!roomId || !taskId) die("usage: claim <roomId> <taskId> [--seq N] [--assignee daman]");
  const seq = flag("seq"); const r = claim(roomId, taskId, seq === "" ? null : Number(seq), { assignee: flag("assignee", "daman") });
  console.log(JSON.stringify(r));
  process.exit(r.duplicate ? 3 : 0);
} else if (cmd === "set") {
  const [roomId, taskId, state] = pos;
  if (!roomId || !taskId || !state) die("usage: set <roomId> <taskId> <state>");
  const patch = {};
  if (flag("sub")) patch.subtasks = flag("sub").split(",").map((s) => s.trim()).filter(Boolean);
  if (flag("mate")) patch.teammates = flag("mate").split(",").map((s) => s.trim()).filter(Boolean);
  if (flag("stall-min")) patch.stallMs = Number(flag("stall-min")) * 60e3;
  const r = setState(taskKey(roomId, taskId), state, patch);
  console.log(JSON.stringify(r));
  // A10：状态非法/超额时把原因打到 stderr——以前只给退出码 2，调用方看不出为什么。
  if (!r.ok) console.error(String(r.error || "set failed"));
  process.exit(r.ok ? 0 : 2);
} else if (cmd === "touch") {
  const [roomId, taskId] = pos;
  if (!roomId || !taskId) die("usage: touch <roomId> <taskId> [--stall-min N] [--note \"...\"]");
  const stall = flag("stall-min");
  const r = touch(taskKey(roomId, taskId), {
    by: flag("by", "daman"),
    note: flag("note"),
    stallMs: stall === "" ? undefined : Number(stall) * 60e3,
  });
  console.log(JSON.stringify(r));
  if (!r.ok) console.error(String(r.error || "touch failed"));
  process.exit(r.ok ? 0 : 2);
} else if (cmd === "get") {
  const [roomId, taskId] = pos;
  if (!roomId || !taskId) die("usage: get <roomId> <taskId>");
  const e = readLedger().tasks[taskKey(roomId, taskId)];
  if (!e) { console.log(JSON.stringify({ ok: false, error: "not found" })); process.exit(3); }
  console.log(JSON.stringify({ ok: true, entry: e }));
} else if (cmd === "list") {
  const all = argv.includes("--all");
  const rows = Object.values(readLedger().tasks).filter((e) => all || !TERMINAL.has(e.state));
  console.log(JSON.stringify({ ok: true, tasks: rows }, null, 2));
} else if (cmd === "resume") {
  const n = markInterrupted();
  console.log(JSON.stringify({ ok: true, interrupted: n }));
} else if (cmd === "promote") {
  const promoted = promote();
  console.log(JSON.stringify({ ok: true, promoted, counts: counts() }));
} else if (cmd === "watchdog") {
  const ms = (name) => { const v = flag(name); return v === "" ? undefined : Number(v) * 60e3; };
  const out = watchdog({ claimedMs: ms("claimed-min"), runMs: ms("run-min"), queuedMs: ms("queued-min") });
  console.log(JSON.stringify({ ok: true, timedOut: out }));
} else if (cmd === "fail") {
  const [roomId, taskId] = pos;
  if (!roomId || !taskId) die("usage: fail <roomId> <taskId> --reason \"...\"");
  const r = fail(taskKey(roomId, taskId), flag("reason"));
  console.log(JSON.stringify(r));
  process.exit(r.ok ? 0 : 2);
} else if (cmd === "review") {
  const [roomId, taskId] = pos;
  if (!roomId || !taskId) die("usage: review <roomId> <taskId> --verdict pass|fail [--by daman] [--note \"...\"]");
  const r = addReview(taskKey(roomId, taskId), { verdict: flag("verdict"), by: flag("by", "daman"), note: flag("note") });
  console.log(JSON.stringify(r));
  process.exit(r.ok ? 0 : 2);
} else if (cmd === "room-status") {
  const [roomId] = pos;
  if (!roomId) die("usage: room-status <roomId>");
  console.log(JSON.stringify({ ok: true, ...roomStatus(roomId) }, null, 2));
} else if (cmd === "counts") {
  console.log(JSON.stringify({ ok: true, cap: CAP, ...counts() }));
} else {
  die("usage: claim|set|touch|get|list|resume|promote|watchdog|fail|review|room-status|counts");
}

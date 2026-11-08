// T2（2026-11-07）：任务走完/失败之后当场提升排队任务，不等 15 秒维护窗口。
// 另测 claimedAwaitingSpawn：已 claimed 但没人 spawn 的，交给桥去提醒，且 5 分钟内不重复。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-promote-"));
process.env.DS_TEAMS_LEDGER = path.join(dir, "ledger.json");
const {
  claim, reportTask, recordGate, acceptTask, fail, writeLedger, readLedger, taskKey, claimedAwaitingSpawn,
} = await import("../dispatch-lib.mjs");

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (!cond) { fails++; console.log("FAIL " + name + (extra ? " :: " + JSON.stringify(extra) : "")); }
  else console.log("PASS " + name);
};

// 1) 先把一个房塞满：两张 claimed，第三张排队。
const a = claim("r1", "T-A", 1), b = claim("r1", "T-B", 2), c = claim("r1", "T-C", 3);
ok("前两张 claimed", a.entry.state === "claimed" && b.entry.state === "claimed");
ok("第三张排队", c.entry.state === "queued" && c.queued === true, c.entry.state);

// 2) A 走完 → accept 当场把 C 提上来，不等定时器。
const kA = taskKey("r1", "T-A");
recordGate(kA, { pass: true, exitCode: 0, by: "t" });
reportTask(kA, { by: "me", summary: "做完了" });
const acc = acceptTask(kA, { by: "you", note: "验收通过" });
ok("accept 顺手提升", Array.isArray(acc.promoted) && acc.promoted.includes(taskKey("r1", "T-C")), acc.promoted);
ok("C 立刻变 claimed", readLedger().tasks[taskKey("r1", "T-C")].state === "claimed");

// 3) 失败到顶（不再回队列）也让出名额。
const kD = taskKey("r1", "T-D");
const d = claim("r1", "T-D", 4);
ok("D 排队", d.entry.state === "queued", d.entry.state);
const db = readLedger();
db.tasks[taskKey("r1", "T-B")].maxAttempts = 1;
writeLedger(db);
const f = fail(taskKey("r1", "T-B"), "spawn 起不来");
ok("失败不再回队列", f.retry === false && f.state === "failed", f);
ok("失败顺手提升 D", Array.isArray(f.promoted) && f.promoted.includes(kD), f.promoted);
ok("D 立刻变 claimed", readLedger().tasks[kD].state === "claimed");

// 4) claimed 未 spawn 的提醒清单：首次命中，5 分钟内不重复，过期后再给。
const first = claimedAwaitingSpawn();
ok("有 claimed 未 spawn 的任务", first.length > 0 && first.includes(taskKey("r1", "T-C")), first);
const second = claimedAwaitingSpawn();
ok("同一批 5 分钟内不重复提醒", second.length === 0, second);
const db2 = readLedger();
for (const k of first) db2.tasks[k].nudgedAt = new Date(Date.now() - 10 * 60e3).toISOString();
writeLedger(db2);
const third = claimedAwaitingSpawn();
ok("过期后重新提醒", third.length === first.length, third);

// 5) 已经 spawn 过（有 subtasks）的不再提醒。
const db3 = readLedger();
db3.tasks[taskKey("r1", "T-C")].subtasks = [{ id: "T-C.s1" }];
for (const k of third) db3.tasks[k].nudgedAt = new Date(0).toISOString();
writeLedger(db3);
ok("已 spawn 的不提醒", !claimedAwaitingSpawn().includes(taskKey("r1", "T-C")));

fs.rmSync(dir, { recursive: true, force: true });
console.log(`\ndispatch-promote: ${fails === 0 ? "all pass" : fails + " fail"}`);
process.exit(fails ? 1 : 0);

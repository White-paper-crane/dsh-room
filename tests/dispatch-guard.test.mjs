// A5 + A10 + A2 的台账侧（2026-11-08）。
// 守的是：长任务有心跳就不会被误杀；真干完的活即使已判超时也记进账；
// set 不再能绕过状态机和并发名额；提醒丢了能立刻重挑、接管给对端的卡不会叫错人。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const libUrl = pathToFileURL(path.join(here, "..", "dispatch-lib.mjs")).href;

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-guard-"));
process.env.DS_TEAMS_LEDGER = path.join(dir, "ledger.json");

const { claim, setState, watchdog, reportTask, touch, clearNudge, claimedAwaitingSpawn, readLedger, writeLedger, taskKey } =
  await import(libUrl + "?guard=" + Date.now());

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { console.log("PASS " + name); return; }
  fails++;
  console.log("FAIL " + name + (extra !== "" ? " :: " + JSON.stringify(extra) : ""));
};

const K = (room, id) => taskKey(room, id);
const hit = (wd, k) => wd.some((x) => x.key === k);
const MIN = 60e3;
const ago = (ms) => new Date(Date.now() - ms).toISOString();
const patch = (key, p) => {
  const db = readLedger();
  if (!db.tasks[key]) throw new Error("test: unknown key " + key);
  Object.assign(db.tasks[key], p);
  writeLedger(db);
};

// ① 有心跳的长任务不该被看门狗误杀（以前只看 updatedAt，claimed 满 15 分钟一律 timeout）
claim("r1", "live", 1);
patch(K("r1", "live"), { claimedAt: ago(20 * MIN), heartbeatAt: new Date().toISOString() });
let wd = watchdog({ claimedMs: 15 * MIN });
ok("有心跳的 claimed 不判超时", !hit(wd, K("r1", "live")), wd);
ok("没有被误杀，状态还是 claimed", readLedger().tasks[K("r1", "live")].state === "claimed");

// ② 没心跳、认领 20 分钟的照旧判超时
claim("r1", "dead", 2);
patch(K("r1", "dead"), { claimedAt: ago(20 * MIN), updatedAt: ago(20 * MIN), heartbeatAt: undefined });
wd = watchdog({ claimedMs: 15 * MIN });
ok("没心跳的老 claimed 判超时", hit(wd, K("r1", "dead")), wd);
ok("状态变 timeout", readLedger().tasks[K("r1", "dead")].state === "timeout");

// ③ 任务可以自报「多久没动静算卡死」
claim("r2", "stalled", 3);
touch(K("r2", "stalled"), { stallMs: 60e3 });
patch(K("r2", "stalled"), { heartbeatAt: ago(2 * MIN) });
wd = watchdog({ claimedMs: 15 * MIN });
ok("自报 1 分钟，2 分钟没心跳就判超时", hit(wd, K("r2", "stalled")), wd);

// ④ 超时后才交上来的报告要收（以前只在 claimed/running/reported 收，干完的活等于白干）
const art = path.join(dir, "report.md");
fs.writeFileSync(art, "报告\n");
const late = reportTask(K("r1", "dead"), { summary: "干完了但已经判超时", artifacts: [art] });
ok("超时后的报告被收下", late.ok === true && late.late === true, late);
const dead = readLedger().tasks[K("r1", "dead")];
ok("终态没被迟到报告改回去", dead.state === "timeout");
ok("报告内容留在台账里", dead.lateSummary === "干完了但已经判超时" && (dead.artifacts || []).some((a) => String(a.path || "").endsWith("report.md")),
  { lateSummary: dead.lateSummary, artifacts: dead.artifacts });

// ⑤ 心跳打不进终态任务（否则错账看起来像还活着）
const t = touch(K("r1", "dead"), { note: "还在干" });
ok("终态任务不收心跳", t.ok === false, t);

// ⑥ set 不能跳状态：queued 直接 done 要拒，并说清允许哪些
claim("r3", "skip", 4);
patch(K("r3", "skip"), { state: "queued" });
const bad = setState(K("r3", "skip"), "done");
ok("queued 直接 done 被拒", bad.ok === false, bad);
ok("错误里写清非法迁移", /illegal transition queued -> done/.test(bad.error || ""), bad.error);
const bad2 = setState(K("r3", "skip"), "archived");
ok("不认识的状态被拒", bad2.ok === false && /unknown state/.test(bad2.error || ""), bad2.error);

// ⑦ set 不能绕过名额
claim("r4", "a", 5);
claim("r5", "b", 6);
const actives = [K("r1", "live"), K("r4", "a"), K("r5", "b")];
ok("三张活跃就位", actives.every((k) => readLedger().tasks[k].state === "claimed"),
  actives.map((k) => readLedger().tasks[k].state));
claim("r6", "c", 7);
ok("第四张只能排队", readLedger().tasks[K("r6", "c")].state === "queued");
const over = setState(K("r6", "c"), "claimed");
ok("set 不能把名额撑到第 4 个", over.ok === false, over);
ok("错误是 cap full 并给出用量", /^cap full:/.test(over.error || ""), over.error);

// ⑧ A2：提醒丢了要能立刻重挑，而不是干等 5 分钟 TTL
clearNudge(K("r1", "live"));
ok("claimed 没提醒过的会被挑出来", claimedAwaitingSpawn("daman").includes(K("r1", "live")));
ok("挑过之后 5 分钟内不重复挑", !claimedAwaitingSpawn("daman").includes(K("r1", "live")));
clearNudge(K("r1", "live"));
ok("还账后能立刻重挑", claimedAwaitingSpawn("daman").includes(K("r1", "live")));

// ⑨ A12：接管给对端的卡（daman → me），只提醒接手端
patch(K("r4", "a"), { handoff: { from: "daman", to: "me", at: new Date().toISOString() } });
clearNudge(K("r4", "a"));
ok("接管走的卡不提醒失联端", !claimedAwaitingSpawn("daman").includes(K("r4", "a")));
ok("接手端自己要提醒", claimedAwaitingSpawn("me").includes(K("r4", "a")));

fs.rmSync(dir, { recursive: true, force: true });
console.log(fails ? `${fails} FAILED` : "ALL PASS");
process.exit(fails ? 1 : 0);

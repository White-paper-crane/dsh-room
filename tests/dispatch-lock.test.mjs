// A3（2026-11-08）：台账锁。
// 以前拿不到锁 3 秒就抛错，10 秒「破锁」还会把别人正拿着的锁删掉。
// 这里守三件用户能感觉到的事：并发写不丢单、活进程的锁不会被抢、死进程留下的锁能自愈。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const libUrl = pathToFileURL(path.join(here, "..", "dispatch-lib.mjs")).href;

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-lock-"));
const ledger = path.join(dir, "ledger.json");
process.env.DS_TEAMS_LEDGER = ledger;
process.env.DS_TEAMS_LOCK_TIMEOUT_MS = "400";
process.env.DS_TEAMS_LOCK_STALE_MS = "400";
const LOCK = ledger + ".lock";

const { claim, setState, readLedger, taskKey } = await import(libUrl + "?lock=" + Date.now());
const K = (room, id) => taskKey(room, id);

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { console.log("PASS " + name); return; }
  fails++;
  console.log("FAIL " + name + (extra !== "" ? " :: " + JSON.stringify(extra) : ""));
};

// ① 正常写：锁用完必须消失，否则下一条命令永远等
claim("room-a", "t1", 1);
ok("claim 后锁文件已释放", !fs.existsSync(LOCK));

// ② 活进程持锁：报忙、说清是谁，绝不拆
fs.writeFileSync(LOCK, JSON.stringify({ pid: process.pid, token: "someone-else", at: new Date().toISOString() }));
let err = null;
try { setState(K("room-a", "t1"), "running"); } catch (e) { err = e; }
ok("活进程持锁时抛错而不是硬拆", !!err);
ok("错误码是 LEDGER_BUSY", err && err.code === "LEDGER_BUSY", err && err.message);
ok("错误里带持锁 pid", err ? String(err.message).includes(String(process.pid)) : false, err && err.message);
fs.rmSync(LOCK, { force: true });

// ③ 锁文件「看着很旧」但持有进程还活着：时间戳不可信，pid 才可信
fs.writeFileSync(LOCK, JSON.stringify({ pid: process.pid, token: "old-but-alive", at: new Date(Date.now() - 60e3).toISOString() }));
err = null;
try { setState(K("room-a", "t1"), "running"); } catch (e) { err = e; }
ok("活进程的老锁不拆", err && err.code === "LEDGER_BUSY", err && err.message);
fs.rmSync(LOCK, { force: true });

// ④ 死进程留下的锁：自愈，不能把整个台账卡死
fs.writeFileSync(LOCK, JSON.stringify({ pid: 999999, token: "dead", at: new Date(Date.now() - 60e3).toISOString() }));
const r4 = setState(K("room-a", "t1"), "running");
ok("死进程的锁被破后能继续写", r4.ok === true, r4);
ok("破锁后不残留锁文件", !fs.existsSync(LOCK));

// ⑤ 真并发：两个进程各写 10 条，一条都不能丢
const child = path.join(dir, "child.mjs");
fs.writeFileSync(child, `
process.env.DS_TEAMS_LEDGER = ${JSON.stringify(ledger)};
const { claim } = await import(${JSON.stringify(pathToFileURL(path.join(here, "..", "dispatch-lib.mjs")).href)} + "?child=" + Date.now());
const who = process.argv[2];
for (let i = 0; i < 10; i++) claim("room-" + who, "t" + i, i + 1);
`);
for (const who of ["p1", "p2"]) {
  const r = spawnSync(process.execPath, [child, who], { stdio: "inherit", timeout: 60000 });
  ok("并发子进程 " + who + " 正常退出", r.status === 0, r.status);
}
const keys = Object.keys(readLedger().tasks).filter((k) => k.startsWith("room-p"));
ok("并发 20 次 claim 一条不丢", keys.length === 20, keys.length);

fs.rmSync(dir, { recursive: true, force: true });
console.log(fails ? `${fails} FAILED` : "ALL PASS");
process.exit(fails ? 1 : 0);

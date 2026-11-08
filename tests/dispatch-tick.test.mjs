// T1（2026-11-07）：排队推进不再只靠大满端桥。
// 起一个临时实例（临时台账 + 临时数据目录 + 临时端口 + 800ms tick），
// 只把第一张标 done，看第三张能不能在没有任何桥接参与的情况下自己变 claimed。
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const NODE = process.execPath;
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-tick-"));
const ledger = path.join(dir, "ledger.json");
const dataDir = path.join(dir, "data");
const port = 18800 + Math.floor(Math.random() * 150);

process.env.DS_TEAMS_LEDGER = ledger;
const { claim, setState, readLedger, taskKey } = await import("../dispatch-lib.mjs");

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (!cond) { fails++; console.log("FAIL " + name + (extra ? " :: " + JSON.stringify(extra) : "")); }
  else console.log("PASS " + name);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 铺状态：同一个房两张 claimed，第三张只能排队（每房上限 2）。
claim("r1", "T-A", 1);
claim("r1", "T-B", 2);
const c = claim("r1", "T-C", 3);
ok("起手第三张排队", c.entry.state === "queued", c.entry.state);

const child = spawn(NODE, ["server.mjs"], {
  cwd: ROOT,
  env: {
    ...process.env,
    DS_TEAMS_LEDGER: ledger,
    DS_TEAMS_DATA_DIR: dataDir,
    DS_TEAMS_PORT: String(port),
    DS_TEAMS_DISPATCH_TICK_MS: "800",
    DS_TEAMS_PATROL_MS: "600000",
  },
  stdio: ["ignore", "pipe", "pipe"],
});
let log = "";
child.stdout.on("data", (b) => { log += b.toString(); });
child.stderr.on("data", (b) => { log += b.toString(); });

let up = false;
for (let i = 0; i < 60; i++) {
  await sleep(250);
  try {
    const r = await fetch(`http://127.0.0.1:${port}/`, { signal: AbortSignal.timeout(800) });
    if (r.status > 0) { up = true; break; }
  } catch {}
}
ok("临时实例起来了", up, log.slice(-400));

if (up) {
  // 释放一个名额：第一张直接置 done（不经过 promise 的 accept 收口，纯看服务端定时器）。
  setState(taskKey("r1", "T-A"), "done");
  let state = "";
  for (let i = 0; i < 20; i++) {
    await sleep(250);
    state = readLedger().tasks[taskKey("r1", "T-C")].state;
    if (state === "claimed") break;
  }
  ok("没有桥接也自动提升（≤5 秒内）", state === "claimed", state);
  ok("服务端日志有 promote 记录", /\[dispatch\] promote/.test(log), log.slice(-300));
}

try { child.kill(); } catch {}
await sleep(300);
try { fs.rmSync(dir, { recursive: true, force: true }); } catch {}
console.log(`\ndispatch-tick: ${fails === 0 ? "all pass" : fails + " fail"}`);
process.exit(fails ? 1 : 0);

// A11：忙的时候消息最多等多久（2026-11-08）。
// 守的是「长任务期间同房消息无限 hold」：超过上限就得放下并回一句，不能一声不吭地攒着。
process.env.DS_TEAMS_HOLD_MAX_MS = "60000";
const path = await import("node:path");
const { fileURLToPath, pathToFileURL } = await import("node:url");

const here = path.dirname(fileURLToPath(import.meta.url));
const libUrl = pathToFileURL(path.join(here, "..", "hold-lib.mjs")).href;
const { createHoldClock, HOLD_MAX_MS } = await import(libUrl + "?h=" + Date.now());

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { console.log("PASS " + name); return; }
  fails++;
  console.log("FAIL " + name + (extra !== "" ? " :: " + JSON.stringify(extra) : ""));
};

ok("上限读环境变量", HOLD_MAX_MS === 60000, HOLD_MAX_MS);

const clock = createHoldClock();
const T = 1_000_000;
clock.mark("busy:7", T);
ok("刚记下时没超时", clock.expired("busy:7", T + 1000) === false);
ok("到点算超时", clock.expired("busy:7", T + 60000) === true);
ok("过了点还算超时", clock.expired("busy:7", T + 120000) === true);

// 反复 mark 不能把等待时间刷新掉——否则一直有新消息进来就永远等不到上限
clock.mark("busy:7", T + 50000);
ok("重复 mark 不重置起点", clock.expired("busy:7", T + 61000) === true, clock.at("busy:7"));
ok("记的是第一次 hold 的时刻", clock.at("busy:7") === T);

// 没记过的 key 不算超时（不能凭空把人赶走）
ok("没记过的 key 不超时", clock.expired("busy:404", T + 999999) === false);

// 投进去了就作废，重新等从头算
clock.clear("busy:7");
ok("clear 之后不再算超时", clock.expired("busy:7", T + 999999) === false);
clock.mark("busy:7", T + 200000);
ok("重新等到满上限才超时",
  clock.expired("busy:7", T + 250000) === false && clock.expired("busy:7", T + 260000) === true);

// prune 只留还在等的那几条
clock.mark("busy:8", T);
clock.mark("busy:9", T);
clock.prune(["busy:7", "busy:8"]);
ok("已经不用等的键被清掉", clock.size === 2 && clock.at("busy:9") === 0, clock.size);
ok("还在等的键保留原起点", clock.at("busy:7") === T + 200000);

console.log(fails ? `${fails} FAILED` : "ALL PASS");
process.exit(fails ? 1 : 0);

// A12：掉线接管判据（2026-11-08）。
// 守的是「同一张卡不会两端各跑一遍」：只有真失联、真没进度、且不在交接冷却里的任务才改派。
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const libUrl = pathToFileURL(path.join(here, "..", "takeover-lib.mjs")).href;
const { decideTakeovers, applyTakeover, lastProgressMs } = await import(libUrl + "?t=" + Date.now());

let fails = 0;
const ok = (name, cond, extra = "") => {
  if (cond) { console.log("PASS " + name); return; }
  fails++;
  console.log("FAIL " + name + (extra !== "" ? " :: " + JSON.stringify(extra) : ""));
};

const MIN = 60e3;
const NOW = Date.parse("2026-11-08T12:00:00.000Z");
const iso = (ms) => new Date(ms).toISOString();

// 一张卡：daman 20 分钟前认领，最后一次动静在 10 分钟前
const card = (over = {}) => ({
  roomId: "r", taskId: "t", title: "修 A12", state: "running", assignee: "daman",
  claimedAt: iso(NOW - 20 * MIN), updatedAt: iso(NOW - 10 * MIN), ...over,
});
const down = (me, daman) => ({ me, daman });

ok("两端都在线不动", decideTakeovers({ now: NOW, down: down(false, false), tasks: { k: card() } }).length === 0);
ok("两端同时失联不动", decideTakeovers({ now: NOW, down: down(true, true), tasks: { k: card() } }).length === 0);

let moves = decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card() } });
ok("daman 失联、卡归 daman、10 分钟没动静 → 转给 me",
  moves.length === 1 && moves[0].from === "daman" && moves[0].to === "me", moves);
ok("转派带 key 和标题", moves[0] && moves[0].key === "k" && moves[0].title === "修 A12", moves);

ok("刚认领 5 分钟的不抢（可能正在动手）",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ claimedAt: iso(NOW - 5 * MIN) }) } }).length === 0);
ok("1 分钟前还有心跳的不抢（活着，只是慢）",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ heartbeatAt: iso(NOW - 1 * MIN) }) } }).length === 0);
ok("卡本来就在 me 名下不动",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ assignee: "me" }) } }).length === 0);
ok("已完成的不动",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ state: "done" }) } }).length === 0);
ok("排队中的不抢（还没开始干）",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ state: "queued" }) } }).length === 0);
ok("10 分钟前刚交接过的，冷却期内不再转",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ handoff: { from: "daman", to: "me", at: iso(NOW - 10 * MIN) } }) } }).length === 0);
ok("交接超过 30 分钟、又没动静的可以再转",
  decideTakeovers({ now: NOW, down: down(false, true), tasks: { k: card({ handoff: { from: "daman", to: "me", at: iso(NOW - 40 * MIN) } }) } }).length === 1);

// 一端掉线的镜像：me 掉线，卡归 me → 转给 daman
moves = decideTakeovers({ now: NOW, down: down(true, false), tasks: { k: card({ assignee: "me" }) } });
ok("me 失联时反着转", moves.length === 1 && moves[0].from === "me" && moves[0].to === "daman", moves);

// lastProgressMs 取三个时间里最新的
ok("lastProgressMs 取最新心跳",
  lastProgressMs({ claimedAt: iso(NOW - 30 * MIN), updatedAt: iso(NOW - 20 * MIN), heartbeatAt: iso(NOW - 2 * MIN) }) === NOW - 2 * MIN);

// applyTakeover 真的落账
const e = card();
applyTakeover(e, { from: "daman", to: "me", title: "修 A12", now: NOW });
ok("接手人写进去了", e.assignee === "me");
ok("交接记录写进去了", e.handoff && e.handoff.from === "daman" && e.handoff.to === "me" && Date.parse(e.handoff.at) === NOW, e.handoff);
ok("留了 takeover 事件", (e.events || []).some((x) => x.type === "takeover"), e.events);

console.log(fails ? `${fails} FAILED` : "ALL PASS");
process.exit(fails ? 1 : 0);

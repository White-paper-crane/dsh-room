// T6：并发名额分桶（房间 / 项目各算各的，全局统管两者之和）。
// 纯函数部分直接喂内存台账；最后一组验证 dispatch-lib 真的接上了这套算法。
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { bucketOf, tally, capFor, admit, DEFAULT_CAPS } from "../capacity-lib.mjs";

const room = (id, roomId = "r1", state = "running") => ({ key: id, roomId, state });
const proj = (id, projectId = "p1", state = "running") => ({ key: id, roomId: projectId, projectId, state });
const db = (...entries) => ({ version: 1, tasks: Object.fromEntries(entries.map((e) => [e.key, e])) });

test("分桶：带 projectId 的算项目桶，只有 roomId 的算房间桶", () => {
  assert.deepEqual(bucketOf(proj("a")), { kind: "project", key: "p1" });
  assert.deepEqual(bucketOf(room("b")), { kind: "room", key: "r1" });
  // 项目 id 与房间 id 撞名也不能串桶
  assert.equal(bucketOf(proj("a", "same")).kind, "project");
  assert.equal(bucketOf(room("b", "same")).kind, "room");
  // 脏数据不抛
  assert.deepEqual(bucketOf(null), { kind: "room", key: "" });
  assert.deepEqual(bucketOf({ roomId: " r2 " }), { kind: "room", key: "r2" });
});

test("计数：只算 ACTIVE，终态与排队不占名额", () => {
  const t = tally(db(
    room("a", "r1", "running"),
    room("b", "r1", "claimed"),
    room("c", "r1", "done"),
    room("d", "r1", "queued"),
    proj("e", "p1", "spawned"),
    proj("f", "p1", "failed"),
  ));
  assert.equal(t.global, 3);
  assert.deepEqual(t.rooms, { r1: 2 });
  assert.deepEqual(t.projects, { p1: 1 });
});

test("分桶：项目桶满了房间还能进，反之亦然", () => {
  const caps = { global: 99, room: 2, project: 2 };
  const full = db(proj("a"), proj("b"));
  assert.equal(admit(proj("c"), full, caps).ok, false);
  assert.equal(admit(proj("c"), full, caps).reason, "bucket full");
  // 同一份台账里，房间任务不受项目桶影响
  assert.equal(admit(room("z"), full, caps).ok, true);

  const fullRoom = db(room("a"), room("b"));
  assert.equal(admit(room("c"), fullRoom, caps).ok, false);
  assert.equal(admit(proj("z", "p9"), fullRoom, caps).ok, true);
});

test("全局名额仍然统管两者之和", () => {
  const caps = { global: 3, room: 2, project: 2 };
  const mixed = db(room("a"), room("b"), proj("c"));
  const r = admit(proj("d"), mixed, caps);
  assert.equal(r.ok, false);
  assert.equal(r.reason, "global full");
  assert.equal(r.global, 3);
  assert.equal(r.globalCap, 3);
  // 各自桶都没满，只是全局满了——明细要能解释这一点
  assert.equal(r.used, 1);
  assert.equal(r.cap, 2);
});

test("上限可配：换一套 caps 立刻生效", () => {
  const one = db(room("a"), room("b"));
  assert.equal(admit(room("c"), one, DEFAULT_CAPS).ok, false);       // 默认每房 2
  assert.equal(admit(room("c"), one, { ...DEFAULT_CAPS, room: 3 }).ok, true);
  assert.equal(admit(room("c"), one, { global: 2, room: 9, project: 9 }).ok, false); // 全局卡住
  // capFor 读的是同一套规则
  assert.equal(capFor(proj("a"), { global: 5, room: 7, project: 11 }), 11);
  assert.equal(capFor(room("a"), { global: 5, room: 7, project: 11 }), 7);
});

test("dispatch-lib 真的接了分桶：项目卡不挤占房间名额", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "cap-"));
  const ledger = path.join(dir, "dispatch-state.json");
  process.env.DS_TEAMS_LEDGER = ledger;
  fs.writeFileSync(ledger, JSON.stringify(db(
    proj("a", "p1", "running"),
    proj("b", "p1", "running"),   // 项目桶已满（默认 2）
    room("c", "r1", "running"),
  )), "utf8");
  const lib = await import("../dispatch-lib.mjs");

  const c = lib.counts();
  assert.equal(c.global, 3);
  assert.deepEqual(c.projects, { p1: 2 });
  assert.deepEqual(c.rooms, { r1: 1 });

  // 项目 p1 再开一张 → 排队（桶满），但不会因为房间在上限上而误判
  const created = lib.createTask({ projectId: "p1", taskId: "t-new", title: "新任务" });
  assert.equal(created.ok, true);
  const started = lib.startTask(created.key);
  assert.equal(started.queued, true, "项目桶满应当排队");
  assert.equal(started.entry.state, "queued");

  // 房间 r1 现在只有 1 张，还能再进一张（房间桶 2）
  const c2 = lib.counts();
  assert.equal(c2.rooms.r1, 1);
  assert.equal(c2.global, 3);

  fs.rmSync(dir, { recursive: true, force: true });
});

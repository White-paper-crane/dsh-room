// v3 服务端改造回归：ack 游标 / 桥接心跳与 down 判定 / 掉线接管 / 房间记忆 / 建群 system 卡
// 运行：node --test tests/server-v3.test.mjs（跑在真实服务 8792 上；写全局状态与伪造失联的用例
// 全部跑在临时数据目录 + 临时台账 + 快 patrol 的隔离实例上，不动真实数据）
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (process.env.DS_TEAMS_URL || "http://127.0.0.1:8792").replace(/\/+$/, "");
const KEY = (process.env.DS_TEAMS_KEY || fs.readFileSync(path.join(ROOT, "data", "key.txt"), "utf8")).trim();
const PEOPLE = JSON.parse(fs.readFileSync(path.join(ROOT, "data", "people.json"), "utf8"));
const PERSONAS = JSON.parse(fs.readFileSync(path.join(ROOT, "data", "personas.json"), "utf8"));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeApi(base, key, people) {
  return async function api(method, p, body) {
    if (body && body.who && !body.key) body = { ...body, key: (people[body.who] || {}).key };
    // 房间内容现在按身份过滤（不是成员就 403）：GET 读房间/归档时默认用小满（agent）身份
    if (method === "GET" && !/[?&](?:who|mk)=/.test(p) && /^\/api\/(?:rooms|archive)\//.test(p)) {
      const k = encodeURIComponent((people.me || {}).key || "");
      p += (p.includes("?") ? "&" : "?") + "who=me&key=" + k;
    }
    const sep = p.includes("?") ? "&" : "?";
    const res = await fetch(`${base}${p}${sep}k=${encodeURIComponent(key)}`, {
      method,
      headers: body ? { "content-type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    let data = {};
    try { data = await res.json(); } catch {}
    return { status: res.status, data };
  };
}
const api = makeApi(BASE, KEY, PEOPLE);

// 测试房收尾：解散 + 归档 + 清掉该房所有落盘残留（房间归档文件 + 两个位置的记忆文件）
async function tidyRoom(a, id) {
  await a("POST", `/api/rooms/${id}/close`, { who: "you" });
  await a("POST", `/api/rooms/${id}/archive`, { who: "you" });
  for (const fp of [
    path.join(ROOT, "data", "archive", `${id}.json`),
    path.join(ROOT, "data", "memory", `${id}.jsonl`),
    path.join(ROOT, "data", "archive", "memory", `${id}.jsonl`),
  ]) { try { if (fs.existsSync(fp)) fs.unlinkSync(fp); } catch {} }
}

async function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, "127.0.0.1", () => { const p = s.address().port; s.close(() => resolve(p)); });
    s.on("error", reject);
  });
}
async function startEphemeral(patrolMs = "400") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-teams-v3-"));
  const dataDir = path.join(dir, "data");
  const ledger = path.join(dir, "ledger.json");
  const port = await freePort();
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: ROOT,
    env: {
      ...process.env,
      DS_TEAMS_PORT: String(port),
      DS_TEAMS_LEDGER: ledger,
      DS_TEAMS_DATA_DIR: dataDir,
      DS_TEAMS_PATROL_MS: patrolMs,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  child.stdout.on("data", (d) => { log += d; });
  child.stderr.on("data", (d) => { log += d; });
  const base = `http://127.0.0.1:${port}`;
  const keyFile = path.join(dataDir, "key.txt");
  const t0 = Date.now();
  while (Date.now() - t0 < 20000) {
    if (child.exitCode !== null) throw new Error(`隔离实例提前退出(${child.exitCode}): ${log.slice(-800)}`);
    if (fs.existsSync(keyFile)) {
      try {
        const key = fs.readFileSync(keyFile, "utf8").trim();
        const res = await fetch(`${base}/api/health?k=${encodeURIComponent(key)}`, { signal: AbortSignal.timeout(2000) });
        if (res.status === 200) {
          const people = JSON.parse(fs.readFileSync(path.join(dataDir, "people.json"), "utf8"));
          return {
            base, key, people, ledger, dir, child,
            kill() { try { child.kill(); } catch {} try { fs.rmSync(dir, { recursive: true, force: true }); } catch {} },
          };
        }
      } catch {}
    }
    await sleep(200);
  }
  try { child.kill(); } catch {}
  throw new Error(`隔离实例启动超时: ${log.slice(-800)}`);
}

/* ================= 真实服务（8792）用例 ================= */

test("state：GET 只读不推进游标，who 参数回显 cursor；ack 非法入参全拒（不写状态）", async () => {
  const s1 = await api("GET", "/api/state?since=0&who=me");
  assert.equal(s1.status, 200);
  assert.equal(typeof s1.data.cursor, "number", "who 参数应回显 cursor");
  const s2 = await api("GET", "/api/state?since=0&who=me");
  assert.equal(s2.data.cursor, s1.data.cursor, "GET /api/state 不能推进游标");
  assert.ok(Number.isInteger(s2.data.seq) && s2.data.seq >= 0);

  // 以下全是校验失败路径（先校验后写，不落任何全局状态）
  assert.equal((await api("POST", "/api/state/ack", { who: "you" })).status, 400, "you 不是桥接端");
  assert.equal((await api("POST", "/api/state/ack", { who: "me" })).status, 400, "缺 seq");
  assert.equal((await api("POST", "/api/state/ack", { who: "me", seq: -1 })).status, 400, "负 seq");
  assert.equal((await api("POST", "/api/state/ack", { who: "me", seq: 1.5 })).status, 400, "小数 seq");
  assert.equal((await api("POST", "/api/state/ack", { who: "me", seq: "abc" })).status, 400, "非数字 seq");
  assert.equal((await api("POST", "/api/state/ack", { who: "me", seq: 0, key: "deadbeef" })).status, 403, "错成员 key");
});

test("bridge status：heartbeat/down/downAfterMs/takeoverAfterMs 形状（只读）", async () => {
  const s = await api("GET", "/api/bridge/status");
  assert.equal(s.status, 200);
  for (const who of ["me", "daman"]) {
    assert.ok(who in s.data.heartbeat, `heartbeat 含 ${who}`);
    assert.ok(who in s.data.down, `down 含 ${who}`);
    assert.equal(typeof s.data.down[who], "boolean");
  }
  assert.equal(s.data.downAfterMs, 180000);
  assert.equal(s.data.takeoverAfterMs, 900000);
});

test("房间记忆：读写/倒序/400/2000 边界/100 条上限/归档还原搬移", async () => {
  const created = await api("POST", "/api/rooms", { who: "me", title: `v3服务端-记忆-${Date.now()}`, declaredBy: "me" });
  assert.equal(created.status, 200);
  const id = created.data.room.id;
  try {
    const w1 = await api("POST", `/api/rooms/${id}/memory`, { who: "me", text: "第一条：把收货单发我" });
    assert.equal(w1.status, 200);
    assert.equal(w1.data.count, 1);
    assert.equal(w1.data.roomId, id);
    assert.equal(w1.data.entries[0].by, "me");

    await api("POST", `/api/rooms/${id}/memory`, { who: "daman", text: "第二条：已收到" });
    const g2 = await api("GET", `/api/rooms/${id}/memory`);
    assert.equal(g2.status, 200);
    assert.equal(g2.data.count, 2);
    assert.equal(g2.data.entries[0].text, "第二条：已收到", "倒序：最新在前");
    assert.equal(g2.data.entries[1].text, "第一条：把收货单发我");

    // 400：空文本 / 超长 / 未知 who；2000 字恰好通过
    assert.equal((await api("POST", `/api/rooms/${id}/memory`, { who: "me", text: "   " })).status, 400);
    assert.equal((await api("POST", `/api/rooms/${id}/memory`, { who: "me", text: "x".repeat(2001) })).status, 400);
    assert.equal((await api("POST", `/api/rooms/${id}/memory`, { who: "ghost" })).status, 400, "未知成员 400");
    assert.equal((await api("POST", `/api/rooms/${id}/memory`, { who: "me", text: "x".repeat(2000) })).status, 200, "恰好 2000 字放行");

    // 错成员 key 403
    assert.equal((await api("POST", `/api/rooms/${id}/memory`, { who: "me", key: "deadbeef", text: "x" })).status, 403);

    // 上限：灌到 103 条 → 保留最近 100，最旧 3 条被丢
    for (let i = 0; i < 100; i++) {
      const r = await api("POST", `/api/rooms/${id}/memory`, { who: "me", text: `bulk-${String(i).padStart(3, "0")}` });
      assert.equal(r.status, 200);
    }
    const g3 = await api("GET", `/api/rooms/${id}/memory`);
    assert.equal(g3.data.count, 100, "每房上限 100 条");
    assert.equal(g3.data.entries[0].text, "bulk-099");
    assert.equal(g3.data.entries[99].text, "bulk-000", "丢最旧 3 条（灌 100 条后保留窗口恰为 bulk-000..bulk-099）");
    assert.ok(!g3.data.entries.some((e) => e.text === "第一条：把收货单发我"));
    assert.ok(!g3.data.entries.some((e) => e.text === "第二条：已收到"));

    // 归档 → 记忆文件随房移入 data/archive/memory/；还原 → 移回 data/memory/
    assert.equal((await api("POST", `/api/rooms/${id}/close`, { who: "you" })).status, 200);
    const arc = await api("POST", `/api/rooms/${id}/archive`, { who: "you" });
    assert.equal(arc.status, 200);
    assert.ok(!fs.existsSync(path.join(ROOT, "data", "memory", `${id}.jsonl`)), "归档后原位置无记忆文件");
    assert.ok(fs.existsSync(path.join(ROOT, "data", "archive", "memory", `${id}.jsonl`)), "记忆文件随房归档");
    const res = await api("POST", `/api/archive/${id}/restore`, { who: "you" });
    assert.equal(res.status, 200);
    assert.ok(fs.existsSync(path.join(ROOT, "data", "memory", `${id}.jsonl`)), "还原后记忆文件移回");
  } finally {
    await tidyRoom(api, id);
  }
});

test("建群卡：POST /api/rooms 自动推 system 卡（成员/三句规则/记忆摘要/人设名）", async () => {
  const title = `v3服务端-建群卡-${Date.now()}`;
  const created = await api("POST", "/api/rooms", { who: "me", title, declaredBy: "me" });
  assert.equal(created.status, 200);
  const id = created.data.room.id;
  try {
    const msgs = await api("GET", `/api/rooms/${id}/messages?since=0`);
    const card = (msgs.data.messages || []).find((m) => m.kind === "system");
    assert.ok(card, "建群必须自动推一张 system 卡");
    assert.ok(card.text.includes("建群"), "卡含建群行");
    assert.ok(card.text.includes("成员"), "卡含成员行");
    assert.ok(card.text.includes("规则"), "卡含规则行");
    assert.ok(card.text.includes("记忆"), "卡含记忆摘要行");
    assert.ok(card.text.includes("暂无历史记忆"), "新房记忆摘要为暂无历史记忆");
    assert.ok(card.text.includes(PERSONAS.me.name), "卡含 me 人设名");
    assert.ok(card.text.includes(PERSONAS.daman.name), "卡含 daman 人设名");
    assert.ok(card.text.includes("机器话只进日志"), "规则含机器话只进日志");
    assert.ok(card.text.includes("交付带证据"), "规则含交付带证据");
  } finally {
    await tidyRoom(api, id);
  }
});

/* ================= 隔离实例用例（临时数据目录，不动真实数据） ================= */

test("隔离实例：ack 推进游标 + 幂等 + 倒退 400 + state 不自动推进", async () => {
  const inst = await startEphemeral();
  const eapi = makeApi(inst.base, inst.key, inst.people);
  try {
    const s0 = await eapi("GET", "/api/state?since=0&who=me");
    assert.equal(s0.data.cursor, 0, "全新实例游标从 0 起");

    const a1 = await eapi("POST", "/api/state/ack", { who: "me", seq: 7 });
    assert.equal(a1.status, 200);
    assert.equal(a1.data.cursor, 7);
    assert.equal(a1.data.advanced, true);

    // 有新消息也不自动推进
    const room = await eapi("POST", "/api/rooms", { who: "me", title: "v3服务端-ack隔离", declaredBy: "me" });
    await eapi("POST", `/api/rooms/${room.data.room.id}/say`, { who: "me", text: "新消息来了" });
    const s1 = await eapi("GET", "/api/state?since=0&who=me");
    const s2 = await eapi("GET", "/api/state?since=0&who=me");
    assert.ok(s2.data.seq > 0, "确有新消息");
    assert.equal(s1.data.cursor, 7, "GET 不推进游标");
    assert.equal(s2.data.cursor, 7, "再读一次仍不推进");

    // 幂等：同 seq 重复 ack 放行且不报错
    const a2 = await eapi("POST", "/api/state/ack", { who: "me", seq: 7 });
    assert.equal(a2.status, 200);
    assert.equal(a2.data.cursor, 7);
    assert.equal(a2.data.advanced, false);

    // 倒退 400
    const a3 = await eapi("POST", "/api/state/ack", { who: "me", seq: 3 });
    assert.equal(a3.status, 400);
    assert.ok(String(a3.data.error).includes("backward"), a3.data.error);

    // 两端游标独立
    assert.equal((await eapi("POST", "/api/state/ack", { who: "daman", seq: 11 })).data.cursor, 11);
    assert.equal((await eapi("GET", "/api/state?since=0&who=me")).data.cursor, 7, "daman 的 ack 不影响 me");
  } finally {
    inst.kill();
  }
});

test("隔离实例：C4 每房游标低水位（一个房 hold 不拖死另一个房）", async () => {
  const inst = await startEphemeral();
  const eapi = makeApi(inst.base, inst.key, inst.people);
  try {
    const ra = await eapi("POST", "/api/rooms", { who: "me", title: "v3服务端-C4低水位A", declaredBy: "me" });
    const rb = await eapi("POST", "/api/rooms", { who: "me", title: "v3服务端-C4低水位B", declaredBy: "me" });
    const A = ra.data.room.id;
    const B = rb.data.room.id;

    // 两个房各自推进：A 停在 5，B 追到 9
    assert.equal((await eapi("POST", "/api/state/ack", { who: "me", seq: 5, roomId: A })).status, 200);
    const b9 = await eapi("POST", "/api/state/ack", { who: "me", seq: 9, roomId: B });
    assert.equal(b9.status, 200);
    assert.equal(b9.data.floor, 5, "B 推进后低水位仍取 A 的 5");

    const s = await eapi("GET", "/api/state?since=0&who=me");
    assert.equal(s.data.cursor, 5, "对外 cursor 取所有 open 房的低水位");
    assert.equal(s.data.roomCursors.me[A], 5);
    assert.equal(s.data.roomCursors.me[B], 9);

    // 只前进：同房倒退 400，各房互不影响
    const back = await eapi("POST", "/api/state/ack", { who: "me", seq: 3, roomId: A });
    assert.equal(back.status, 400);
    assert.ok(String(back.data.error).includes("backward"), back.data.error);
    assert.equal((await eapi("POST", "/api/state/ack", { who: "me", seq: 6, roomId: A })).data.floor, 6);

    // 不存在的房 404，不会写坏游标
    assert.equal((await eapi("POST", "/api/state/ack", { who: "me", seq: 7, roomId: "room-doesnotexist" })).status, 404);

    // 老的全局 ack（不带 roomId）仍然可用，且不影响按房取到的低水位
    assert.equal((await eapi("POST", "/api/state/ack", { who: "me", seq: 20 })).status, 200);
    assert.equal((await eapi("GET", "/api/state?since=0&who=me")).data.cursor, 6, "按房低水位不被全局 ack 顶高");
  } finally {
    inst.kill();
  }
});

test("隔离实例：心跳写入 / status / down 判定（伪造旧时间戳，从未见过不算 down）", async () => {
  const inst = await startEphemeral();
  const eapi = makeApi(inst.base, inst.key, inst.people);
  try {
    const s0 = await eapi("GET", "/api/bridge/status");
    assert.equal(s0.status, 200);
    assert.equal(s0.data.heartbeat.me, null);
    assert.equal(s0.data.down.me, false, "从没见过心跳不算 down");
    assert.equal(s0.data.down.daman, false);

    const h1 = await eapi("POST", "/api/bridge/heartbeat", { who: "me" });
    assert.equal(h1.status, 200);
    assert.equal(h1.data.who, "me");
    const s1 = await eapi("GET", "/api/bridge/status");
    assert.equal(s1.data.heartbeat.me, h1.data.at, "心跳已写入");
    assert.equal(s1.data.down.me, false, "新鲜心跳不算 down");

    const old = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const h2 = await eapi("POST", "/api/bridge/heartbeat", { who: "me", at: old });
    assert.equal(h2.data.at, new Date(old).toISOString());
    const s2 = await eapi("GET", "/api/bridge/status");
    assert.equal(s2.data.down.me, true, "距上次心跳 >180s 算 down");

    assert.equal((await eapi("POST", "/api/bridge/heartbeat", { who: "me", at: "not-a-date" })).status, 400, "坏时间戳 400");
    assert.equal((await eapi("POST", "/api/bridge/heartbeat", { who: "me", key: "deadbeef" })).status, 403, "错成员 key 403");
    assert.equal((await eapi("POST", "/api/bridge/heartbeat", { who: "you" })).status, 400, "you 不是桥接端");
  } finally {
    inst.kill();
  }
});

test("隔离实例：掉线接管（临时台账 + 测试房，15 分钟条件、双端 down 不动作）", async () => {
  const inst = await startEphemeral("400");
  const eapi = makeApi(inst.base, inst.key, inst.people);
  try {
    const room = await eapi("POST", "/api/rooms", { who: "me", title: "v3服务端-takeover隔离", declaredBy: "me" });
    assert.equal(room.status, 200);
    const rid = room.data.room.id;
    // A12（2026-11-08）：接管的三个条件——失联、认领满 15 分钟、且至少 5 分钟没有任何动静。
    // 这里的 updatedAt 跟着 claimedAt 走：真掉线的那端写不了账，卡自然不会显新。
    const mk = (taskId, title, claimedAgoMin) => ({
      roomId: rid, taskId, title, assignee: "me", state: "claimed", attempts: 1,
      claimedAt: new Date(Date.now() - claimedAgoMin * 60 * 1000).toISOString(),
      updatedAt: new Date(Date.now() - claimedAgoMin * 60 * 1000).toISOString(), events: [],
    });
    const tasks = { [`${rid}::t1`]: mk("t1", "掉线交接任务一", 20), [`${rid}::t2`]: mk("t2", "刚接的任务二", 1) };
    fs.writeFileSync(inst.ledger, JSON.stringify({ version: 1, tasks }, null, 2));

    // me 掉线（伪造旧心跳），daman 存活（新鲜心跳）
    await eapi("POST", "/api/bridge/heartbeat", { who: "me", at: new Date(Date.now() - 10 * 60 * 1000).toISOString() });
    await eapi("POST", "/api/bridge/heartbeat", { who: "daman" });
    const st = await eapi("GET", "/api/bridge/status");
    assert.equal(st.data.down.me, true);
    assert.equal(st.data.down.daman, false);

    // 等 patrol：t1（claimedAt 20 分钟前）转给 daman；t2（1 分钟前）不动
    const deadline = Date.now() + 15000;
    let card = null;
    while (Date.now() < deadline) {
      const msgs = await eapi("GET", `/api/rooms/${rid}/messages?since=0`);
      card = (msgs.data.messages || []).find((m) => m.kind === "system" && m.text.includes("掉线"));
      if (card) break;
      await sleep(200);
    }
    assert.ok(card, "房间应收到掉线转接 system 卡");
    assert.ok(card.text.includes("转给"), "卡文案含「转给」");
    assert.ok(card.text.includes("掉线交接任务一"), "卡文案含任务标题");
    assert.ok(card.text.includes(PERSONAS.daman.name), "卡文案含存活端人设名");

    const ledger = JSON.parse(fs.readFileSync(inst.ledger, "utf8"));
    assert.equal(ledger.tasks[`${rid}::t1`].assignee, "daman", "旧任务 assignee 转给存活端");
    const ev = ledger.tasks[`${rid}::t1`].events.find((e) => e.type === "takeover");
    assert.ok(ev && ev.by === "system" && String(ev.detail).includes("me -> daman"), "台账写 takeover 事件");
    assert.equal(ledger.tasks[`${rid}::t2`].assignee, "me", "claimedAt 不足 15 分钟不接管");

    // 双端都 down：不动作
    ledger.tasks[`${rid}::t3`] = {
      roomId: rid, taskId: "t3", title: "任务三", assignee: "daman", state: "claimed", attempts: 1,
      claimedAt: new Date(Date.now() - 20 * 60 * 1000).toISOString(),
      updatedAt: new Date().toISOString(), events: [],
    };
    fs.writeFileSync(inst.ledger, JSON.stringify(ledger, null, 2));
    await eapi("POST", "/api/bridge/heartbeat", { who: "daman", at: new Date(Date.now() - 10 * 60 * 1000).toISOString() });
    await sleep(1500);
    const ledger2 = JSON.parse(fs.readFileSync(inst.ledger, "utf8"));
    assert.equal(ledger2.tasks[`${rid}::t3`].assignee, "daman", "双端都 down 不动作");
  } finally {
    inst.kill();
  }
});

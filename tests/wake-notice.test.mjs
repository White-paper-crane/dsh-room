// T5（2026-11-07）：预算/上下文被闸门拒了，房里要留一句话，不能静默 ack。
// 测两层：
//   1) 文案与幂等键（wake-notice-lib.mjs 纯函数，桥两端共用）；
//   2) 落盘幂等：同一 msgId 连发两次只落一条（服务端 push 的 msgId 去重），换原因才多一条。
// 全部跑在隔离实例上（临时数据目录 + 临时端口），不动真实 8792。
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { noticeFor, noticeDay } from "../wake-notice-lib.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function freePort() {
  return new Promise((resolve, reject) => {
    const s = net.createServer();
    s.listen(0, "127.0.0.1", () => { const p = s.address().port; s.close(() => resolve(p)); });
    s.on("error", reject);
  });
}

async function startEphemeral() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ds-teams-notice-"));
  const dataDir = path.join(dir, "data");
  const ledger = path.join(dir, "ledger.json");
  const port = await freePort();
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: ROOT,
    env: { ...process.env, DS_TEAMS_PORT: String(port), DS_TEAMS_LEDGER: ledger, DS_TEAMS_DATA_DIR: dataDir, DS_TEAMS_PATROL_MS: "600000" },
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
            base, key, people,
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

function api(base, key, people) {
  return async (method, p, body) => {
    if (body && body.who && !body.key) body = { ...body, key: (people[body.who] || {}).key };
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

test("文案：COUNT / CTX / 其他原因，都写明是哪条没处理", () => {
  const a = noticeFor("COUNT", 42);
  assert.match(a.text, /^【自动提示】/);
  assert.match(a.text, /唤醒额度用完了/);
  assert.match(a.text, /#42/);
  assert.equal(a.msgId, `wake-blocked:COUNT:${noticeDay()}`);

  const b = noticeFor("CTX", 7);
  assert.match(b.text, /上下文太满/);
  assert.match(b.text, /#7/);

  const c = noticeFor("REJECTED", 1);
  assert.match(c.text, /REJECTED/);
  // 同一原因同一天 → 同一幂等键；换原因 → 换键
  assert.equal(noticeFor("COUNT", 99).msgId, a.msgId, "同原因同日幂等键必须一致（seq 不影响）");
  assert.notEqual(b.msgId, a.msgId, "不同原因各自一条");
  // 显式日期覆盖（测试内可复现）
  assert.equal(noticeFor("COUNT", 1, "2026-11-07").msgId, "wake-blocked:COUNT:2026-11-07");
});

test("落盘幂等：同 msgId 连发两次只落一条，换原因才多一条", async () => {
  const inst = await startEphemeral();
  const eapi = api(inst.base, inst.key, inst.people);
  try {
    const made = await eapi("POST", "/api/rooms", { title: "提示幂等", by: "me", declaredBy: "me", key: inst.people.me.key });
    assert.equal(made.status, 200, JSON.stringify(made.data));
    const roomId = made.data.room.id;

    const n1 = noticeFor("COUNT", 12);
    const p1 = await eapi("POST", `/api/rooms/${roomId}/say`, { who: "me", text: n1.text, msgId: n1.msgId });
    assert.equal(p1.status, 200, JSON.stringify(p1.data));
    const p2 = await eapi("POST", `/api/rooms/${roomId}/say`, { who: "me", text: n1.text, msgId: n1.msgId });
    assert.equal(p2.status, 200);
    assert.equal(p2.data.message.seq, p1.data.message.seq, "同一 msgId 再发不落新条（返回既有的那条）");

    const n2 = noticeFor("CTX", 13);
    const p3 = await eapi("POST", `/api/rooms/${roomId}/say`, { who: "me", text: n2.text, msgId: n2.msgId });
    assert.equal(p3.status, 200);
    assert.notEqual(p3.data.message.seq, p1.data.message.seq, "不同原因各落一条");

    const got = await eapi("GET", `/api/rooms/${roomId}?who=me&mk=${encodeURIComponent(inst.people.me.key)}`, null);
    assert.equal(got.status, 200);
    const notices = got.data.messages.filter((m) => /^【自动提示】/.test(String(m.text || "")));
    assert.equal(notices.length, 2, "房里只有两条提示，不是三条");
    assert.equal(notices.filter((m) => m.msgId === n1.msgId).length, 1, "同 msgId 只一条");
    assert.equal(notices[0].who, "me");
    assert.equal(notices[0].kind, "text", "/say 只落 text 种别（system 走服务端专用路径）");
  } finally {
    inst.kill();
  }
});

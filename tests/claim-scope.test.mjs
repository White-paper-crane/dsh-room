// T4：claim 适用范围（file / task / system 不再双投）。
import test from "node:test";
import assert from "node:assert/strict";
import { claimScope, shouldClaim } from "../claim-scope-lib.mjs";

const IDS = { who: "me", peer: "daman" };

test("自己发的不碰，对端 agent 发的不碰", () => {
  assert.equal(claimScope({ who: "me", kind: "text" }, IDS), "never");
  assert.equal(claimScope({ who: "daman", kind: "text" }, IDS), "never");
  assert.equal(claimScope({ who: "me", kind: "file" }, IDS), "never");
  assert.equal(claimScope({ who: "daman", kind: "task" }, IDS), "never");
  assert.equal(claimScope(null, IDS), "never");
});

test("真人 text / say 走歧义判定（点名明确的让给对方）", () => {
  assert.equal(claimScope({ who: "you", kind: "text" }, IDS), "ifAmbiguous");
  assert.equal(claimScope({ who: "you", kind: "say" }, IDS), "ifAmbiguous");
  assert.equal(claimScope({ who: "guest01", kind: "text" }, IDS), "ifAmbiguous");
  // 不歧义（明确点 me）→ 不抢；歧义 → 抢
  assert.equal(shouldClaim({ who: "you", kind: "text" }, { ...IDS, ambiguous: false }), false);
  assert.equal(shouldClaim({ who: "you", kind: "text" }, { ...IDS, ambiguous: true }), true);
});

test("file / task / system 一律 claim（这是 T4 修的双投）", () => {
  for (const kind of ["file", "task", "system", "image"]) {
    assert.equal(claimScope({ who: "you", kind }, IDS), "always", `${kind} 应当 always`);
    assert.equal(shouldClaim({ who: "you", kind }, { ...IDS, ambiguous: false }), true, `${kind} 不吃歧义开关`);
  }
  // 系统提示（who=system）同样要走 claim，只注入一次
  assert.equal(shouldClaim({ who: "system", kind: "text" }, { ...IDS, ambiguous: false }), true);
  assert.equal(shouldClaim({ who: "system", kind: "notice" }, { ...IDS, ambiguous: false }), true);
});

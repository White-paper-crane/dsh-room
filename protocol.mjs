// ds-teams 协议常量单一来源（零依赖）。
// 只放可执行约束；散文解释仍在 docs/协作协议-总览.md。
import { pathToFileURL } from "node:url";

export const PROTOCOL_VERSION = "1.0.0";

// 房主 / 群主身份。历史数据里固定为 you。
export const OWNER = "you";
export const ROLES = Object.freeze(["you", "me", "daman"]);

// v3 桥接两端：只有 me/daman 能走心跳与游标 ack（you 是群主，不桥接）。
export const BRIDGE_ENDS = Object.freeze(["me", "daman"]);

// 可对外的消息类型；system / patrol 只由服务端内部产生。
export const MESSAGE_KINDS = Object.freeze([
  "text",
  "progress",
  "report",
  "review",
  "task",
  "file",
  "ack",
  "confirm",
]);
export const SYSTEM_KIND = "system";
export const INTERNAL_KINDS = Object.freeze(["system", "patrol"]);
export const MESSAGE_KIND_SET = new Set(MESSAGE_KINDS);
// 实质消息：巡检的静默计时只认这些 kind，system / patrol 不算。
export const SUBSTANTIVE_KINDS = new Set(MESSAGE_KINDS);

export const TASK_STATES = Object.freeze([
  "queued",
  "claimed",
  "spawned",
  "running",
  "reported",
  "blocked",
  "done",
  "reviewed",
  "failed",
  "timeout",
  "aborted",
  "interrupted",
]);
// 活跃：占用并发名额、超时要看门狗处理的状态。
export const ACTIVE = new Set(["claimed", "spawned", "running", "reported"]);
// 终态：看门狗不再推进。
export const TERMINAL = new Set(["done", "failed", "timeout", "aborted", "interrupted"]);
// 台账收口：房间任务算「已完成」的集合（reviewed 为历史通过态）。
export const CLOSED = new Set(["done", "aborted", "reviewed", "failed"]);

export const LIMITS = Object.freeze({
  // T6（2026-11-07）：并发上限不再写死，环境变量可调（默认值与历史一致）。
  // 房间任务与项目任务分两个桶，全局额度仍管两者之和。
  maxActiveTasks: envInt("DS_TEAMS_CAP_GLOBAL", 3),
  maxActiveTasksPerRoom: envInt("DS_TEAMS_CAP_ROOM", 2),
  maxActiveTasksPerProject: envInt("DS_TEAMS_CAP_PROJECT", 2),
  burstLimit: envInt("DS_TEAMS_BURST_LIMIT", 5),
  claimWaitMs: 2000,
  claimRetention: 200,
  // 体检告警阈值，不是服务端过期时间：claim 记录只按 target 数保留，不随时间删除。
  staleWarnMs: 24 * 3600e3,
  stallClaimedMs: 15 * 60e3,
  stallRunMs: 45 * 60e3,
  stallQueuedMs: 2 * 3600e3,
  maxAttempts: 3,

  // v3 桥接：心跳失联判定窗口与接管生效窗口（毫秒）。
  downAfterMs: 180_000,
  takeoverAfterMs: 900_000,
  // 接管条件：任务 claimedAt 早于该阈值（毫秒）才移交，避免刚接的活被抢。
  takeoverClaimMinMs: 15 * 60_000,
  // A12（2026-11-08）：光看 claimedAt 会把「刚接活但在忙」的卡也转走，两端各跑一遍。
  // 转卡还要求最近 takeoverStaleMs 内没有任何进度信号，且这张卡 takeoverCooldownMs 内没被转过。
  takeoverStaleMs: 5 * 60e3,
  takeoverCooldownMs: 30 * 60_000,

  // v3 房间记忆：单条文本上限（字符）与每房保留条数。
  memoryMaxText: 2000,
  memoryMaxEntries: 100,
});

// 唤醒治理（docs/wake-governance.md §5）：数值可被同名环境变量覆盖（测试用小限额）。
// 判定路径零模型调用：这里全是规则计数，没有模型。
function envNum(name, dflt) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return dflt;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : dflt;
}

// 整数型环境变量：非数字、负数、越界一律退回默认值，不让配置把服务端搞挂。
function envInt(name, dflt, { min = 1, max = 64 } = {}) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return dflt;
  const n = Number(raw);
  if (!Number.isFinite(n)) return dflt;
  const i = Math.floor(n);
  if (i < min) return min;
  if (i > max) return max;
  return i;
}

export const WAKE = Object.freeze({
  dailyLimit: Math.floor(envNum("WAKE_DAILY_LIMIT", 200)),                    // 每端每日唤醒次数硬上限
  contextAgeThreshold: Math.floor(envNum("WAKE_CONTEXT_AGE_THRESHOLD", 500)), // 累计注入次数 >= 此值 → HIGH_CONTEXT_AGE
  softTokenPerTurn: Math.floor(envNum("WAKE_SOFT_TOKEN_PER_TURN", 40000)),    // token 软阈值估算用（历史均值/回合）
  softTokenDailyLimit: Math.floor(envNum("WAKE_SOFT_TOKEN_DAILY_LIMIT", 8000000)), // 每日估算 token 软阈值：超过只告警不阻断
  outcomeWindowMs: Math.floor(envNum("WAKE_OUTCOME_WINDOW_MS", 90000)),       // 注入后观察窗口：90s 内本端无房内输出 → NO_REPLY
});

// settle 的合法 outcome（先到先得，已存在不覆盖）。
export const WAKE_OUTCOMES = Object.freeze(["REPLIED", "NO_REPLY", "MONOLOGUE", "ERROR"]);
export const WAKE_OUTCOME_SET = new Set(WAKE_OUTCOMES);

// 会议室（docs/meeting-room.md）：report 落库即开议题，对方评审，收敛式循环。
// 评审方沉默（wake NO_REPLY）按无意见收敛；verdict 参数化，零语义猜测。
export const MEETING = Object.freeze({
  roundCap: 3,                  // 最多评审轮次，超了收敛并提示群主裁决
  silenceWindowMs: 90_000,      // 与 WAKE.outcomeWindowMs 对齐
  verdicts: Object.freeze(["pass", "issues"]), // review 卡合法 verdict
});
export const MEETING_VERDICT_SET = new Set(MEETING.verdicts);

export const CAP = Object.freeze({
  global: LIMITS.maxActiveTasks,
  room: LIMITS.maxActiveTasksPerRoom,
  project: LIMITS.maxActiveTasksPerProject,
});

export const CLAIM = Object.freeze({
  post: "/api/rooms/:id/claim",
  get: "/api/rooms/:id/claim",
  body: ["who", "key", "targetSeq"],
  token: "db.claimSeq 单调自增",
  winner: "token 最小者",
  loser: "静默，不发群消息",
  postsMessage: false,
  waitMs: LIMITS.claimWaitMs,
  retention: LIMITS.claimRetention,
  staleWarnMs: LIMITS.staleWarnMs,
  // 服务端不按时间淘汰：超过 staleWarnMs 只由 check.mjs 报警，人工收尾。
  ttlMs: 0,
  conflictStatus: 409,
});

export const AUTH = Object.freeze({
  platform: Object.freeze({ query: "k", cookie: "tk", source: "data/key.txt" }),
  member: Object.freeze({ body: "key", header: "x-member-key", source: "data/people.json" }),
  requireBoth: true,
});

function textDescription() {
  const lines = [
    `ds-teams 协议 v${PROTOCOL_VERSION}（单一来源 protocol.mjs）`,
    `roles      ${ROLES.join(" / ")}（owner=${OWNER}）`,
    `messages   ${MESSAGE_KINDS.join(" ")}（system/patrol 内部）`,
    `states     ${TASK_STATES.join(" ")}`,
    `active     ${[...ACTIVE].join(" ")}（global ${CAP.global} / room ${CAP.room} / project ${CAP.project}）`,
    `terminal   ${[...TERMINAL].join(" ")}`,
    `closed     ${[...CLOSED].join(" ")}（台账）`,
    `limits     burst=${LIMITS.burstLimit} claimWait=${LIMITS.claimWaitMs}ms claimRetention=${LIMITS.claimRetention} maxAttempts=${LIMITS.maxAttempts}`,
    `stall      claimed=${LIMITS.stallClaimedMs / 60000}min run=${LIMITS.stallRunMs / 60000}min queued=${LIMITS.stallQueuedMs / 3600000}h`,
    `claim      POST/GET ${CLAIM.post} body {${CLAIM.body.join(",")}}，${CLAIM.winner}，${CLAIM.loser}，wait ${CLAIM.waitMs}ms`,
    `claim-ttl  服务端无过期（按 target 保留 ${CLAIM.retention} 个）；staleWarn=${CLAIM.staleWarnMs / 3600000}h 仅体检告警`,
    `auth       平台 ?${AUTH.platform.query}= / ${AUTH.platform.cookie} cookie + 成员 body.${AUTH.member.body} / ${AUTH.member.header}（两者都要）`,
    `bridge     down=${LIMITS.downAfterMs / 1000}s takeover=${LIMITS.takeoverAfterMs / 1000}s claimMin=${LIMITS.takeoverClaimMinMs / 60000}min`,
    `memory     每房 ${LIMITS.memoryMaxEntries} 条 / 单条 ${LIMITS.memoryMaxText} 字（data/memory/<roomId>.jsonl）`,
    `wake       dailyLimit=${WAKE.dailyLimit}/端 contextAge=${WAKE.contextAgeThreshold} outcome=${WAKE_OUTCOMES.join("|")} window=${WAKE.outcomeWindowMs / 1000}s`,
  ];
  return lines.join("\n");
}

// describe({ json: true }) 返回结构化对象；默认返回 ≤50 行的紧凑文本。
export function describe(opts = {}) {
  if (opts && opts.json) {
    return {
      version: PROTOCOL_VERSION,
      owner: OWNER,
      roles: [...ROLES],
      messageKinds: [...MESSAGE_KINDS],
      systemKind: SYSTEM_KIND,
      internalKinds: [...INTERNAL_KINDS],
      substantiveKinds: [...SUBSTANTIVE_KINDS],
      taskStates: [...TASK_STATES],
      active: [...ACTIVE],
      terminal: [...TERMINAL],
      closed: [...CLOSED],
      limits: { ...LIMITS },
      cap: { ...CAP },
      claim: { ...CLAIM, body: [...CLAIM.body] },
      wake: { ...WAKE, outcomes: [...WAKE_OUTCOMES] },
      auth: {
        platform: { ...AUTH.platform },
        member: { ...AUTH.member },
        requireBoth: AUTH.requireBoth,
      },
    };
  }
  return textDescription();
}

function isMain() {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return import.meta.url === pathToFileURL(entry).href;
  } catch {
    return false;
  }
}

if (isMain()) {
  const arg = process.argv[2] || "";
  if (!arg || arg === "--text") {
    console.log(describe());
  } else if (arg === "--json") {
    console.log(JSON.stringify(describe({ json: true })));
  } else {
    console.error("用法：node protocol.mjs [--json]");
    process.exit(2);
  }
}

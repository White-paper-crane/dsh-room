// 并发名额分桶（体检 T6 / C6 / A8）。
//
// 问题：原来所有 ACTIVE 任务都按 roomId 记在一个桶里，项目任务（createTask 建的卡）
// 的 roomId 就是 projectId，于是「每房 2」退化成「每项目 2」，而且项目任务和房间任务
// 一起抢那 3 个全局名额——谁先来谁把位置吃满，另一边只能排队。
//
// 改法：把「哪些任务在跑」这个计算挪成一个纯函数（不读文件、不碰锁），
// 分两个桶统计（房间 / 项目），各有各的上限，全局上限仍然统管两者之和。
// 纯函数的好处：可以直接对着一个内存台账测，不用起服务、不用抢锁。

import { ACTIVE } from "./protocol.mjs";

// 默认值与 protocol.mjs 的 CAP 一致；这里再写一份是为了让本文件能被独立测试。
export const DEFAULT_CAPS = Object.freeze({ global: 3, room: 2, project: 2 });

// 一个任务属于哪个桶：控制面 createTask 建的卡带 projectId；房间任务只有 roomId。
export function bucketOf(entry) {
  const e = entry ?? {};
  const pid = e.projectId == null ? "" : String(e.projectId).trim();
  if (pid) return { kind: "project", key: pid };
  return { kind: "room", key: String(e.roomId ?? "").trim() };
}

// 统计当前占名额的任务：global 是总数，rooms / projects 是分桶计数。
export function tally(db, active = ACTIVE) {
  const rooms = {};
  const projects = {};
  let global = 0;
  const tasks = db && db.tasks ? db.tasks : {};
  for (const entry of Object.values(tasks)) {
    if (!entry || !active.has(entry.state)) continue;
    global += 1;
    const { kind, key } = bucketOf(entry);
    const box = kind === "project" ? projects : rooms;
    box[key] = (box[key] ?? 0) + 1;
  }
  return { global, rooms, projects };
}

export function capFor(entry, caps = DEFAULT_CAPS) {
  return bucketOf(entry).kind === "project" ? caps.project : caps.room;
}

// 这个任务现在能不能占名额：先看全局，再看它自己那个桶。
// 返回明细而不是布尔值——调用方要写日志、要解释为什么排队。
export function admit(entry, db, caps = DEFAULT_CAPS) {
  const counts = tally(db);
  const { kind, key } = bucketOf(entry);
  const used = (kind === "project" ? counts.projects : counts.rooms)[key] ?? 0;
  const cap = capFor(entry, caps);
  const globalOk = counts.global < caps.global;
  const bucketOk = used < cap;
  return {
    ok: globalOk && bucketOk,
    reason: globalOk ? (bucketOk ? "" : "bucket full") : "global full",
    bucket: kind,
    key,
    used,
    cap,
    global: counts.global,
    globalCap: caps.global,
  };
}

export default { DEFAULT_CAPS, bucketOf, tally, capFor, admit };

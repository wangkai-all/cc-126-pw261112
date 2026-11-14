"use strict";

const SHARD_COLORS = ["var(--series-1)", "var(--series-2)", "var(--series-3)"];
const SHARD_SOFT = ["var(--series-1-soft)", "var(--series-2-soft)", "var(--series-3-soft)"];
const SHARD_LABELS = ["A", "B", "C", "D", "E", "F"];
const UNIT_LABELS = {
  planned: "待迁移",
  copying: "复制中",
  interrupted: "已中断",
  copied: "复制完成",
  verified: "已核验",
  switched: "已切换",
  conflict: "冲突已停止",
};
const EVENT_KIND = {
  seed: "初始化", plan: "规划", copy: "复制", copied: "复制完成",
  interrupt: "中断", verify: "核验", switch: "切换", cleanup: "清理",
  conflict: "冲突", error: "错误", dtl: "演练", write: "写入",
  retire: "退役", complete: "完成", attention: "需处理",
};
const STATUS_CHIP = {
  running: ["warning", "迁移进行中"],
  completed: ["good", "迁移已完成"],
  needs_attention: ["critical", "需要处理"],
};

const state = {
  data: null,
  selectedCustomer: null,
  lastQuery: null,
  compare: null,
  busy: false,
};

/* ------------------------------------------------------------------ utils */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));
}

function money(cents) {
  return (cents / 100).toLocaleString("zh-CN", { style: "currency", currency: "CNY" });
}

function shortTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString("zh-CN", { hour12: false }) + "." +
    String(d.getMilliseconds()).padStart(3, "0");
}

function dateTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("zh-CN", { hour12: false });
}

function shardName(index) {
  return `分片 ${index} · ${SHARD_LABELS[index] || index}`;
}

function shardSwatch(index) {
  return `<span class="swatch" style="background:${SHARD_COLORS[index % 3]}"></span>`;
}

function toast(message, bad = false) {
  const el = $("#toast");
  el.textContent = message;
  el.className = "toast" + (bad ? " bad" : "");
  el.hidden = false;
  clearTimeout(toast._timer);
  toast._timer = setTimeout(() => { el.hidden = true; }, 4200);
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.error || `请求失败（${res.status}）`);
  return body;
}

function post(path, body) {
  return api(path, { method: "POST", body: JSON.stringify(body || {}) });
}

let chain = Promise.resolve();

/* Serialize every user action: an action that lands while another is running
   queues behind it instead of racing its refresh. */
function withBusy(button, fn) {
  const run = async () => {
    const wasBusy = state.busy;
    state.busy = true;
    if (button) button.classList.add("spin");
    try {
      await fn();
    } catch (err) {
      toast(err.message, true);
    } finally {
      state.busy = wasBusy;
      if (button) button.classList.remove("spin");
    }
  };
  const next = chain.then(run, run);
  chain = next.catch(() => {});
  return next;
}

/* --------------------------------------------------------------- rendering */
function renderStats(data) {
  const stats = data.stats;
  const latest = data.latest_migration ? data.latest_migration.migration : null;
  const total = stats.units_total || 0;
  const switched = stats.units_switched || 0;
  const liveVersion = data.versions.find((v) => v.status === "live");
  const staging = data.versions.find((v) => v.status === "staging");
  const cards = [
    {
      label: "客户 / 订单",
      value: `${stats.customers}<small>位客户</small>`,
      detail: `${data.stats.orders_owned.toLocaleString("zh-CN")} 笔订单（按所有者计）`,
    },
    {
      label: "在线路由版本",
      value: `v${stats.live_version}`,
      detail: liveVersion
        ? `${liveVersion.shard_count} 个分片 · ${liveVersion.algorithm}`
        : "—",
    },
    staging
      ? {
          label: "待启用版本",
          value: `v${staging.version}`,
          detail: `${staging.shard_count} 个分片 · 旧路由保持 live`,
        }
      : {
          label: "待启用版本",
          value: "—",
          detail: "没有进行中的扩分片计划",
        },
    {
      label: "迁移单元",
      value: total ? `${switched}<small>/ ${total}</small>` : "—",
      detail: latest ? `迁移 #${latest.id} · v${latest.old_version} → v${latest.new_version}` : "尚未创建迁移",
      warn: latest && latest.status === "needs_attention",
    },
    {
      label: "冲突证据",
      value: stats.conflicts,
      detail: stats.conflicts ? "已停止相关单元，旧路由未退役" : "无冲突；核验通过后才切换",
      warn: stats.conflicts > 0,
    },
  ];
  $("#stat-row").innerHTML = cards.map((c) => `
    <div class="stat ${c.warn ? "warn" : ""}">
      <div class="label">${esc(c.label)}</div>
      <div class="value">${c.value}</div>
      <div class="detail">${esc(c.detail)}</div>
    </div>`).join("");
}

function renderShards(data) {
  const counts = data.counts || [];
  const owned = data.owned_by_shard || {};
  const maxOwned = Math.max(1, ...Object.values(owned));
  const sectors = counts.map((c) => {
    const ownedHere = owned[String(c.shard)] || 0;
    const retained = c.customers - ownedHere;
    return `
      <div class="shard-card">
        <div class="shard-head">
          <div class="shard-name">${shardSwatch(c.shard)}${esc(shardName(c.shard))}</div>
          <span class="shard-tag ${ownedHere ? "tag-live" : ""}">${ownedHere ? "有归属客户" : "空闲"}</span>
        </div>
        <div class="shard-metrics">
          <div class="shard-metric"><div class="k">归属客户</div><div class="v">${ownedHere}</div></div>
          <div class="shard-metric"><div class="k">物理客户行</div><div class="v">${c.customers}</div></div>
          <div class="shard-metric"><div class="k">订单</div><div class="v">${c.orders}</div></div>
        </div>
        <div class="shard-bar"><i style="width:${Math.max(2, (ownedHere / maxOwned) * 100)}%;background:${SHARD_COLORS[c.shard % 3]}"></i></div>
        <div class="shard-path">${retained > 0
          ? `另有 ${retained} 行是已切换后保留的旧副本（不可路由，等待清理）`
          : "无冗余副本"} · ${c.orphan_orders ? `<b>${c.orphan_orders} 条孤儿订单</b>` : "无孤儿订单"}</div>
      </div>`;
  }).join("");
  $("#shard-cards").innerHTML = sectors;

  $("#version-table tbody").innerHTML = data.versions.map((v) => `
    <tr>
      <td><span class="mono">v${v.version}</span></td>
      <td class="mono">${esc(v.algorithm)}</td>
      <td class="num">${v.shard_count}</td>
      <td>${v.status === "live"
        ? '<span class="chip good"><span class="dot"></span>live</span>'
        : v.status === "staging"
          ? '<span class="chip warning"><span class="dot"></span>staging</span>'
          : '<span class="chip neutral"><span class="dot"></span>retired</span>'}</td>
      <td class="dim">${dateTime(v.created_at)}${v.retired_at ? ` → 退役 ${dateTime(v.retired_at)}` : ""}</td>
    </tr>`).join("");

  $("#shard-files").textContent = data.shard_files
    .map((p) => p.split("/").pop()).join(" · ");
}

function renderCustomerSelect(data) {
  const select = $("#customer-select");
  const current = state.selectedCustomer || select.value;
  const customers = data.customers || [];
  select.innerHTML = customers.map((c) => `
    <option value="${esc(c.customer_id)}">${esc(c.customer_id)} · ${esc(c.name)}
      （${esc(shardName(c.shard))} · v${c.version}）</option>`).join("");
  if (!customers.length) {
    select.innerHTML = '<option value="">暂无客户</option>';
    return;
  }
  const stillThere = customers.some((c) => c.customer_id === current);
  select.value = stillThere ? current : customers[0].customer_id;
  state.selectedCustomer = select.value;
  $("#order-customer").value = state.selectedCustomer;
}

function unitStatusPill(status) {
  const label = UNIT_LABELS[status] || status;
  return `<span class="status-pill st-${esc(status)}"><span class="dot"></span>${esc(label)}</span>`;
}

function checkCell(unit) {
  const verify = unit.checks.filter((c) => c.phase === "verify");
  const switched = unit.checks.filter((c) => c.phase === "switch");
  const cleanup = unit.checks.filter((c) => c.phase === "cleanup");
  const last = switched[switched.length - 1] || verify[verify.length - 1] || cleanup[cleanup.length - 1];
  if (!last) {
    return '<span class="check-pending">未核验</span>';
  }
  const ok = last.equal === 1;
  const kind = last.phase === "switch" ? "切换后" : last.phase === "cleanup" ? "清理后" : "核验";
  const fp = (last.source_hash || last.target_hash || "").slice(0, 8) || "—";
  return `<span class="${ok ? "check-ok" : "check-bad"}">${ok ? "✓" : "✕"} ${kind} ${last.source_orders}/${last.target_orders}</span>
    <span class="cell-sub mono" title="内容指纹">${ok ? "指纹一致" : "指纹不一致"} · ${fp}</span>`;
}

function unitActions(unit) {
  const mid = unit.migration_id;
  const cid = encodeURIComponent(unit.customer_id);
  const base = `/api/migrations/${mid}/units/${cid}`;
  const ops = [];
  if (unit.status === "planned" || unit.status === "interrupted" || unit.status === "copying") {
    ops.push(`<button class="btn micro" data-act="copy" data-base="${base}" data-cid="${esc(unit.customer_id)}" data-max="2">复制 2 行</button>`);
    ops.push(`<button class="btn micro" data-act="copy" data-base="${base}" data-cid="${esc(unit.customer_id)}">复制完成</button>`);
  }
  if (["copied", "interrupted", "verified"].includes(unit.status)) {
    ops.push(`<button class="btn micro" data-act="conflict" data-base="${base}">注入冲突</button>`);
  }
  if (unit.status === "copied" || unit.status === "interrupted" || unit.status === "verified") {
    ops.push(`<button class="btn micro" data-act="verify" data-base="${base}">核验</button>`);
  }
  if (unit.status === "verified" || unit.status === "copied" || unit.status === "interrupted") {
    ops.push(`<button class="btn micro primary" data-act="switch" data-base="${base}">核验并切换</button>`);
  }
  if (unit.status === "switched" && !unit.cleaned_at) {
    ops.push(`<button class="btn micro ghost" data-act="cleanup" data-base="${base}">清理旧副本</button>`);
  }
  if (unit.status === "conflict") {
    ops.push('<span class="dim" style="font-size:11px">已停止，人工处理</span>');
  }
  return `<div class="unit-ops">${ops.join("")}</div>`;
}

function renderMigration(data) {
  const mid = data.latest_migration;
  const chip = $("#migration-status-chip");
  const meta = $("#migration-meta");
  if (!mid) {
    chip.className = "chip neutral";
    chip.innerHTML = '<span class="dot"></span>尚未规划';
    meta.hidden = true;
    $("#progress-wrap").hidden = true;
    $("#units-table tbody").innerHTML = "";
    $("#units-empty").hidden = false;
    $("#conflict-panel").hidden = true;
    renderPlan(null);
    return;
  }
  const migration = mid.migration;
  const [tone, label] = STATUS_CHIP[migration.status] || ["neutral", migration.status];
  chip.className = `chip ${tone}`;
  chip.innerHTML = `<span class="dot"></span>${esc(label)} · #${migration.id}`;

  meta.hidden = false;
  meta.innerHTML = `
    <div class="kv"><span class="k">路由</span><span class="v">v${migration.old_version} → v${migration.new_version}</span></div>
    <div class="kv"><span class="k">分片数</span><span class="v">${migration.plan.old_shard_count} → ${migration.plan.new_shard_count}</span></div>
    <div class="kv"><span class="k">需搬迁</span><span class="v">${migration.plan.movers}</span></div>
    <div class="kv"><span class="k">原地不动</span><span class="v">${migration.plan.stay}</span></div>
    <div class="kv"><span class="k">开始</span><span class="v">${shortTime(migration.created_at)}</span></div>`;

  const units = mid.units || [];
  const total = units.length;
  const counts = { switched: 0, copied: 0, conflict: 0, pending: 0 };
  units.forEach((u) => {
    if (u.status === "switched") counts.switched += 1;
    else if (u.status === "conflict") counts.conflict += 1;
    else if (u.status === "planned") counts.pending += 1;
    else counts.copied += 1;
  });
  const progress = $("#progress-wrap");
  progress.hidden = false;
  $("#progress-text").textContent = `${counts.switched} / ${total}`;
  $("#segbar").innerHTML = units.map((u) => {
    const color = u.status === "switched" ? "var(--good)"
      : u.status === "conflict" ? "var(--critical)"
        : u.status === "verified" ? "var(--series-3)"
          : u.status === "copied" ? "var(--serious)"
            : u.status === "interrupted" ? "var(--warning)"
              : u.status === "copying" ? "var(--series-1)" : "var(--grid)";
    return `<span class="seg" title="${esc(u.customer_id)} · ${esc(UNIT_LABELS[u.status] || u.status)}" style="background:${color}"></span>`;
  }).join("");
  $("#segbar-legend").innerHTML = [
    ["var(--good)", `已切换 ${counts.switched}`],
    ["var(--serious)", `已复制待核验/切换 ${counts.copied}`],
    ["var(--grid)", `待迁移 ${counts.pending}`],
    ["var(--critical)", `冲突 ${counts.conflict}`],
  ].map(([color, text]) => `<span class="item"><span class="sw" style="background:${color}"></span>${esc(text)}</span>`).join("");

  $("#units-table tbody").innerHTML = units.map((u) => {
    const movedLabel = `${u.source_shard} → ${u.target_shard}`;
    return `<tr>
      <td>
        <button class="linkbtn" data-pick="${esc(u.customer_id)}">${esc(u.customer_id)}</button>
        <span class="cell-sub">单元 #${u.id} · ${esc(UNIT_LABELS[u.status] || u.status)}</span>
      </td>
      <td><span class="mono">${movedLabel}</span><span class="cell-sub">v${u.old_version} → v${u.new_version}</span></td>
      <td>${unitStatusPill(u.status)}</td>
      <td class="num">${u.source_orders} 单<span class="cell-sub">${u.source_customers} 客户</span></td>
      <td class="num">${u.staged_rows}<span class="cell-sub">行含客户</span></td>
      <td>${checkCell(u)}</td>
      <td>${unitActions(u)}</td>
    </tr>`;
  }).join("");
  $("#units-empty").hidden = units.length > 0;

  const conflicts = mid.conflicts || [];
  const panel = $("#conflict-panel");
  if (conflicts.length) {
    panel.hidden = false;
    panel.innerHTML = `
      <div class="cp-head">⚠ 发现 ${conflicts.length} 条冲突证据，相关单元已停止</div>
      <div class="cp-body">${conflicts.slice(0, 3).map((c) => `
        <div><b>${esc(c.customer_id)}</b> · ${esc(c.table_name)}/${esc(c.row_key)} · ${esc(c.detail)}</div>
      `).join("")}${conflicts.length > 3 ? `<div class="dim">…另有 ${conflicts.length - 3} 条，见下方冲突证据表</div>` : ""}</div>`;
  } else {
    panel.hidden = true;
  }
  renderPlan(mid);
  renderConflictTable(mid.conflicts || []);
}

function renderPlan(detail) {
  const tbody = $("#plan-table tbody");
  if (!detail) {
    tbody.innerHTML = "";
    $("#plan-empty").hidden = false;
    return;
  }
  const plan = detail.migration.plan;
  $("#plan-empty").hidden = true;
  tbody.innerHTML = plan.customers.map((c) => `
    <tr>
      <td class="mono">${esc(c.customer_id)}</td>
      <td class="num">${c.pinned_shard}</td>
      <td class="num">${c.old_shard}</td>
      <td class="num">${c.new_shard}</td>
      <td>${c.moves
        ? `<span class="chip ${c.new_shard >= 2 ? "serious" : "warning"}"><span class="dot"></span>搬迁 ${c.pinned_shard} → ${c.new_shard}</span>`
        : '<span class="chip neutral"><span class="dot"></span>原地保留</span>'}</td>
    </tr>`).join("");
}

const FIELD_LABELS = {
  order_id: "订单号", customer_id: "客户", amount_cents: "金额（分）",
  created_at: "创建时间", note: "备注", name: "名称",
};

function evidenceDiff(c) {
  const source = c.source || {};
  const target = c.target || {};
  if (!c.source || !c.target) {
    const side = c.source ? "源端存在该行，暂存副本缺失" : "暂存副本存在该行，源端已无";
    return `<div class="diff-line"><span class="diff-key">${side}</span></div>`;
  }
  const keys = Array.from(new Set([...Object.keys(source), ...Object.keys(target)]));
  const rows = keys.filter((k) => JSON.stringify(source[k]) !== JSON.stringify(target[k])
    && k !== "routed_version" && k !== "write_seq")
    .map((k) => `
      <div class="diff-line">
        <span class="diff-key">${esc(FIELD_LABELS[k] || k)}</span>
        <span class="diff-val source">${esc(fmtValue(source[k]))}</span>
        <span class="diff-arrow">→</span>
        <span class="diff-val target">${esc(fmtValue(target[k]))}</span>
      </div>`).join("");
  return rows
    ? `<div class="diff-head"><span></span><span>源端</span><span></span><span>暂存</span></div>${rows}`
    : '<div class="diff-line dim">两侧内容完全相同（冲突来自缺失行）</div>';
}

function fmtValue(value) {
  if (value === null || value === undefined) return "∅";
  if (typeof value === "string" && value.length > 40) return value.slice(0, 40) + "…";
  return String(value);
}

function renderConflictTable(conflicts) {
  const tbody = $("#conflict-table tbody");
  $("#conflict-count").textContent = `${conflicts.length} 条`;
  tbody.innerHTML = conflicts.map((c) => `
    <tr>
      <td class="mono">${esc(c.customer_id)}<span class="cell-sub">${shortTime(c.created_at)}</span></td>
      <td>${c.table_name === "orders" ? "订单" : "客户"}<span class="cell-sub mono">${esc(c.row_key)}</span></td>
      <td><span class="chip neutral">${esc(c.phase === "copy" ? "续传比对" : c.phase === "verify" ? "核验" : "演练注入")}</span></td>
      <td class="evidence-cell"><div class="diff">${evidenceDiff(c)}</div>
        <div class="cell-sub">${esc(c.detail)}</div></td>
      <td class="dim">单元停止<br>两边均保留</td>
    </tr>`).join("");
  $("#conflict-empty").hidden = conflicts.length > 0;
}

function renderEvents(events) {
  $("#event-list").innerHTML = events.map((e) => `
    <li>
      <time>${shortTime(e.at)}</time>
      <div><span class="event-kind ${esc(e.kind)}">${esc(EVENT_KIND[e.kind] || e.kind)}</span>${esc(e.detail)}
        ${e.customer_id ? `<span class="dim"> · ${esc(e.customer_id)}</span>` : ""}</div>
    </li>`).join("");
}

function renderQueryLog(entries) {
  $("#query-log-table tbody").innerHTML = entries.map((q) => `
    <tr>
      <td class="dim">${shortTime(q.at)}</td>
      <td class="mono">${esc(q.customer_id)}</td>
      <td>${q.mode === "owner" ? "所有者" : "三视图"}</td>
      <td class="num">${q.owner_shard}</td>
      <td class="num">${q.orders_count}</td>
      <td>${q.mode === "owner"
        ? `分片 ${q.owner_shard} · v${q.owner_version}`
        : `哈希 v→分片 ${q.hash_shard}`}</td>
    </tr>`).join("") || '<tr><td colspan="6" class="dim">还没有查询</td></tr>';
}

/* ------------------------------------------------------------ query panel */
function queryResultView(result) {
  if (!result.found) {
    return `<div class="result-empty">分片 ${result.shard} 上没有该客户的数据。
      ${result.basis === "raw-hash" ? "这是诊断直读——所有者可能在别处。" : ""}</div>`;
  }
  return `<div class="orders">
    ${result.orders.map((o) => `
      <div class="order-item">
        <span><span class="oid">${esc(o.order_id)}</span> · ${esc(o.note)}</span>
        <span class="amt">${money(o.amount_cents)}</span>
      </div>`).join("")}
  </div>`;
}

function presenceView(result) {
  return `<div class="presence">
    <span class="dim" style="font-size:11.5px;align-self:center">数据实际所在：</span>
    ${result.presence.map((p) => `
      <span class="presence-pill ${p.shard === result.owner_shard ? "here" : ""}">
        ${shardSwatch(p.shard)} ${esc(shardName(p.shard))}
        <b>${p.present ? `${p.orders} 单` : "无"}</b>
        ${p.shard === result.owner_shard ? "· 所有者" : ""}
      </span>`).join("")}
  </div>`;
}

function renderQuery(result) {
  const box = $("#query-result");
  box.hidden = false;
  box.innerHTML = `
    <div class="result-head">
      <span class="result-title">${esc(result.customer_id)} · ${esc(result.customer?.name || "未找到")}</span>
      <span class="result-meta">
        <span class="kv"><span class="k">落点</span><span class="v">${esc(shardName(result.shard))}</span></span>
        <span class="kv"><span class="k">版本</span><span class="v">v${result.version}</span></span>
        <span class="kv"><span class="k">依据</span><span class="v">${result.basis === "binding" ? "绑定（所有者）" : "版本哈希（诊断）"}</span></span>
        <span class="kv"><span class="k">订单</span><span class="v">${result.orders_count}</span></span>
        <span class="kv"><span class="k">合计</span><span class="v">${money(result.total_cents)}</span></span>
      </span>
    </div>
    <div class="result-note">${esc(result.note)}</div>
    ${queryResultView(result)}
    ${presenceView(result)}`;
  $("#customer-context").textContent = result.context;
  $("#customer-context").className = "chip " +
    (result.context.includes("冲突") ? "critical"
      : result.context.includes("已切换") ? "good"
        : result.context.includes("中断") ? "warning" : "neutral");
  $("#order-customer").value = result.customer_id;
  $("#customer-select").value = result.customer_id;
  state.selectedCustomer = result.customer_id;
}

async function doQuery() {
  const cid = $("#customer-select").value;
  const mode = $("#query-mode").value;
  if (!cid) return;
  if (mode === "compare") {
    return doCompare(cid);
  }
  const result = await api(`/api/query?customer_id=${encodeURIComponent(cid)}&mode=owner`);
  state.lastQuery = result;
  renderQuery(result);
  await refreshLogOnly();
}

async function doCompare(cid) {
  const versions = state.data.versions.map((v) => v.version);
  const owner = await api(`/api/query?customer_id=${encodeURIComponent(cid)}&mode=owner`);
  const raws = [];
  for (const v of versions) {
    raws.push(await api(`/api/query?customer_id=${encodeURIComponent(cid)}&mode=raw_v${v}`));
  }
  state.lastQuery = owner;
  const box = $("#query-result");
  box.hidden = false;
  const ownerRow = state.data.latest_migration
    ? state.data.latest_migration.units.find((u) => u.customer_id === cid)
    : null;
  const switching = ownerRow && ["copied", "verified", "interrupted"].includes(ownerRow.status);
  box.innerHTML = `
    <div class="result-head">
      <span class="result-title">${esc(cid)} · ${esc(owner.customer?.name || "未找到")}</span>
      <span class="result-meta">
        <span class="kv"><span class="k">所有者</span><span class="v">${esc(shardName(owner.owner_shard))} · v${owner.owner_version}</span></span>
        <span class="kv"><span class="k">订单</span><span class="v">${owner.orders_count}</span></span>
        <span class="kv"><span class="k">合计</span><span class="v">${money(owner.total_cents)}</span></span>
      </span>
    </div>
    <div class="result-note">${switching
      ? `该单元尚未切换：写入与查询仍归属 ${esc(shardName(owner.owner_shard))}；目标分片的暂存副本不对外提供读取。`
      : esc(owner.note)}</div>
    <div class="compare-grid">
      <div class="compare-cell owner-cell">
        <div class="cell-head"><span class="cell-name">所有者 · 页面实际读取</span><span class="cell-shard">分片 ${owner.owner_shard}</span></div>
        <div class="cell-line">v${owner.owner_version} · ${owner.orders_count} 单 · ${money(owner.total_cents)}</div>
        <div class="cell-line">${esc(owner.note)}</div>
      </div>
      ${raws.map((r) => `
        <div class="compare-cell">
          <div class="cell-head"><span class="cell-name">v${r.version} 哈希直读</span><span class="cell-shard">分片 ${r.shard}</span></div>
          <div class="cell-line">${r.found ? `${r.orders_count} 单 · ${money(r.total_cents)}` : "无数据"}</div>
          <div class="cell-line">${r.found
            ? (r.shard === owner.owner_shard ? "与所有者一致" : "副本/未切换数据，不对外路由")
            : "该落点尚无数据"}</div>
        </div>`).join("")}
    </div>
    ${presenceView(owner)}`;
  $("#customer-context").textContent = owner.context;
  $("#customer-context").className = "chip " + (owner.context.includes("冲突") ? "critical"
    : owner.context.includes("已切换") ? "good" : "neutral");
  $("#order-customer").value = cid;
  $("#query-mode").value = "compare";
  state.selectedCustomer = cid;
  await refreshLogOnly();
}

async function refreshLogOnly() {
  const log = await api("/api/query-log");
  renderQueryLog(log.entries);
}

/* ------------------------------------------------------------------ load */
async function loadState({ keepQuery = true } = {}) {
  const data = await api("/api/state");
  state.data = data;
  renderStats(data);
  renderShards(data);
  renderCustomerSelect(data);
  renderMigration(data);
  renderEvents(data.events);
  const log = await api("/api/query-log");
  renderQueryLog(log.entries);
  if (keepQuery && state.lastQuery) {
    const cid = state.lastQuery.customer_id;
    try {
      const fresh = await api(`/api/query?customer_id=${encodeURIComponent(cid)}&mode=${state.lastQuery.mode}`);
      state.lastQuery = fresh;
      renderQuery(fresh);
    } catch { /* customer may no longer exist */ }
  } else if (!state.lastQuery && data.customers.length) {
    $("#customer-select").value = state.selectedCustomer || data.customers[0].customer_id;
    $("#order-customer").value = $("#customer-select").value;
  }
}

/* --------------------------------------------------------------- actions */
async function runScenario(name) {
  const mid = state.data?.latest_migration?.migration?.id;
  const units = state.data?.latest_migration?.units || [];
  const fresh = units.filter((u) => u.status !== "switched" && u.status !== "conflict");
  if (!mid || !units.length) {
    toast("请先规划迁移，并保留至少一个未完成单元", true);
    return;
  }
  if (!fresh.length && name !== "switch-view") {
    toast("迁移单元已全部切换或停止，没有可演练的未完成单元", true);
    return;
  }
  const unit = (fresh[0] || units[0]).customer_id;
  const base = `/api/migrations/${mid}/units/${encodeURIComponent(unit)}`;
  await withBusy(null, async () => {
    if (name === "interrupt-write-resume") {
      await post(`${base}/copy`, { max_new_rows: 1 });
      await post("/api/orders", {
        customer_id: unit, amount_cents: 2600, note: "复制中断期间补写（仍归源分片）",
      });
      await loadState();
      await post(`${base}/copy`, {});
      await post(`${base}/verify`, { switch: true });
      await post("/api/orders", {
        customer_id: unit, amount_cents: 3300, note: "切换后补写（归目标分片）",
      });
      state.selectedCustomer = unit;
      await loadState();
      await doCompare(unit);
      toast(`演练完成：${unit} 中断 → 补写 → 续传 → 核验 → 切换`);
    } else if (name === "conflict") {
      await post(`${base}/copy`, { max_new_rows: 2 });
      await post(`${base}/conflict`, {});
      const res = await post(`${base}/copy`, {});
      state.selectedCustomer = unit;
      await loadState();
      if (res.status !== "conflict") {
        toast("预期冲突但续传未发现差异，请重建演示数据后重试", true);
      } else {
        toast(`单元 ${unit} 续传时发现内容冲突：已停止，两侧证据保留，未覆盖`);
      }
    } else if (name === "switch-view") {
      const chosen = fresh[0] || units[0];
      if (chosen && ["planned", "copying", "interrupted"].includes(chosen.status)) {
        await post(`${base}/copy`, {});
      }
      state.selectedCustomer = unit;
      await loadState();
      await doCompare(unit);
      toast("对照视角：所有者、各版本哈希落点与物理副本");
    }
  });
}

function bindEvents() {
  $("#query-btn").addEventListener("click", () => withBusy($("#query-btn"), doQuery));
  $("#customer-select").addEventListener("change", () => {
    state.selectedCustomer = $("#customer-select").value;
    $("#order-customer").value = state.selectedCustomer;
  });
  $("#query-mode").addEventListener("change", () => {
    if ($("#query-mode").value === "compare") withBusy($("#query-btn"), doQuery);
  });

  $("#order-btn").addEventListener("click", () => withBusy($("#order-btn"), async () => {
    const cid = $("#customer-select").value;
    const amount = Number($("#order-amount").value);
    const note = $("#order-note").value.trim();
    const res = await post("/api/orders", { customer_id: cid, amount_cents: amount, note });
    const line = $("#write-result");
    line.hidden = false;
    line.textContent = `订单 ${res.order.order_id} 已写入 分片 ${res.shard}`
      + `（所有者绑定 · v${res.version}，金额 ${money(res.order.amount_cents)}）`;
    $("#order-note").value = "";
    await loadState();
    if (state.lastQuery && state.lastQuery.customer_id === cid) {
      state.lastQuery = await api(`/api/query?customer_id=${encodeURIComponent(cid)}&mode=owner`);
      renderQuery(state.lastQuery);
    }
  }));

  $("#plan-btn").addEventListener("click", () => withBusy($("#plan-btn"), async () => {
    const res = await post("/api/migrations", {});
    toast(res.reused ? res.message : `已规划迁移 #${res.migration_id}：${res.plan.movers} 位客户需搬迁`);
    await loadState();
  }));

  $("#run-btn").addEventListener("click", () => withBusy($("#run-btn"), async () => {
    const mid = state.data?.latest_migration?.migration?.id;
    if (!mid) { toast("请先规划迁移", true); return; }
    const res = await post(`/api/migrations/${mid}/run`, { stop_at_interrupt: true });
    await loadState();
    const done = res.steps.filter((s) => s.status === "switched").length;
    const conflicts = res.steps.filter((s) => s.status === "conflict").length;
    toast(conflicts ? `已处理 ${done} 个单元，遇到 ${conflicts} 个冲突后停止` : `本轮切换 ${done} 个单元`);
  }));

  $("#cleanup-all-btn").addEventListener("click", () => withBusy($("#cleanup-all-btn"), async () => {
    const mid = state.data?.latest_migration?.migration?.id;
    if (!mid) { toast("没有迁移可清理", true); return; }
    const res = await post(`/api/migrations/${mid}/cleanup`, {});
    await loadState();
    toast(`已清理 ${res.cleaned} 个已切换单元的旧副本`);
  }));

  $("#audit-btn").addEventListener("click", () => withBusy($("#audit-btn"), async () => {
    const audit = await api("/api/audit");
    const line = $("#audit-line");
    line.hidden = false;
    line.className = "audit-line " + (audit.ok ? "ok" : "bad");
    line.innerHTML = audit.ok
      ? `✓ 审计通过：${audit.customers} 位客户各自只有一个所有者分片；${audit.bindings} 条绑定全部指向存在数据的表；无孤儿订单、无非所有者分片内容冲突。`
      : `✕ 审计发现 ${audit.problems.length} 个问题：${audit.problems.map(esc).join("；")}`;
  }));

  $("#reset-demo").addEventListener("click", () => withBusy(null, async () => {
    await post("/api/reset", {});
    await post("/api/seed", {});
    state.lastQuery = null;
    await loadState({ keepQuery: false });
    toast("演示数据已重建为 2 分片 / v1");
  }));

  document.addEventListener("click", (event) => {
    const pick = event.target.closest("[data-pick]");
    if (pick) {
      state.selectedCustomer = pick.dataset.pick;
      $("#customer-select").value = state.selectedCustomer;
      withBusy(null, doQuery);
      return;
    }
    const scenario = event.target.closest("[data-scenario]");
    if (scenario) {
      runScenario(scenario.dataset.scenario);
      return;
    }
    const act = event.target.closest("[data-act]");
    if (!act) return;
    withBusy(act, async () => {
      const base = act.dataset.base;
      if (act.dataset.act === "copy") {
        const body = act.dataset.max ? { max_new_rows: Number(act.dataset.max) } : {};
        const res = await post(`${base}/copy`, body);
        toast(res.note || `${res.customer_id}：${UNIT_LABELS[res.status] || res.status}，暂存 ${res.staged_total} 行`);
      } else if (act.dataset.act === "verify") {
        const res = await post(`${base}/verify`, { switch: false });
        toast(res.status === "verified" ? "核验通过：数量与内容一致" : "核验未通过");
      } else if (act.dataset.act === "switch") {
        const res = await post(`${base}/verify`, { switch: true });
        toast(res.status === "switched"
          ? `${res.customer_id} 已原子切换：${res.source} → ${res.target}（v${res.version}）`
          : `${res.customer_id} 未切换`);
      } else if (act.dataset.act === "conflict") {
        await post(`${base}/conflict`, {});
        toast("已在源端改动一行；核验时会发现两侧内容不同并停止");
      } else if (act.dataset.act === "cleanup") {
        const res = await post(`${base}/cleanup`, {});
        toast(`清理完成：旧分片删除 ${res.removed_orders} 单，目标仍 ${res.target_orders} 单`);
      }
      await loadState();
    });
  });

  $("#theme-toggle").addEventListener("click", () => {
    const root = document.documentElement;
    const dark = root.dataset.theme === "dark" ||
      (!root.dataset.theme && matchMedia("(prefers-color-scheme: dark)").matches);
    root.dataset.theme = dark ? "light" : "dark";
    $("#theme-toggle").textContent = dark ? "深色" : "浅色";
  });
  const prefersDark = matchMedia("(prefers-color-scheme: dark)").matches;
  $("#theme-toggle").textContent = prefersDark ? "浅色" : "深色";
}

bindEvents();
loadState({ keepQuery: false }).catch((err) => toast(err.message, true));

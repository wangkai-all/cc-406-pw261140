/* 监测告警系统前端：原生 JS，零依赖。 */
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = s => String(s ?? "").replace(/[&<>"']/g,
  c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || res.status);
  return data;
}

// ---------- Tab 切换 ----------
$$("nav button").forEach(btn => btn.addEventListener("click", () => {
  $$("nav button").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  $$(".tab").forEach(t => t.classList.remove("active"));
  $(`#tab-${btn.dataset.tab}`).classList.add("active");
  loaders[btn.dataset.tab]?.();
}));

const loaders = {
  alerts: loadAlerts, baselines: loadBaselines, maintenance: loadMaintenance,
  jobs: loadJobs, outbox: loadOutbox,
};

// ---------- 告警 ----------
async function loadAlerts() {
  const rows = await api("/api/alerts");
  const tbody = $("#alerts-table tbody");
  tbody.innerHTML = rows.map(a => `
    <tr data-id="${a.alert_id}" class="alert-row">
      <td>${a.alert_id}</td>
      <td>${esc(a.sensor_id)}<br><small>${esc(a.org_id)}</small></td>
      <td><small>${esc(a.window_start)}</small></td>
      <td><span class="sev sev-${a.severity}">${a.severity}</span></td>
      <td>${esc(a.status)}</td>
      <td>${a.analysis_versions} 版</td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">暂无告警</td></tr>`;
  $$(".alert-row", tbody).forEach(tr =>
    tr.addEventListener("click", () => showAlert(tr.dataset.id)));
}

async function showAlert(id) {
  const d = await api(`/api/alerts/${id}`);
  const t = d.threshold_snapshot, s = d.stat_snapshot;
  $("#alert-detail").innerHTML = `
    <h2>告警 #${d.alert.alert_id} <span class="sev sev-${d.alert.severity}">${d.alert.severity}</span>
      <small>当前状态：${esc(d.current_status)}</small></h2>
    <p class="hint">事实行写入时间 ${esc(d.alert.triggered_at)}；fingerprint <code>${esc(d.alert.fingerprint.slice(0, 16))}…</code></p>

    <h3>窗口内原始点（${d.raw_points.length} 个）</h3>
    ${sparkline(d.raw_points, t)}

    <h3>事件当时的阈值依据（快照，不可变）</h3>
    <table class="kv">
      <tr><th>规则</th><td>${esc(t.rule_kind)}，基线 #${t.baseline_id}（小时槽 ${t.hour_of_day}）</td></tr>
      <tr><th>基线中位数 / MAD</th><td>${t.baseline_median} / ${t.baseline_mad}</td></tr>
      <tr><th>z 阈值</th><td>warning ≥ ${t.z_warning}，critical ≥ ${t.z_critical}</td></tr>
      <tr><th>基线批准/生效</th><td>${esc(t.approved_at)} → ${esc(t.effective_from)}</td></tr>
    </table>

    <h3>当时窗口统计（快照）</h3>
    <table class="kv">
      <tr><th>n / 中位数 / 均值</th><td>${s.n} / ${fmt(s.median)} / ${fmt(s.mean)}</td></tr>
      <tr><th>min / max / p95</th><td>${fmt(s.min_value)} / ${fmt(s.max_value)} / ${fmt(s.p95)}</td></tr>
      <tr><th>观测值 / 维护中</th><td>${fmt(d.alert.observed_value)} / ${s.in_maintenance ? "是" : "否"}</td></tr>
      <tr><th>点指纹</th><td><code>${esc(s.points_digest)}</code></td></tr>
    </table>

    <h3>分析版本（v1 = 当时依据，后续 = 更正分析版）</h3>
    ${d.analyses.map(a2 => `
      <div class="analysis verdict-${a2.verdict}">
        <strong>v${a2.version} · ${a2.mode} · ${a2.verdict}</strong>
        <small>${esc(a2.computed_at)}${a2.recalc_job_id ? " · " + esc(a2.recalc_job_id) : ""}</small>
        <p>${esc(a2.rationale)}</p>
        <small>使用基线 #${a2.baseline_id_used ?? "无"}</small>
      </div>`).join("")}

    <h3>状态流转</h3>
    <div class="row">
      <select id="new-status">
        <option>open</option><option>ack</option><option>resolved</option><option>suppressed</option>
      </select>
      <button id="status-btn">变更状态（留痕，不改事实）</button>
    </div>`;
  $("#status-btn").addEventListener("click", async () => {
    await api(`/api/alerts/${id}/status`, {
      method: "POST",
      body: { to_status: $("#new-status").value, note: "web 手工变更" },
    });
    showAlert(id); loadAlerts();
  });
}

function fmt(v) { return v == null ? "—" : (Math.round(v * 1000) / 1000); }

/* SVG 迷你图：原始点折线 + 基线中位数线 + 上下阈值线 */
function sparkline(points, t) {
  if (!points.length) return "<p class='hint'>无点</p>";
  const W = 560, H = 220, pad = 34;
  const vals = points.map(p => p.value);
  const lo0 = Math.min(...vals, t.baseline_median - t.z_critical * t.baseline_mad);
  const hi0 = Math.max(...vals, t.baseline_median + t.z_critical * t.baseline_mad);
  const lo = lo0 - (hi0 - lo0) * 0.08, hi = hi0 + (hi0 - lo0) * 0.08;
  const x = i => pad + i * (W - 2 * pad) / Math.max(points.length - 1, 1);
  const y = v => H - pad - (v - lo) * (H - 2 * pad) / (hi - lo);
  const line = v => `<line x1="${pad}" x2="${W - pad}" y1="${y(v)}" y2="${y(v)}"
     stroke="var(--accent)" stroke-dasharray="4 3" stroke-width="1"/>
     <text x="${W - pad + 2}" y="${y(v) + 3}" class="svg-label">${fmt(v)}</text>`;
  const path = points.map((p, i) => `${i ? "L" : "M"}${x(i)},${y(p.value)}`).join(" ");
  return `<svg viewBox="0 0 ${W} ${H}" class="spark">
    ${line(t.baseline_median + t.z_critical * t.baseline_mad)}
    ${line(t.baseline_median - t.z_critical * t.baseline_mad)}
    <line x1="${pad}" x2="${W - pad}" y1="${y(t.baseline_median)}" y2="${y(t.baseline_median)}"
      stroke="var(--ok)" stroke-width="1.5"/>
    <text x="${W - pad + 2}" y="${y(t.baseline_median) + 3}" class="svg-label">中位数</text>
    <path d="${path}" fill="none" stroke="var(--fg)" stroke-width="1.5"/>
    ${points.map((p, i) => `<circle cx="${x(i)}" cy="${y(p.value)}" r="2.5"
       fill="${Math.abs(p.value - t.baseline_median) > t.z_warning * t.baseline_mad ? "var(--crit)" : "var(--fg)"}"/>`).join("")}
  </svg>
  <small class="hint">虚线=critical 阈值，绿线=基线中位数，红点=超出 warning 阈值的原始点</small>`;
}

// ---------- 基线 ----------
async function loadBaselines() {
  const rows = await api("/api/baselines");
  $("#baselines-table tbody").innerHTML = rows.map(b => `
    <tr>
      <td>${b.baseline_id}</td><td>${esc(b.sensor_id)} <small>${esc(b.sensor_name)}</small></td>
      <td>${String(b.hour_of_day).padStart(2, "0")}时</td>
      <td>${fmt(b.median)}</td><td>${fmt(b.mad)}</td>
      <td>${fmt(b.z_warning)}/${fmt(b.z_critical)}</td>
      <td>${b.sample_count}</td>
      <td><span class="status-${b.status}">${b.status}</span></td>
      <td><small>${esc(b.effective_from ?? "—")}</small></td>
      <td>${b.status === "proposed" ? `<button class="approve-btn" data-id="${b.baseline_id}">批准</button>` : "—"}</td>
    </tr>`).join("") || `<tr><td colspan="10" class="hint">暂无基线</td></tr>`;
  $$(".approve-btn").forEach(btn => btn.addEventListener("click", () =>
    openApproveModal(btn.dataset.id)));
}

function openApproveModal(id) {
  $("#modal-title").textContent = `批准基线 #${id}`;
  $("#modal-body").innerHTML = `
    <label>z_warning <input id="m-zw" type="number" step="0.1" value="3.5"></label>
    <label>z_critical <input id="m-zc" type="number" step="0.1" value="6.0"></label>
    <label>生效时间（留空=立即，仅对之后判定生效）<input id="m-eff" placeholder="2026-11-17T00:00:00Z"></label>
    <p class="hint">批准后同测点同时槽旧基线退休；历史告警保留其原始快照不被覆盖。</p>`;
  $("#modal").classList.remove("hidden");
  $("#modal-close").onclick = () => $("#modal").classList.add("hidden");
  let btn = $(".confirm-btn", $(".modal-box"));
  if (!btn) {
    btn = document.createElement("button");
    btn.className = "confirm-btn";
    $(".modal-box").insertBefore(btn, $("#modal-close").parentElement);
  }
  btn.textContent = "确认批准";
  btn.onclick = async () => {
    await api(`/api/baselines/${id}/approve`, {
      method: "POST",
      body: {
        z_warning: parseFloat($("#m-zw").value),
        z_critical: parseFloat($("#m-zc").value),
        effective_from: $("#m-eff").value || null,
      },
    });
    $("#modal").classList.add("hidden");
    loadBaselines();
  };
}

$("#train-form").addEventListener("submit", async e => {
  e.preventDefault();
  const f = new FormData(e.target);
  const ids = (f.get("sensor_ids") || "").split(",").map(s => s.trim()).filter(Boolean);
  const data = await api("/api/baselines/train", {
    method: "POST",
    body: { train_start: f.get("train_start"), train_end: f.get("train_end"), sensor_ids: ids },
  });
  e.target.querySelector(".form-msg").textContent = `已生成 ${data.count} 条提议`;
  loadBaselines();
});

// ---------- 维护窗口 ----------
async function loadMaintenance() {
  const rows = await api("/api/maintenance");
  $("#mw-table tbody").innerHTML = rows.map(m => `
    <tr class="${m.superseded_at ? "superseded" : ""}">
      <td>${m.mw_id}</td><td>${esc(m.sensor_id)}</td>
      <td><small>${esc(m.start_ts)}</small></td><td><small>${esc(m.end_ts)}</small></td>
      <td>${esc(m.reason)}</td><td><small>${esc(m.recorded_at)}</small></td>
      <td>${m.prev_mw_id ? `← #${m.prev_mw_id}` : "—"}</td>
      <td>${m.superseded_at ? "已被更正" : `<button class="correct-mw" data-id="${m.mw_id}">追溯更正</button>`}</td>
    </tr>`).join("") || `<tr><td colspan="8" class="hint">暂无维护记录</td></tr>`;
  $$(".correct-mw").forEach(btn => btn.addEventListener("click", () => {
    const st = prompt("新的开始时间，例如 2026-11-16T04:00:00Z");
    const en = prompt("新的结束时间，例如 2026-11-16T06:00:00Z");
    const reason = prompt("更正原因") || "web 更正";
    if (st && en) api(`/api/maintenance/${btn.dataset.id}/correct`, {
      method: "POST", body: { start_ts: st, end_ts: en, reason },
    }).then(loadMaintenance);
  }));
}

$("#mw-form").addEventListener("submit", async e => {
  e.preventDefault();
  const f = new FormData(e.target);
  await api("/api/maintenance", { method: "POST", body: Object.fromEntries(f) });
  e.target.querySelector(".form-msg").textContent = "已补录";
  loadMaintenance();
});

// ---------- 重算作业 ----------
async function loadJobs() {
  const rows = await api("/api/jobs");
  $("#jobs-table tbody").innerHTML = rows.map(j => `
    <tr>
      <td><small>${esc(j.job_id)}</small></td><td>${j.mode}</td>
      <td><span class="status-${j.status}">${j.status}</span></td>
      <td>${j.done_windows}/${j.total_windows}</td>
      <td>${esc(j.reason)}</td><td><small>${esc(j.created_at)}</small></td>
      <td>${["paused", "pending", "running", "failed"].includes(j.status)
        ? `<button class="run-job" data-id="${j.job_id}">恢复/继续</button>` : "—"}</td>
    </tr>`).join("") || `<tr><td colspan="7" class="hint">暂无作业</td></tr>`;
  $$(".run-job").forEach(btn => btn.addEventListener("click", async () => {
    const r = await api(`/api/jobs/${btn.dataset.id}/run`, { method: "POST", body: {} });
    alert(`作业 ${r.status}，本次处理 ${r.processed ?? 0} 窗口`);
    loadJobs();
  }));
}

$("#job-form").addEventListener("submit", async e => {
  e.preventDefault();
  const f = new FormData(e.target);
  const scope = (f.get("scope_sensors") || "").split(",").map(s => s.trim()).filter(Boolean);
  const job = await api("/api/jobs", {
    method: "POST",
    body: {
      mode: f.get("mode"), scope_sensors: scope,
      range_start: f.get("range_start") || null,
      range_end: f.get("range_end") || null,
      reason: f.get("reason"),
    },
  });
  const max_windows = f.get("max_windows") ? parseInt(f.get("max_windows"), 10) : null;
  const r = await api(`/api/jobs/${job.job_id}/run`, { method: "POST", body: { max_windows } });
  e.target.querySelector(".form-msg").textContent =
    `${job.job_id} → ${r.status}（${r.processed ?? 0} 窗口），可在列表中恢复`;
  loadJobs();
});

// ---------- 发件箱 ----------
async function loadOutbox() {
  const rows = await api("/api/outbox");
  $("#outbox-table tbody").innerHTML = rows.map(n => `
    <tr>
      <td>${n.notif_id}</td><td>#${n.alert_id}</td><td>${n.channel}</td>
      <td><span class="status-${n.status}">${n.status}</span></td>
      <td><small>${esc(JSON.stringify(n.payload))}</small></td>
      <td><small>${esc(n.sent_at ?? "—")}</small></td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">发件箱为空</td></tr>`;
}

$("#refresh-alerts").onclick = loadAlerts;
$("#refresh-baselines").onclick = loadBaselines;
$("#refresh-mw").onclick = loadMaintenance;
$("#refresh-jobs").onclick = loadJobs;
$("#refresh-outbox").onclick = loadOutbox;

loadAlerts();

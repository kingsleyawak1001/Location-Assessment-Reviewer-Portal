let mapChart;
let journeyDurationChart;
let journeyKindChart;

const apiOutput = document.getElementById("apiOutput");
const executionLog = document.getElementById("executionLog");
const mapForm = document.getElementById("mapForm");
const journeyForm = document.getElementById("journeyForm");
const apiBaseInput = document.getElementById("apiBase");
const apiBaseHint = document.getElementById("apiBaseHint");
const presetProfile = document.getElementById("presetProfile");
const presetDescription = document.getElementById("presetDescription");
const runFullDemoBtn = document.getElementById("runFullDemoBtn");
const copyLogBtn = document.getElementById("copyLogBtn");
const copyRawBtn = document.getElementById("copyRawBtn");
const liveMonitorWidget = document.getElementById("liveMonitorWidget");
const toggleMonitorBtn = document.getElementById("toggleMonitorBtn");
const liveUpdatesToggle = document.getElementById("liveUpdatesToggle");
const mapSubmitBtn = mapForm.querySelector('button[type="submit"]');
const journeySubmitBtn = journeyForm.querySelector('button[type="submit"]');
let currentRunBounds = null;
let activeRunId = "";
let isFullDemoRunning = false;
let liveUpdatesEnabled = true;
let healthCheckTimerId = null;
let lastRunRefreshAttemptMs = 0;
const runRefreshCooldownMs = 4000;
const apiBaseStorageKey = "reviewer_api_base";
const activityTimeline = document.getElementById("activityTimeline");
const kpiEls = {
  apiLatency: document.getElementById("kpiApiLatency"),
  mapCells: document.getElementById("kpiMapCells"),
  journeyVisits: document.getElementById("kpiJourneyVisits"),
  suggestionCount: document.getElementById("kpiSuggestionCount"),
};
const activityEvents = [];
const stageEls = {
  api: {
    badge: document.getElementById("stateApi"),
    detail: document.getElementById("stateApiDetail"),
  },
  run: {
    badge: document.getElementById("stateRun"),
    detail: document.getElementById("stateRunDetail"),
  },
  preset: {
    badge: document.getElementById("statePreset"),
    detail: document.getElementById("statePresetDetail"),
  },
  map: {
    badge: document.getElementById("stateMap"),
    detail: document.getElementById("stateMapDetail"),
  },
  suggestions: {
    badge: document.getElementById("stateSuggestions"),
    detail: document.getElementById("stateSuggestionsDetail"),
  },
  journey: {
    badge: document.getElementById("stateJourney"),
    detail: document.getElementById("stateJourneyDetail"),
  },
  charts: {
    badge: document.getElementById("stateCharts"),
    detail: document.getElementById("stateChartsDetail"),
  },
};

const MAP_DEFAULTS = {
  start_date: "2025-01-01",
  end_date: "2025-01-05",
  west: "-180",
  east: "180",
  south: "-90",
  north: "90",
  movement_type: "stay",
  min_visits: "1",
  limit: "20",
};

const JOURNEY_DEFAULTS = {
  device_id: "78805d50-5ebb-4772-a634-0301b479f300",
  start_ts: "2025-01-01T00:00:00Z",
  end_ts: "2025-01-05T23:59:59Z",
  include_pass_by: "true",
  limit: "200",
};

const PRESET_PROFILES = [
  {
    id: "p1",
    label: "1) Balanced Overview (All Movements)",
    description:
      "Best default for rich charts: mixed movement map view + full journey trace.",
    map: { ...MAP_DEFAULTS, movement_type: "", min_visits: "1", limit: "120" },
    journey: { ...JOURNEY_DEFAULTS, include_pass_by: "true", limit: "500" },
  },
  {
    id: "p2",
    label: "2) Stay Intelligence",
    description:
      "Focus on stable stops: stay-only map and journey for behavior-heavy analysis.",
    map: { ...MAP_DEFAULTS, movement_type: "stay", min_visits: "2", limit: "120" },
    journey: { ...JOURNEY_DEFAULTS, include_pass_by: "false", limit: "400" },
  },
  {
    id: "p3",
    label: "3) Mobility Pulse (Pass-by)",
    description:
      "Focus on movement flow: pass_by map density with journey including transitions.",
    map: { ...MAP_DEFAULTS, movement_type: "pass_by", min_visits: "1", limit: "120" },
    journey: { ...JOURNEY_DEFAULTS, include_pass_by: "true", limit: "400" },
  },
];

function appendLog(message) {
  const now = new Date().toISOString();
  executionLog.textContent += `\n[${now}] ${message}`;
  executionLog.scrollTop = executionLog.scrollHeight;
}

function appendRawPayload(label, payload) {
  appendLog(`${label} raw response:`);
  appendLog(JSON.stringify(payload, null, 2));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function addTimelineEvent(label, state = "idle", detail = "") {
  const timestamp = new Date().toISOString();
  activityEvents.unshift({
    label,
    state,
    detail,
    timestamp,
  });
  if (activityEvents.length > 20) {
    activityEvents.length = 20;
  }
  activityTimeline.innerHTML = activityEvents
    .map(
      (event) => `
      <details class="timeline-event ${event.state}">
        <summary>
          <div class="dot"></div>
          <div class="body">
            <div class="label">${escapeHtml(event.label)}</div>
            <div class="meta">${escapeHtml(event.timestamp)}</div>
          </div>
        </summary>
        <div class="timeline-detail">
          ${escapeHtml(event.detail || "No extra details.")}
        </div>
      </details>
    `
    )
    .join("");
  activityTimeline.parentElement.scrollTop = 0;
}

function setKpi(name, value) {
  const el = kpiEls[name];
  if (el) {
    el.textContent = String(value);
  }
}

function setStageState(stage, state, detail) {
  const entry = stageEls[stage];
  if (!entry) {
    return;
  }
  const stageLabels = {
    api: "API Connectivity",
    run: "Run Resolution",
    preset: "Preset Application",
    map: "Map Query",
    suggestions: "Device Suggestions",
    journey: "Journey Query",
    charts: "Chart Rendering",
  };
  const detailSummary = String(detail || "")
    .split("\n")[0]
    .replace(/^Description:\s*/, "");
  entry.badge.className = `status-badge ${state}`;
  entry.badge.textContent = state;
  entry.detail.textContent = detailSummary || "No details.";
  addTimelineEvent(stageLabels[stage] || stage, state, detail);
}

function parseForm(form) {
  return Object.fromEntries(new FormData(form).entries());
}

function setFormValues(formElement, values) {
  Object.entries(values).forEach(([key, value]) => {
    const input = formElement.elements.namedItem(key);
    if (input) {
      input.value = value;
    }
  });
}

function isLocalPageHost() {
  return ["localhost", "127.0.0.1", "::1"].includes(window.location.hostname);
}

function isLoopbackApiBase(baseUrl) {
  try {
    const parsed = new URL(baseUrl);
    return ["localhost", "127.0.0.1", "::1"].includes(parsed.hostname);
  } catch {
    return false;
  }
}

function updateApiBaseHint() {
  const base = getApiBase();
  if (!base) {
    apiBaseHint.textContent = "Set API Base URL to a reachable backend endpoint.";
    apiBaseHint.className = "muted hint-warning";
    return;
  }
  if (!isLocalPageHost() && isLoopbackApiBase(base)) {
    apiBaseHint.textContent =
      "You are on a hosted page. 127.0.0.1 points to this device. Use a public API URL or your PC LAN IP (with API listening on 0.0.0.0).";
    apiBaseHint.className = "muted hint-warning";
    return;
  }
  apiBaseHint.textContent = "API Base URL looks reachable for this environment.";
  apiBaseHint.className = "muted";
}

function initApiBaseInput() {
  const urlBase = new URLSearchParams(window.location.search).get("api_base");
  const savedBase = localStorage.getItem(apiBaseStorageKey);
  if (urlBase) {
    apiBaseInput.value = urlBase;
    localStorage.setItem(apiBaseStorageKey, urlBase);
  } else if (savedBase) {
    apiBaseInput.value = savedBase;
  } else if (!isLocalPageHost() && isLoopbackApiBase(apiBaseInput.value)) {
    apiBaseInput.value = "";
  }
  updateApiBaseHint();
}

function setElementLoading(element, isLoading, loadingLabel = "Loading...") {
  if (!element) {
    return;
  }
  if (!element.dataset.originalLabel) {
    element.dataset.originalLabel = element.textContent;
  }
  element.disabled = isLoading;
  element.classList.toggle("is-loading", isLoading);
  element.textContent = isLoading ? loadingLabel : element.dataset.originalLabel;
}

function formatEventDetail(description, requestUrl = "", source = "") {
  const parts = [`Description: ${description}`];
  if (requestUrl) {
    parts.push(`Request: ${requestUrl}`);
  }
  if (source) {
    parts.push(`Source: ${source}`);
  }
  return parts.join("\n");
}

function applyPreset(profileId) {
  const profile = PRESET_PROFILES.find((item) => item.id === profileId);
  if (!profile) {
    appendLog(`Preset not found: ${profileId}`);
    setStageState("preset", "error", "Preset id not found.");
    return;
  }
  setFormValues(mapForm, profile.map);
  setFormValues(journeyForm, profile.journey);
  if (currentRunBounds?.start_ts_utc && currentRunBounds?.end_ts_utc) {
    // Keep preset tuning while preserving the active raw run time window.
    setFormValues(journeyForm, {
      start_ts: currentRunBounds.start_ts_utc,
      end_ts: currentRunBounds.end_ts_utc,
    });
    setFormValues(mapForm, {
      start_date: currentRunBounds.start_ts_utc.slice(0, 10),
      end_date: currentRunBounds.end_ts_utc.slice(0, 10),
    });
  }
  presetDescription.textContent = profile.description;
  appendLog(`Applied preset: ${profile.label}`);
  setStageState("preset", "success", profile.label);
}

function initPresets() {
  appendLog("Loading preset profiles.");
  PRESET_PROFILES.forEach((profile) => {
    const option = document.createElement("option");
    option.value = profile.id;
    option.textContent = profile.label;
    presetProfile.appendChild(option);
  });
  presetProfile.value = PRESET_PROFILES[0].id;
  applyPreset(PRESET_PROFILES[0].id);
}

async function findSampleDevice(source = "Auto load") {
  if (!(await ensureActiveRunLoaded())) {
    apiOutput.textContent =
      "Sample device load blocked: active raw_pings run_id is missing.";
    return;
  }
  const journey = parseForm(journeyForm);
  const url = `${getApiBase()}/api/devices/suggestions?${new URLSearchParams({
    run_id: getActiveRunId(),
    start_ts: journey.start_ts,
    end_ts: journey.end_ts,
    include_pass_by: journey.include_pass_by,
    limit: "5",
  }).toString()}`;
  appendLog(`Loading sample devices: ${url}`);
  const startedAt = performance.now();
  setStageState(
    "suggestions",
    "running",
    formatEventDetail("Loading sample device suggestions.", url, source)
  );
  try {
    const response = await fetch(url);
    setKpi("apiLatency", `${Math.round(performance.now() - startedAt)} ms`);
    appendLog(`Sample device response status: ${response.status}`);
    setStageState(
      "api",
      response.ok ? "success" : "error",
      formatEventDetail(`device suggestions -> ${response.status}`, url, source)
    );
    const payload = await response.json();
    appendRawPayload("Sample device suggestions", payload);
    const devices = payload?.data?.devices ?? [];
    setKpi("suggestionCount", devices.length);
    if (!devices.length) {
      appendLog("No sample devices found for selected window.");
      setStageState(
        "suggestions",
        "warn",
        formatEventDetail("No devices in selected bounds.", url, source)
      );
      return;
    }
    const selected = devices[0];
    setFormValues(journeyForm, { device_id: selected.device_id });
    appendLog(
      `Selected device_id=${selected.device_id} (visits=${selected.visits_count}) from suggestions.`
    );
    setStageState(
      "suggestions",
      "success",
      formatEventDetail(
        `${selected.device_id} (${selected.visits_count} visits)`,
        url,
        source
      )
    );
  } catch (error) {
    appendLog(`Sample device loading failed: ${String(error)}`);
    setStageState(
      "suggestions",
      "error",
      formatEventDetail("Suggestion query failed.", url, source)
    );
    setStageState(
      "api",
      "error",
      formatEventDetail("Suggestion request failed.", url, source)
    );
  }
}

function getApiBase() {
  return apiBaseInput.value.trim().replace(/\/$/, "");
}

function getActiveRunId() {
  return activeRunId;
}

async function ensureActiveRunLoaded() {
  if (getActiveRunId()) {
    return true;
  }
  const now = Date.now();
  if (now - lastRunRefreshAttemptMs < runRefreshCooldownMs) {
    appendLog("Skipping repeated run refresh attempt (cooldown active).");
    return false;
  }
  lastRunRefreshAttemptMs = now;
  appendLog("Active run_id is empty. Attempting auto-refresh from raw_pings.csv lineage.");
  await refreshActiveRun("Auto ensureActiveRunLoaded");
  if (!getActiveRunId()) {
    appendLog("Unable to resolve active run_id. Run raw_pings pipeline and refresh.");
    return false;
  }
  return true;
}

async function refreshActiveRun(source = "Auto load") {
  const startedAt = performance.now();
  const url = `${getApiBase()}/api/runs/latest?${new URLSearchParams({
    source_contains: "raw_pings.csv",
  }).toString()}`;
  appendLog(`Loading latest raw run: ${url}`);
  setStageState(
    "run",
    "running",
    formatEventDetail("Resolving latest raw_pings run.", url, source)
  );
  try {
    const response = await fetch(url);
    setKpi("apiLatency", `${Math.round(performance.now() - startedAt)} ms`);
    appendLog(`Latest run response status: ${response.status}`);
    setStageState(
      "api",
      response.ok ? "success" : "error",
      formatEventDetail(`runs/latest -> ${response.status}`, url, source)
    );
    const payload = await response.json();
    const run = payload?.data;
    if (run?.run_id) {
      activeRunId = run.run_id;
      appendLog(`Active run_id set: ${run.run_id}`);
      const boundsUrl = `${getApiBase()}/api/runs/${encodeURIComponent(run.run_id)}/bounds`;
      appendLog(`Loading run bounds: ${boundsUrl}`);
      const boundsResp = await fetch(boundsUrl);
      appendLog(`Run bounds response status: ${boundsResp.status}`);
      const boundsPayload = await boundsResp.json();
      const bounds = boundsPayload?.data;
      if (bounds?.start_ts_utc && bounds?.end_ts_utc) {
        currentRunBounds = bounds;
        setFormValues(journeyForm, {
          start_ts: bounds.start_ts_utc,
          end_ts: bounds.end_ts_utc,
        });
        setFormValues(mapForm, {
          start_date: bounds.start_ts_utc.slice(0, 10),
          end_date: bounds.end_ts_utc.slice(0, 10),
        });
        appendLog(
          `Applied run bounds to forms: ${bounds.start_ts_utc} .. ${bounds.end_ts_utc}`
        );
        setStageState(
          "run",
          "success",
          formatEventDetail(
            `${run.run_id.slice(0, 8)}... | ${bounds.start_ts_utc} .. ${bounds.end_ts_utc}`,
            boundsUrl,
            source
          )
        );
      }
    } else {
      activeRunId = "";
      appendLog("No raw_pings run found in lineage. Run phase1 on raw_pings.csv first.");
      setStageState(
        "run",
        "warn",
        formatEventDetail("No raw_pings run found.", url, source)
      );
    }
  } catch (error) {
    activeRunId = "";
    appendLog(`Failed to load latest raw run: ${String(error)}`);
    setStageState("api", "error", formatEventDetail("Could not reach API.", url, source));
    setStageState(
      "run",
      "error",
      formatEventDetail("Run resolution failed.", url, source)
    );
  }
}

function renderMapCharts(cells) {
  const labels = cells.map((c) => c.hex_id);
  const totalPings = cells.map((c) => c.total_pings ?? 0);

  if (mapChart) {
    mapChart.destroy();
  }

  mapChart = new Chart(document.getElementById("mapBarChart"), {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Total Pings",
          data: totalPings,
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: {
          labels: { color: "#e8eaf0" },
        },
      },
      scales: {
        x: { ticks: { color: "#aab3c7" } },
        y: { ticks: { color: "#aab3c7" } },
      },
    },
  });
  setStageState("charts", "success", `Map chart rendered (${cells.length} bars).`);
  setKpi("mapCells", cells.length);
}

function renderJourneyCharts(journey) {
  const labels = journey.map((_, idx) => `Visit ${idx + 1}`);
  const durations = journey.map((j) => j.duration_seconds ?? 0);
  const byKind = journey.reduce(
    (acc, item) => {
      const key = item.visit_kind || "unknown";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    },
    {}
  );

  if (journeyDurationChart) {
    journeyDurationChart.destroy();
  }
  if (journeyKindChart) {
    journeyKindChart.destroy();
  }

  journeyDurationChart = new Chart(document.getElementById("journeyDurationChart"), {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Duration (seconds)",
          data: durations,
          tension: 0.2,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: {
          labels: { color: "#e8eaf0" },
        },
      },
      scales: {
        x: { ticks: { color: "#aab3c7" } },
        y: { ticks: { color: "#aab3c7" } },
      },
    },
  });

  journeyKindChart = new Chart(document.getElementById("journeyKindChart"), {
    type: "doughnut",
    data: {
      labels: Object.keys(byKind),
      datasets: [
        {
          data: Object.values(byKind),
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: {
          labels: { color: "#e8eaf0" },
        },
      },
    },
  });
  setStageState("charts", "success", `Journey charts rendered (${journey.length} visits).`);
  setKpi("journeyVisits", journey.length);
}

mapForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await executeMapQuery();
});

async function executeMapQuery(source = "Manual Map Query") {
  setElementLoading(mapSubmitBtn, true, "Running...");
  if (!(await ensureActiveRunLoaded())) {
    apiOutput.textContent = "Map query blocked: active raw_pings run_id is missing.";
    setElementLoading(mapSubmitBtn, false);
    return false;
  }
  setStageState("map", "running", "Executing map analytics query.");
  const raw = parseForm(mapForm);
  raw.response_format = "assessment";
  raw.run_id = getActiveRunId();
  const params = new URLSearchParams(
    Object.entries(raw).filter(([, value]) => String(value).trim() !== "")
  );
  const url = `${getApiBase()}/api/map/data?${params.toString()}`;
  appendLog(`Map query started: ${url}`);
  const startedAt = performance.now();
  try {
    const response = await fetch(url);
    setKpi("apiLatency", `${Math.round(performance.now() - startedAt)} ms`);
    appendLog(`Map query response status: ${response.status}`);
    setStageState(
      "api",
      response.ok ? "success" : "error",
      formatEventDetail(`map/data -> ${response.status}`, url, source)
    );
    const payload = await response.json();
    apiOutput.textContent = JSON.stringify(payload, null, 2);
    appendRawPayload("Map query", payload);
    const cells = Array.isArray(payload?.data?.cells)
      ? payload.data.cells
      : payload?.data?.hex_id
        ? [payload.data]
        : [];
    renderMapCharts(cells);
    appendLog(`Map query completed: cells=${cells.length}`);
    setStageState(
      "map",
      cells.length > 0 ? "success" : "warn",
      formatEventDetail(`cells=${cells.length}`, url, source)
    );
    return response.ok;
  } catch (error) {
    apiOutput.textContent = `Map query failed: ${String(error)}`;
    appendLog(`Map query failed: ${String(error)}`);
    setStageState("map", "error", formatEventDetail("Map query failed.", url, source));
    setStageState("api", "error", formatEventDetail("Map request failed.", url, source));
    return false;
  } finally {
    setElementLoading(mapSubmitBtn, false);
  }
}

journeyForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await executeJourneyQuery();
});

async function executeJourneyQuery(source = "Manual Journey Query") {
  setElementLoading(journeySubmitBtn, true, "Running...");
  if (!(await ensureActiveRunLoaded())) {
    apiOutput.textContent = "Journey query blocked: active raw_pings run_id is missing.";
    setElementLoading(journeySubmitBtn, false);
    return false;
  }
  setStageState("journey", "running", "Executing device journey query.");
  const form = parseForm(journeyForm);
  if (!form.device_id || !String(form.device_id).trim()) {
    apiOutput.textContent = "Journey query failed: device_id is required.";
    appendLog("Journey query blocked: device_id is required.");
    setElementLoading(journeySubmitBtn, false);
    return false;
  }
  const deviceId = encodeURIComponent(form.device_id || "");
  const query = new URLSearchParams({
    run_id: getActiveRunId(),
    start_ts: form.start_ts,
    end_ts: form.end_ts,
    include_pass_by: form.include_pass_by,
    limit: form.limit,
  });
  const url = `${getApiBase()}/api/devices/${deviceId}/journey?${query.toString()}`;
  appendLog(`Journey query started: ${url}`);
  const startedAt = performance.now();
  try {
    const response = await fetch(url);
    setKpi("apiLatency", `${Math.round(performance.now() - startedAt)} ms`);
    appendLog(`Journey query response status: ${response.status}`);
    setStageState(
      "api",
      response.ok ? "success" : "error",
      formatEventDetail(`journey -> ${response.status}`, url, source)
    );
    const payload = await response.json();
    apiOutput.textContent = JSON.stringify(payload, null, 2);
    appendRawPayload("Journey query", payload);
    const journey = payload?.data?.journey ?? [];
    renderJourneyCharts(journey);
    appendLog(`Journey query completed: visits=${journey.length}`);
    setStageState(
      "journey",
      journey.length > 0 ? "success" : "warn",
      formatEventDetail(`visits=${journey.length}`, url, source)
    );
    return response.ok;
  } catch (error) {
    apiOutput.textContent = `Journey query failed: ${String(error)}`;
    appendLog(`Journey query failed: ${String(error)}`);
    setStageState(
      "journey",
      "error",
      formatEventDetail("Journey query failed.", url, source)
    );
    setStageState(
      "api",
      "error",
      formatEventDetail("Journey request failed.", url, source)
    );
    return false;
  } finally {
    setElementLoading(journeySubmitBtn, false);
  }
}

presetProfile.addEventListener("change", async () => {
  presetProfile.disabled = true;
  presetProfile.classList.add("is-loading");
  presetDescription.textContent = "Applying preset and loading sample device...";
  setStageState("preset", "running", "Applying preset profile.");
  try {
    applyPreset(presetProfile.value);
    if (liveUpdatesEnabled) {
      await findSampleDevice("Preset change");
    } else {
      appendLog("Live updates are disabled. Auto suggestions skipped for preset change.");
      setStageState("suggestions", "idle", "Live updates disabled: auto suggestions paused.");
    }
  } finally {
    presetProfile.disabled = false;
    presetProfile.classList.remove("is-loading");
  }
});

copyLogBtn.addEventListener("click", async () => {
  setElementLoading(copyLogBtn, true, "Copying...");
  try {
    await navigator.clipboard.writeText(executionLog.textContent);
    appendLog("Execution log copied to clipboard.");
  } catch (error) {
    appendLog(`Failed to copy execution log: ${String(error)}`);
  } finally {
    setElementLoading(copyLogBtn, false);
  }
});

copyRawBtn.addEventListener("click", async () => {
  setElementLoading(copyRawBtn, true, "Copying...");
  try {
    await navigator.clipboard.writeText(apiOutput.textContent);
    appendLog("Raw API response copied to clipboard.");
  } catch (error) {
    appendLog(`Failed to copy raw API response: ${String(error)}`);
  } finally {
    setElementLoading(copyRawBtn, false);
  }
});

toggleMonitorBtn.addEventListener("click", () => {
  const isCollapsed = liveMonitorWidget.classList.toggle("collapsed");
  toggleMonitorBtn.textContent = isCollapsed ? "Show" : "Hide";
  appendLog(`Live Monitor ${isCollapsed ? "collapsed" : "expanded"}.`);
});

async function runHealthCheck() {
  const url = `${getApiBase()}/api/health`;
  const startedAt = performance.now();
  try {
    const response = await fetch(url);
    setStageState(
      "api",
      response.ok ? "success" : "error",
      formatEventDetail(`health -> ${response.status}`, url, "Background health check")
    );
    setKpi("apiLatency", `${Math.round(performance.now() - startedAt)} ms`);
  } catch {
    setStageState(
      "api",
      "error",
      formatEventDetail("health check failed", url, "Background health check")
    );
  }
}

function setLiveUpdates(isEnabled) {
  liveUpdatesEnabled = isEnabled;
  if (healthCheckTimerId) {
    clearInterval(healthCheckTimerId);
    healthCheckTimerId = null;
  }
  if (isEnabled) {
    appendLog("Live updates enabled: health checks and auto suggestions are active.");
    setStageState("suggestions", "idle", "Live updates enabled: auto suggestions are active.");
    runHealthCheck();
    healthCheckTimerId = setInterval(runHealthCheck, 10000);
    return;
  }
  appendLog("Live updates disabled: background health checks and auto suggestions are paused.");
  setStageState("api", "idle", "Live updates disabled: health checks paused.");
  setStageState("suggestions", "idle", "Live updates disabled: auto suggestions paused.");
}

liveUpdatesToggle.addEventListener("change", () => {
  setLiveUpdates(liveUpdatesToggle.checked);
});

apiBaseInput.addEventListener("change", () => {
  localStorage.setItem(apiBaseStorageKey, getApiBase());
  updateApiBaseHint();
});

runFullDemoBtn.addEventListener("click", async () => {
  if (isFullDemoRunning) {
    return;
  }
  isFullDemoRunning = true;
  setElementLoading(runFullDemoBtn, true, "Running...");
  presetProfile.disabled = true;
  presetProfile.classList.add("is-loading");
  appendLog("Full demo started: refresh run -> sample device -> map -> journey.");
  setStageState("preset", "running", "Full demo in progress.");

  let mapOk = false;
  let journeyOk = false;
  try {
    await refreshActiveRun("Run Full Demo");
    await findSampleDevice("Run Full Demo");
    mapOk = await executeMapQuery("Run Full Demo");
    journeyOk = await executeJourneyQuery("Run Full Demo");
    if (mapOk && journeyOk) {
      setStageState("preset", "success", "Full demo completed successfully.");
      appendLog("Full demo completed successfully.");
    } else {
      setStageState("preset", "warn", "Full demo finished with warnings.");
      appendLog("Full demo finished with warnings. Check status badges and log.");
    }
  } catch (error) {
    appendLog(`Full demo failed: ${String(error)}`);
    setStageState("preset", "error", "Full demo failed.");
  } finally {
    isFullDemoRunning = false;
    setElementLoading(runFullDemoBtn, false);
    presetProfile.disabled = false;
    presetProfile.classList.remove("is-loading");
  }
});

mermaid.initialize({ startOnLoad: true, theme: "dark" });
initApiBaseInput();
initPresets();
appendLog("API Playground loaded.");
async function bootstrapPlayground() {
  await refreshActiveRun("Page bootstrap");
  if (liveUpdatesEnabled) {
    await findSampleDevice("Page bootstrap");
  } else {
    appendLog("Live updates are disabled. Auto suggestions skipped during bootstrap.");
  }
}
bootstrapPlayground();
setLiveUpdates(true);

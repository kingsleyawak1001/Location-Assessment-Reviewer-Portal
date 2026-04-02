const DOCS = [
  {
    title: "Solution Index",
    path: "/docs/solution/README.md",
  },
  {
    title: "Full Solution Documentation",
    path: "/docs/solution/01_full_solution_documentation.md",
  },
  {
    title: "Part 1: ETL Pipeline",
    path: "/docs/solution/domains/part1_etl_pipeline.md",
  },
  {
    title: "Part 2: Database Architecture",
    path: "/docs/solution/domains/part2_database_architecture.md",
  },
  {
    title: "Part 3: API/Query Layer",
    path: "/docs/solution/domains/part3_api_query_layer.md",
  },
  {
    title: "Part 4: Production Architecture",
    path: "/docs/solution/domains/part4_production_architecture.md",
  },
  {
    title: "Appendix: Technology Decisions",
    path: "/docs/solution/appendix/technology_decisions.md",
  },
  {
    title: "Appendix: System Diagrams",
    path: "/docs/solution/appendix/system_diagrams.md",
  },
];

let mapChart;
let journeyDurationChart;
let journeyKindChart;

const navButtons = document.querySelectorAll(".nav-btn");
const panels = document.querySelectorAll(".panel");
const docList = document.getElementById("docList");
const docViewer = document.getElementById("docViewer");
const apiOutput = document.getElementById("apiOutput");

function switchPanel(panelId) {
  navButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.target === panelId));
  panels.forEach((panel) => panel.classList.toggle("active", panel.id === panelId));
  if (panelId === "docs") {
    renderDocsMenu();
  }
}

async function loadDoc(path) {
  try {
    const response = await fetch(path);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const md = await response.text();
    docViewer.innerHTML = marked.parse(md);
    mermaid.run({ querySelector: ".doc-viewer .mermaid" });
  } catch (error) {
    docViewer.innerHTML = `<p>Failed to load document: ${String(error)}</p>`;
  }
}

function renderDocsMenu() {
  if (docList.children.length > 0) {
    return;
  }
  DOCS.forEach((doc) => {
    const button = document.createElement("button");
    button.className = "doc-item";
    button.textContent = doc.title;
    button.addEventListener("click", () => loadDoc(doc.path));
    docList.appendChild(button);
  });
}

function parseForm(form) {
  return Object.fromEntries(new FormData(form).entries());
}

function getApiBase() {
  return document.getElementById("apiBase").value.trim().replace(/\/$/, "");
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
}

document.getElementById("mapForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const params = new URLSearchParams(parseForm(event.target));
  const url = `${getApiBase()}/api/map/data?${params.toString()}`;
  try {
    const response = await fetch(url);
    const payload = await response.json();
    apiOutput.textContent = JSON.stringify(payload, null, 2);
    const cells = payload?.data?.cells ?? [];
    renderMapCharts(cells);
  } catch (error) {
    apiOutput.textContent = `Map query failed: ${String(error)}`;
  }
});

document.getElementById("journeyForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = parseForm(event.target);
  const deviceId = encodeURIComponent(form.device_id || "");
  const query = new URLSearchParams({
    start_ts: form.start_ts,
    end_ts: form.end_ts,
    include_pass_by: form.include_pass_by,
    limit: form.limit,
  });
  const url = `${getApiBase()}/api/devices/${deviceId}/journey?${query.toString()}`;
  try {
    const response = await fetch(url);
    const payload = await response.json();
    apiOutput.textContent = JSON.stringify(payload, null, 2);
    const journey = payload?.data?.journey ?? [];
    renderJourneyCharts(journey);
  } catch (error) {
    apiOutput.textContent = `Journey query failed: ${String(error)}`;
  }
});

navButtons.forEach((btn) => {
  btn.addEventListener("click", () => switchPanel(btn.dataset.target));
});

mermaid.initialize({ startOnLoad: true, theme: "dark" });
switchPanel("overview");

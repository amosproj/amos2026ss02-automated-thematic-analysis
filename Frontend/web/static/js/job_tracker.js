// Background codebook-generation tracking.
//
// Tracked jobs live in localStorage so polling survives navigation. On every
// page load this module reads the active set, polls each one's JSON status
// endpoint, updates any progress bars on the page, and renders a toast in
// the top-right corner when a job reaches a terminal state. Clicking a
// success toast navigates to the right destination based on mode.
//
// Auto mode → `/codebooks/<id>/themes` (read-only browser)
// Semi mode → `/codebooks/<id>/review` (editor)
(() => {
  const STORAGE_KEY = "ata_active_jobs";
  const POLL_MS = 2000;

  function loadJobs() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); }
    catch { return []; }
  }
  function saveJobs(jobs) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(jobs));
  }
  function trackJob(entry) {
    const jobs = loadJobs();
    if (jobs.some((j) => j.id === entry.id)) return;
    jobs.push({ ...entry, started_at: Date.now() });
    saveJobs(jobs);
    renderListPlaceholders();
  }
  function untrackJob(id) {
    saveJobs(loadJobs().filter((j) => j.id !== id));
    const row = document.querySelector(`[data-job-row="${id}"]`);
    if (row) row.remove();
  }

  async function fetchStatus(jobId) {
    try {
      const r = await fetch(`/codebooks/new/jobs/${jobId}.json`,
        { headers: { "Accept": "application/json" } });
      return await r.json();
    } catch {
      return null;
    }
  }

  // ---- Toast ---------------------------------------------------------
  function showToast({ title, body, link, kind }) {
    const wrapper = document.getElementById("ata-toasts")
      || document.body.appendChild(Object.assign(document.createElement("div"),
                                                  { id: "ata-toasts" }));
    const toast = document.createElement("div");
    toast.className = `ata-toast ata-toast-${kind || "info"}`;
    if (link) toast.classList.add("ata-toast-clickable");
    toast.innerHTML = `
      <button type="button" class="btn-close" aria-label="Close"></button>
      <div class="ata-toast-title">${title}</div>
      <div class="ata-toast-body">${body}</div>`;
    toast.querySelector(".btn-close").addEventListener("click", (e) => {
      e.stopPropagation();
      toast.remove();
    });
    if (link) {
      toast.addEventListener("click", () => { window.location.href = link; });
    } else {
      // Non-success toasts auto-dismiss after 10s.
      setTimeout(() => toast.remove(), 10000);
    }
    wrapper.appendChild(toast);
  }

  // ---- Codebook-list placeholder rows --------------------------------
  function renderListPlaceholders() {
    const host = document.getElementById("ata-job-placeholders");
    if (!host) return;
    const jobs = loadJobs();
    host.innerHTML = "";
    if (!jobs.length) return;
    jobs.forEach((job) => {
      // Skip jobs already shown by the server-rendered row.
      if (document.querySelector(`[data-job-row="${job.id}"]`)) return;
      const row = document.createElement("div");
      row.className = "alert alert-info d-flex align-items-center gap-3 mb-2 py-2";
      row.setAttribute("data-job-row", job.id);
      row.innerHTML = `
        <div class="flex-grow-1">
          <div class="fw-semibold">${job.name || "Codebook"}</div>
          <div class="progress mt-1" style="height: .6rem;">
            <div class="progress-bar progress-bar-striped progress-bar-animated"
                 data-job-progress="${job.id}"
                 style="width: 2%">2%</div>
          </div>
          <div class="small text-secondary mt-1" data-job-caption="${job.id}">
            Waiting for worker
          </div>
        </div>
        <a class="btn btn-sm btn-outline-secondary" href="/codebooks/new/jobs/${job.id}?mode=${job.mode || 'auto'}">
          Watch
        </a>`;
      host.appendChild(row);
    });
  }

  function paintProgress(jobId, status) {
    const bar = document.querySelector(`[data-job-progress="${jobId}"]`);
    const caption = document.querySelector(`[data-job-caption="${jobId}"]`);
    if (!bar && !caption) return;
    const total = status.documents_total || status.analysis_units_total || status.passages_total || 0;
    const done = status.documents_done || status.analysis_units_done || status.passages_done || 0;
    const raw = status.progress_percent || (total > 0 ? Math.round(done / total * 100) : 2);
    const pct = Math.min(99, Math.max(2, raw));
    if (bar) {
      bar.style.width = pct + "%";
      bar.textContent = pct + "%";
    }
    if (caption) {
      caption.textContent = statusCaption(status);
    }
  }

  function formatCount(value) {
    return Number(value || 0).toLocaleString();
  }

  function statusCaption(status) {
    if (status.status === "queued") return "Waiting for worker";
    if (status.status === "succeeded") {
      const pieces = [];
      if (status.quotes_created !== null && status.quotes_created !== undefined) {
        pieces.push(`${formatCount(status.quotes_created)} quotes`);
      }
      if (status.codes_created !== null && status.codes_created !== undefined) {
        pieces.push(`${formatCount(status.codes_created)} codes`);
      }
      return pieces.length ? `Complete — ${pieces.join(", ")}` : "Complete";
    }
    if (status.status === "failed") return "Failed";
    if (status.status === "cancelled") return "Cancelled";

    const labels = {
      extracting_quote_codes: "Finding evidence",
      consolidating_codes: "Consolidating codes",
      synthesizing_themes: "Building themes",
      evaluating_iterations: "Evaluating fit",
      persisting_codebook: "Saving codebook",
      applying_codebook: "Applying codebook",
    };
    const label = labels[status.phase] || "Running";
    if (status.phase === "consolidating_codes" && status.analysis_units_total > 0) {
      return `${label} — ${formatCount(status.analysis_units_done)} / ${formatCount(status.analysis_units_total)}`;
    }
    if (status.phase === "applying_codebook" && status.analysis_units_total > 0) {
      return `${label} — ${formatCount(status.analysis_units_done)} / ${formatCount(status.analysis_units_total)}`;
    }
    if (status.documents_total > 0) {
      return `${label} — ${formatCount(status.documents_done)} / ${formatCount(status.documents_total)}`;
    }
    return label;
  }

  // ---- Terminal handling --------------------------------------------
  function onTerminal(job, status) {
    untrackJob(job.id);
    const cbId = status.codebook_id;
    if (status.status === "succeeded" && cbId) {
      const runQuery = status.application_run_id
        ? `?application_run_id=${encodeURIComponent(status.application_run_id)}`
        : "";
      const link = job.mode === "semi"
        ? `/codebooks/${cbId}/review`
        : (status.corpus_id
            ? `/codebooks/${status.corpus_id}/${cbId}/themes${runQuery}`
            : `/codebooks/${cbId}/themes${runQuery}`);
      const verb = job.mode === "semi" ? "ready for review" : "ready";
      showToast({
        title: "Codebook ready",
        body: `"${job.name || "Codebook"}" is ${verb}.` +
              (job.mode === "semi" ? " Opening the review editor…" : " Click to open."),
        link,
        kind: "success",
      });
      // Semi mode: the user's intent is to review, so auto-navigate after a
      // short delay so they perceive the completion before the page changes.
      // Auto mode: stay on whatever page they're on and let them click the
      // toast (or browse the codebook list) when ready.
      if (job.mode === "semi") {
        setTimeout(() => { window.location.href = link; }, 1500);
        return;
      }
    } else if (status.status === "failed") {
      showToast({
        title: "Generation failed",
        body: `"${job.name || "Codebook"}": ${status.error_message || "unknown error"}`,
        kind: "danger",
      });
    } else {
      showToast({
        title: "Generation cancelled",
        body: `"${job.name || "Codebook"}" was cancelled.`,
        kind: "warning",
      });
    }
    // If we're on the codebook list, refresh so the new codebook shows up.
    if (/^\/codebooks\/[^/]+\/?$/.test(window.location.pathname)) {
      setTimeout(() => window.location.reload(), 1200);
    }
  }

  // Poll server-rendered rows not in localStorage (started in another session).
  // Reloads the list on terminal so finished rows drop off.
  async function tickServerRows(trackedIds) {
    const onList = /^\/codebooks\/[^/]+\/?$/.test(window.location.pathname);
    if (!onList) return;
    let anyTerminal = false;
    for (const row of document.querySelectorAll("[data-job-row]")) {
      const id = row.getAttribute("data-job-row");
      if (trackedIds.has(id)) continue; // already handled above
      const status = await fetchStatus(id);
      if (!status || status.error) continue;
      paintProgress(id, status);
      if (["succeeded", "failed", "cancelled"].includes(status.status)) {
        anyTerminal = true;
      }
    }
    if (anyTerminal) setTimeout(() => window.location.reload(), 1200);
  }

  // ---- Poll loop ----------------------------------------------------
  async function tick() {
    const jobs = loadJobs();
    for (const job of jobs) {
      const status = await fetchStatus(job.id);
      if (!status || status.error) continue;
      paintProgress(job.id, status);
      if (["succeeded", "failed", "cancelled"].includes(status.status)) {
        onTerminal(job, status);
      }
    }
    await tickServerRows(new Set(jobs.map((j) => j.id)));
  }

  // ---- Pick up new-job query params from redirect -------------------
  function captureFromUrl() {
    const params = new URLSearchParams(window.location.search);
    if (!params.has("new_job")) return;
    trackJob({
      id: params.get("new_job"),
      name: params.get("name") || "Codebook",
      mode: params.get("mode") || "auto",
    });
    // Clean up the URL so a refresh doesn't re-track.
    const url = new URL(window.location.href);
    ["new_job", "name", "mode"].forEach((k) => url.searchParams.delete(k));
    window.history.replaceState({}, "", url.toString());
  }

  // ---- Public API + bootstrap --------------------------------------
  window.ATAJobTracker = { track: trackJob, untrack: untrackJob, all: loadJobs };

  document.addEventListener("DOMContentLoaded", () => {
    captureFromUrl();
    renderListPlaceholders();
    tick();
    setInterval(tick, POLL_MS);
  });
})();

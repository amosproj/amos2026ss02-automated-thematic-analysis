(function () {
    // Per-theme demographic breakdown panel (UC 3.6).
    //
    // Listens for the `theme:selected` event from codebook_themes.js, lets the
    // researcher pick demographic dimensions (selection persisted per theme via
    // localStorage), fetches the breakdown, and renders a table + horizontal bar
    // chart per dimension. All user-supplied values are written with textContent
    // / dataset, never innerHTML, so theme/group labels cannot inject markup.

    const appRoot = document.getElementById("theme-app");
    if (!appRoot) return;

    let dimensions = [];
    try {
        dimensions = JSON.parse(appRoot.dataset.demographicDimensions || "[]");
    } catch (_) {
        dimensions = [];
    }
    // No demographic data → the panel renders a disabled explanation server-side
    // and there is nothing to wire up here.
    if (!Array.isArray(dimensions) || dimensions.length === 0) return;

    const breakdownUrlTemplate = appRoot.dataset.breakdownUrlTemplate || "";
    const applicationRunId     = appRoot.dataset.applicationRunId || "";
    const codebookId           = appRoot.dataset.codebookId || "";

    const picker     = document.getElementById("breakdown-dimension-picker");
    const emptyEl    = document.getElementById("breakdown-empty");
    const loadingEl  = document.getElementById("breakdown-loading");
    const errorEl    = document.getElementById("breakdown-error");
    const resultsEl  = document.getElementById("breakdown-results");
    const themeNameEl = document.getElementById("breakdown-theme-name");
    const checkboxes = Array.from(document.querySelectorAll("[data-breakdown-dimension]"));

    if (!picker || !resultsEl || checkboxes.length === 0) return;

    let currentThemeId   = null;
    let currentThemeName = "";
    let requestToken     = 0;

    // ------------------------------------------------------------------
    // Per-theme selection persistence
    // ------------------------------------------------------------------

    function storageKey(themeId) {
        return `ata-breakdown:${codebookId}:${applicationRunId}:${themeId}`;
    }

    function loadSelection(themeId) {
        if (!themeId) return [];
        try {
            const raw = window.localStorage.getItem(storageKey(themeId));
            const parsed = raw ? JSON.parse(raw) : [];
            return Array.isArray(parsed) ? parsed.filter((d) => dimensions.includes(d)) : [];
        } catch (_) {
            return [];
        }
    }

    function saveSelection(themeId, selected) {
        if (!themeId) return;
        try {
            window.localStorage.setItem(storageKey(themeId), JSON.stringify(selected));
        } catch (_) {
            // Storage may be unavailable (private mode); selection stays in-memory.
        }
    }

    function selectedDimensions() {
        return checkboxes.filter((cb) => cb.checked).map((cb) => cb.value);
    }

    function applySelectionToCheckboxes(selected) {
        const set = new Set(selected);
        checkboxes.forEach((cb) => { cb.checked = set.has(cb.value); });
    }

    // ------------------------------------------------------------------
    // State / rendering helpers
    // ------------------------------------------------------------------

    function setState(opts) {
        const { empty = false, loading = false, error = null } = opts || {};
        emptyEl.classList.toggle("d-none", !empty);
        loadingEl.classList.toggle("d-none", !loading);
        if (error) {
            errorEl.textContent = error;
            errorEl.classList.remove("d-none");
        } else {
            errorEl.classList.add("d-none");
        }
        if (empty || loading || error) resultsEl.replaceChildren();
    }

    function breakdownUrl(themeId, dims) {
        const url = new URL(
            breakdownUrlTemplate.replace("__THEME__", themeId),
            window.location.origin
        );
        url.searchParams.set("dimensions", dims.join(","));
        if (applicationRunId) url.searchParams.set("application_run_id", applicationRunId);
        return url.toString();
    }

    function clampPct(value) {
        const num = typeof value === "number" ? value : 0;
        return Math.min(Math.max(num, 0), 100);
    }

    function formatPct(value) {
        return (typeof value === "number" ? value : 0).toFixed(1) + "%";
    }

    function smallSampleBadge() {
        const badge = document.createElement("span");
        badge.className = "breakdown-small-sample";
        badge.textContent = "small sample";
        badge.title = "Few interviews in this group; percentage may be unreliable.";
        return badge;
    }

    function renderChart(groups) {
        const chart = document.createElement("div");
        chart.className = "breakdown-chart";

        for (const group of groups) {
            const row = document.createElement("div");
            row.className = "breakdown-row";

            const label = document.createElement("div");
            label.className = "breakdown-row-label";
            label.textContent = group.group_value;
            label.title = group.group_value;
            row.appendChild(label);

            const right = document.createElement("div");
            right.className = "d-flex align-items-center";

            const track = document.createElement("div");
            track.className = "breakdown-track flex-grow-1";
            const bar = document.createElement("div");
            bar.className = "breakdown-bar";
            bar.style.width = clampPct(group.percentage) + "%";
            track.appendChild(bar);
            right.appendChild(track);

            const value = document.createElement("span");
            value.className = "breakdown-bar-value";
            value.textContent =
                `${formatPct(group.percentage)} (${group.present_count}/${group.group_total})`;
            right.appendChild(value);

            row.appendChild(right);
            chart.appendChild(row);
        }
        return chart;
    }

    function renderTable(groups) {
        const wrap = document.createElement("div");
        wrap.className = "table-responsive";

        const table = document.createElement("table");
        table.className = "table table-sm align-middle mb-0 breakdown-table";

        const thead = document.createElement("thead");
        const headRow = document.createElement("tr");
        ["Group", "Count", "Group total", "% within group"].forEach((text, idx) => {
            const th = document.createElement("th");
            th.textContent = text;
            if (idx > 0) th.className = "text-end";
            headRow.appendChild(th);
        });
        thead.appendChild(headRow);
        table.appendChild(thead);

        const tbody = document.createElement("tbody");
        for (const group of groups) {
            const tr = document.createElement("tr");

            const groupCell = document.createElement("td");
            groupCell.textContent = group.group_value;
            if (group.small_sample) groupCell.appendChild(smallSampleBadge());
            tr.appendChild(groupCell);

            const countCell = document.createElement("td");
            countCell.className = "text-end";
            countCell.textContent = String(group.present_count);
            if (group.present_count === 0) countCell.classList.add("theme-zero");
            tr.appendChild(countCell);

            const totalCell = document.createElement("td");
            totalCell.className = "text-end";
            totalCell.textContent = String(group.group_total);
            tr.appendChild(totalCell);

            const pctCell = document.createElement("td");
            pctCell.className = "text-end";
            pctCell.textContent = formatPct(group.percentage);
            tr.appendChild(pctCell);

            tbody.appendChild(tr);
        }
        table.appendChild(tbody);
        wrap.appendChild(table);
        return wrap;
    }

    function renderDimension(dim) {
        const wrap = document.createElement("div");
        wrap.className = "breakdown-dimension";

        const title = document.createElement("p");
        title.className = "breakdown-dim-title";
        title.textContent = dim.dimension;
        wrap.appendChild(title);

        const groups = dim.groups || [];
        if (groups.length === 0) {
            const none = document.createElement("p");
            none.className = "text-secondary small mb-0";
            none.textContent = "No coded interviews for this variable in the selected run.";
            wrap.appendChild(none);
            return wrap;
        }

        wrap.appendChild(renderChart(groups));
        wrap.appendChild(renderTable(groups));
        return wrap;
    }

    function renderResults(data) {
        setState({});
        resultsEl.replaceChildren();

        const dims = data.dimensions || [];
        if (dims.length === 0) {
            setState({ empty: true });
            return;
        }

        let anySmallSample = false;
        for (const dim of dims) {
            resultsEl.appendChild(renderDimension(dim));
            if ((dim.groups || []).some((g) => g.small_sample)) anySmallSample = true;
        }

        if (anySmallSample) {
            const note = document.createElement("p");
            note.className = "text-secondary small mt-2 mb-0";
            note.textContent =
                "⚠ Groups marked “small sample” have very few interviews; " +
                "read their percentages with caution.";
            resultsEl.appendChild(note);
        }
    }

    // ------------------------------------------------------------------
    // Fetch
    // ------------------------------------------------------------------

    async function refresh() {
        const dims = selectedDimensions();
        if (currentThemeId) saveSelection(currentThemeId, dims);

        if (!currentThemeId || dims.length === 0) {
            setState({ empty: true });
            return;
        }

        const token = ++requestToken;
        setState({ loading: true });

        try {
            const response = await fetch(breakdownUrl(currentThemeId, dims));
            const data = await response.json();
            if (token !== requestToken) return; // a newer request superseded this one
            if (!response.ok || data.error) {
                throw new Error(data.error || "HTTP " + response.status);
            }
            renderResults(data);
        } catch (err) {
            if (token !== requestToken) return;
            setState({ error: "Could not load breakdown: " + err.message });
        }
    }

    // ------------------------------------------------------------------
    // Wiring
    // ------------------------------------------------------------------

    function onThemeChange(themeId, themeName) {
        currentThemeId = themeId || null;
        currentThemeName = themeName || "";
        if (themeNameEl) {
            themeNameEl.textContent = currentThemeName || "the selected theme";
        }
        applySelectionToCheckboxes(loadSelection(currentThemeId));
        refresh();
    }

    checkboxes.forEach((cb) => cb.addEventListener("change", refresh));

    document.addEventListener("theme:selected", (event) => {
        const detail = event.detail || {};
        onThemeChange(detail.themeId, detail.themeName);
    });

    // Cover the boot race: codebook_themes.js auto-selects the top theme during
    // its own IIFE, which may run before this listener is attached.
    const initial = window.__ataCurrentTheme;
    if (initial && initial.themeId) {
        onThemeChange(initial.themeId, initial.themeName);
    } else {
        setState({ empty: true });
    }
})();

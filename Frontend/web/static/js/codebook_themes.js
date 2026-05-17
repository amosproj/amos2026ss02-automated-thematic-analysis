(function () {
    const appRoot = document.getElementById("theme-app");
    if (!appRoot) return;

    // Data is embedded server-side as JSON — no extra HTTP requests needed.
    const frequencies = JSON.parse(appRoot.dataset.frequencies || "[]");
    const tree        = JSON.parse(appRoot.dataset.tree        || "[]");

    const totalThemesValue  = document.getElementById("total-themes-value");
    const rootThemesValue   = document.getElementById("root-themes-value");
    const subThemesValue    = document.getElementById("sub-themes-value");
    const themesTableBody   = document.getElementById("themes-table-body");
    const themeDetailsEmpty   = document.getElementById("theme-details-empty");
    const themeDetailsContent = document.getElementById("theme-details-content");
    const themeDetailsName    = document.getElementById("theme-details-name");
    const themeDetailsId      = document.getElementById("theme-details-id");
    const themeDetailsOccur   = document.getElementById("theme-details-occurrences");
    const themeDetailsCoverage = document.getElementById("theme-details-coverage");

    // Guard: table body is absent when an error or empty-state is rendered.
    if (!themesTableBody) return;

    let selectedThemeId      = null;
    let currentThemeInfoById = {};

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    function formatCoverage(value) {
        return typeof value === "number" ? `${value.toFixed(2)}%` : "0.00%";
    }

    function coverageBarClass(pct) {
        if (pct <= 33) return "theme-progress-bar--low";
        if (pct <= 66) return "theme-progress-bar--mid";
        return "theme-progress-bar--high";
    }

    function sortByFrequency(themes) {
        return [...themes].sort((a, b) => {
            if (b.occurrence_count !== a.occurrence_count) {
                return b.occurrence_count - a.occurrence_count;
            }
            return a.theme_name.localeCompare(b.theme_name);
        });
    }

    function flattenTreeNodes(nodes, out) {
        for (const node of nodes) {
            out.push(node);
            flattenTreeNodes(node.children ?? [], out);
        }
    }

    // Merge frequency list + tree into one lookup map keyed by theme id.
    function buildThemeInfoMap(freqList, treeNodes) {
        const map = {};
        for (const f of freqList) {
            map[f.theme_id] = { ...f };
        }
        const flat = [];
        flattenTreeNodes(treeNodes, flat);
        for (const node of flat) {
            const t   = node.theme;
            const cur = map[t.id] ?? {};
            map[t.id] = {
                theme_id:                      t.id,
                theme_name:                    cur.theme_name ?? t.label,
                occurrence_count:              cur.occurrence_count ?? 0,
                interview_coverage_percentage: cur.interview_coverage_percentage ?? 0,
            };
        }
        return map;
    }

    // ------------------------------------------------------------------
    // Metric cards
    // ------------------------------------------------------------------

    function updateMetricCards() {
        const total     = Object.keys(currentThemeInfoById).length;
        const rootCount = tree.length;
        const subCount  = total - rootCount;

        if (totalThemesValue) totalThemesValue.textContent = String(total);
        if (rootThemesValue)  rootThemesValue.textContent  = String(rootCount);
        if (subThemesValue)   subThemesValue.textContent   = String(subCount < 0 ? 0 : subCount);
    }

    // ------------------------------------------------------------------
    // Detail panel
    // ------------------------------------------------------------------

    function clearThemeDetails() {
        selectedThemeId = null;
        themeDetailsEmpty.classList.remove("d-none");
        themeDetailsContent.classList.add("d-none");
        themeDetailsName.textContent     = "";
        themeDetailsId.textContent       = "";
        themeDetailsOccur.textContent    = "";
        themeDetailsCoverage.textContent = "";
    }

    // Sync the visual selection highlight across both the table and tree.
    function highlightSelectedRow() {
        themesTableBody.querySelectorAll("tr").forEach((row) => {
            row.classList.toggle("theme-row-selected", row.dataset.themeId === selectedThemeId);
        });
        document.querySelectorAll("#theme-tree [data-theme-id]").forEach((el) => {
            el.classList.toggle("selected", el.dataset.themeId === selectedThemeId);
        });
    }

    function showThemeDetails(themeId) {
        const info = currentThemeInfoById[themeId];
        if (!info) { clearThemeDetails(); return; }

        selectedThemeId = themeId;
        themeDetailsEmpty.classList.add("d-none");
        themeDetailsContent.classList.remove("d-none");
        themeDetailsName.textContent     = info.theme_name ?? "-";
        themeDetailsId.textContent       = info.theme_id   ?? "-";
        themeDetailsOccur.textContent    = String(info.occurrence_count ?? 0);
        themeDetailsCoverage.textContent = formatCoverage(info.interview_coverage_percentage ?? 0);
        highlightSelectedRow();
    }

    // ------------------------------------------------------------------
    // Frequency table
    // ------------------------------------------------------------------

    function renderThemeTable(themes) {
        themesTableBody.innerHTML = "";
        const sorted = sortByFrequency(themes);

        for (const theme of sorted) {
            const row = document.createElement("tr");
            row.dataset.themeId = theme.theme_id;
            row.classList.add("theme-row-selectable");
            row.addEventListener("click", () => showThemeDetails(theme.theme_id));

            // Name
            const nameCell = document.createElement("td");
            nameCell.textContent = theme.theme_name;

            // Occurrence count
            const countCell = document.createElement("td");
            countCell.className   = "text-end";
            countCell.textContent = String(theme.occurrence_count);
            if (theme.occurrence_count === 0) countCell.classList.add("theme-zero");

            // Coverage with mini progress bar
            const pct      = theme.interview_coverage_percentage || 0;
            const barWidth = Math.min(Math.max(pct, 0), 100);
            const coverageCell = document.createElement("td");
            coverageCell.style.paddingLeft = "3rem";
            coverageCell.innerHTML = `
                <div class="d-flex align-items-center gap-2">
                    <div class="theme-progress">
                        <div class="theme-progress-bar ${coverageBarClass(pct)}" style="width:${barWidth}%"></div>
                    </div>
                    <span class="coverage-label${pct === 0 ? " theme-zero" : ""}">${formatCoverage(pct)}</span>
                </div>`;

            row.append(nameCell, countCell, coverageCell);
            themesTableBody.appendChild(row);
        }

        highlightSelectedRow();
    }

    // ------------------------------------------------------------------
    // Tree with connector lines (file-explorer style, pure vanilla JS)
    // ------------------------------------------------------------------

    function renderTree(treeData) {
        const container = document.getElementById("theme-tree");
        if (!container) return;

        let html = '<ul class="tree-root" role="tree">';
        for (const rootNode of treeData) {
            const rootId    = rootNode.theme.id;
            const rootLabel = rootNode.theme.label;

            const hasChildren = rootNode.children && rootNode.children.length > 0;
            const toggleBtn   = hasChildren
                ? `<button class="tree-toggle" aria-expanded="false" aria-label="Toggle ${rootLabel}" tabindex="-1"></button>`
                : `<span class="tree-toggle-gap"></span>`;

            html += `<li class="tree-group" role="none">`;
            html += `<div class="tree-root-row" data-theme-id="${rootId}" role="treeitem" tabindex="0">${toggleBtn}${rootLabel}</div>`;

            if (hasChildren) {
                html += '<ul class="tree-children" role="group" style="display:none">';
                for (const child of rootNode.children) {
                    html += `<li class="tree-child-item" role="none">`;
                    html += `<div class="tree-child-row" data-theme-id="${child.theme.id}" role="treeitem" tabindex="0">${child.theme.label}</div>`;
                    html += `</li>`;
                }
                html += '</ul>';
            }

            html += '</li>';
        }
        html += '</ul>';

        container.innerHTML = html;

        // Root row: clicking anywhere (including the arrow) selects the theme AND toggles children.
        container.querySelectorAll(".tree-root-row").forEach((row) => {
            row.addEventListener("click", () => {
                showThemeDetails(row.dataset.themeId);
                const toggleBtn = row.querySelector(".tree-toggle");
                if (!toggleBtn) return;
                const group    = row.closest(".tree-group");
                const children = group.querySelector(".tree-children");
                const expanded = toggleBtn.getAttribute("aria-expanded") === "true";
                toggleBtn.setAttribute("aria-expanded", String(!expanded));
                if (children) children.style.display = expanded ? "none" : "";
            });
        });

        // Child row: clicking selects the theme only (no collapse behaviour).
        container.querySelectorAll(".tree-child-row").forEach((el) => {
            el.addEventListener("click", () => showThemeDetails(el.dataset.themeId));
        });
    }

    // ------------------------------------------------------------------
    // Boot
    // ------------------------------------------------------------------

    currentThemeInfoById = buildThemeInfoMap(frequencies, tree);
    clearThemeDetails();
    updateMetricCards();
    renderThemeTable(frequencies);
    renderTree(tree);

    // Auto-select the top theme by frequency on load.
    const top = sortByFrequency(frequencies)[0];
    if (top) showThemeDetails(top.theme_id);
})();

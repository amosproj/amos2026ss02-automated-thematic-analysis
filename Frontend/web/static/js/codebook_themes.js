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
    // Tree with connector lines (file-explorer style, pure vanilla JS).
    //
    // Built with the DOM API (createElement + textContent + setAttribute)
    // — never innerHTML on user data — so a malicious theme label like
    // `<img src=x onerror=alert(1)>` is rendered as literal text and cannot
    // execute as JS.
    //
    // The recursive `buildNodeElement` handles arbitrary tree depth:
    // any non-leaf node gets a toggle button and click-to-expand, regardless
    // of how deeply nested it sits in the hierarchy.
    // ------------------------------------------------------------------

    function renderTree(treeData) {
        const container = document.getElementById("theme-tree");
        if (!container) return;

        container.replaceChildren();
        const rootUl = document.createElement("ul");
        rootUl.className = "tree-root";
        rootUl.setAttribute("role", "tree");

        for (const rootNode of treeData) {
            rootUl.appendChild(buildNodeElement(rootNode, /*isRoot=*/ true));
        }
        container.appendChild(rootUl);
    }

    function buildNodeElement(node, isRoot) {
        const theme       = node.theme;
        const children    = node.children ?? [];
        const hasChildren = children.length > 0;

        const li = document.createElement("li");
        li.className = isRoot ? "tree-group" : "tree-child-item";
        li.setAttribute("role", "none");

        const row = document.createElement("div");
        row.className = isRoot ? "tree-root-row" : "tree-child-row";
        row.setAttribute("role", "treeitem");
        row.setAttribute("tabindex", "0");
        row.dataset.themeId = theme.id;

        if (hasChildren) {
            const toggle = document.createElement("button");
            toggle.className = "tree-toggle";
            toggle.setAttribute("aria-expanded", "false");
            toggle.setAttribute("aria-label", "Toggle " + theme.label);
            toggle.setAttribute("tabindex", "-1");
            row.appendChild(toggle);
        } else if (isRoot) {
            // Keep root rows aligned when they have no children.
            const gap = document.createElement("span");
            gap.className = "tree-toggle-gap";
            row.appendChild(gap);
        }

        // Label rendered via textContent — auto-escapes any HTML in the label.
        row.appendChild(document.createTextNode(theme.label));
        li.appendChild(row);

        let childrenUl = null;
        if (hasChildren) {
            childrenUl = document.createElement("ul");
            childrenUl.className = "tree-children";
            childrenUl.setAttribute("role", "group");
            childrenUl.style.display = "none";
            for (const child of children) {
                childrenUl.appendChild(buildNodeElement(child, /*isRoot=*/ false));
            }
            li.appendChild(childrenUl);
        }

        // Click handler: every row selects; rows with children also toggle.
        // Uses the closure-captured `li` / `childrenUl` so it works at any depth
        // (the old code's `row.closest(".tree-group")` only worked at root level).
        row.addEventListener("click", () => {
            showThemeDetails(row.dataset.themeId);
            if (!hasChildren) return;
            const toggle  = row.querySelector(".tree-toggle");
            const expanded = toggle.getAttribute("aria-expanded") === "true";
            toggle.setAttribute("aria-expanded", String(!expanded));
            childrenUl.style.display = expanded ? "none" : "";
        });

        return li;
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

(function () {
    const appRoot = document.getElementById("theme-app");
    if (!appRoot) return;

    // Data is embedded server-side as JSON — no extra HTTP requests needed.
    const frequencies       = JSON.parse(appRoot.dataset.frequencies || "[]");
    const tree              = JSON.parse(appRoot.dataset.tree        || "[]");
    const codes             = JSON.parse(appRoot.dataset.codes       || "[]");
    const quotesUrlTemplate = appRoot.dataset.quotesUrlTemplate || "";
    const readUrlTemplate   = appRoot.dataset.readUrlTemplate   || "";

    // Topics the researcher asked the AI to focus on.
    const researcherTopics = (() => {
        try {
            const raw = JSON.parse(appRoot.dataset.researcherTopics || '""');
            return String(raw)
                .split(",")
                .map(t => t.trim().toLowerCase())
                .filter(Boolean);
        } catch (_) {
            return [];
        }
    })();

    // Returns the matched topic for a label, or null. Space-padded so "work" won't match "network" but "isolation" matches "Social Isolation".
    function matchedTopicFor(label) {
        if (!researcherTopics.length || !label) return null;
        const cleaned = label.replace(/^\[CODE\]\s*/i, "");
        const normalized = " " + cleaned.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim() + " ";
        for (const topic of researcherTopics) {
            const phrase = topic.replace(/[^a-z0-9]+/g, " ").trim();
            if (phrase && normalized.includes(" " + phrase + " ")) return topic;
            const words = topic.split(/[^a-z0-9]+/).filter(w => w.length >= 4);
            if (words.some(w => normalized.includes(" " + w + " "))) return topic;
        }
        return null;
    }

    function makeTopicBadge(matchedTopic) {
        const badge = document.createElement("span");
        badge.className = "theme-topic-badge";
        badge.textContent = "Requested topic";
        badge.title = "Matches your requested topic: " + matchedTopic;
        return badge;
    }

    function _flattenTree(nodes, out) {
        for (const node of nodes) {
            out.push(node);
            _flattenTree(node.children ?? [], out);
        }
    }

    const flatTree = [];
    _flattenTree(tree, flatTree);
    const existingNodeIds = new Set(flatTree.map(n => n.theme.id));

    // Integrate flat Codes as top-level Tree Nodes
    for (const c of codes) {
        if (!existingNodeIds.has(c.id)) {
            tree.push({
                theme: {
                    id: c.id,
                    label: `[CODE] ${c.name}`,
                    description: c.description,
                    type: "CODE"
                },
                children: []
            });
        } else {
            // It is nested in the tree. We modify the label to include [CODE]
            const node = flatTree.find(n => n.theme.id === c.id);
            if (node) {
                node.theme.label = `[CODE] ${c.name}`;
                node.theme.type = "CODE";
            }
        }
    }

    const totalThemesValue  = document.getElementById("total-themes-value");
    const rootThemesValue   = document.getElementById("root-themes-value");
    const subThemesValue    = document.getElementById("sub-themes-value");
    const codesValue        = document.getElementById("codes-value");
    const themesTableBody   = document.getElementById("themes-table-body");
    const themeDetailsEmpty       = document.getElementById("theme-details-empty");
    const themeDetailsContent     = document.getElementById("theme-details-content");
    const themeDetailsName        = document.getElementById("theme-details-name");
    const themeDetailsDescription = document.getElementById("theme-details-description");
    const themeDetailsId          = document.getElementById("theme-details-id");
    const themeDetailsOccur       = document.getElementById("theme-details-occurrences");
    const themeDetailsCoverage    = document.getElementById("theme-details-coverage");

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
        _flattenTree(nodes, out);
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
                description:                   t.description ?? null,
            };
        }
        return map;
    }

    // ------------------------------------------------------------------
    // Metric cards
    // ------------------------------------------------------------------

    function updateMetricCards() {
        const flat = [];
        flattenTreeNodes(tree, flat);
        
        let total = 0;
        flat.forEach(n => {
            if (n.theme.type !== 'CODE' && n.theme.node_type !== 'CODE') {
                total++;
            }
        });
        
        let rootCount = 0;
        tree.forEach(n => {
            if (n.theme.type !== 'CODE' && n.theme.node_type !== 'CODE') {
                rootCount++;
            }
        });

        const subCount = total - rootCount;

        if (totalThemesValue) totalThemesValue.textContent = String(total);
        if (rootThemesValue)  rootThemesValue.textContent  = String(rootCount);
        if (subThemesValue)   subThemesValue.textContent   = String(subCount < 0 ? 0 : subCount);
        if (codesValue)       codesValue.textContent       = String(codes.length);
    }

    // ------------------------------------------------------------------
    // Detail panel
    // ------------------------------------------------------------------

    function clearThemeDetails() {
        selectedThemeId = null;
        themeDetailsEmpty.classList.remove("d-none");
        themeDetailsContent.classList.add("d-none");
        themeDetailsName.textContent        = "";
        themeDetailsDescription.textContent = "";
        themeDetailsId.textContent          = "";
        themeDetailsOccur.textContent       = "";
        themeDetailsCoverage.textContent    = "";
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
        const matchedDetailTopic = matchedTopicFor(info.theme_name);
        if (matchedDetailTopic) {
            themeDetailsName.appendChild(makeTopicBadge(matchedDetailTopic));
        }
        themeDetailsDescription.textContent = info.description ?? "";
        themeDetailsDescription.classList.toggle("d-none", !info.description);
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

            const nameCell = document.createElement("td");
            nameCell.appendChild(document.createTextNode(theme.theme_name));
            const matchedTopic = matchedTopicFor(theme.theme_name);
            if (matchedTopic) {
                row.classList.add("theme-row-matched");
                nameCell.appendChild(makeTopicBadge(matchedTopic));
            }

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
        const matchedTreeTopic = matchedTopicFor(theme.label);
        if (matchedTreeTopic) {
            const star = document.createElement("span");
            star.className = "theme-topic-star";
            star.textContent = "★";
            star.title = "Matches your requested topic: " + matchedTreeTopic;
            row.appendChild(star);
        }
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

    // ------------------------------------------------------------------
    // Quotes modal
    // ------------------------------------------------------------------

    const quotesModal      = document.getElementById("quotes-modal");
    const quotesLoading    = document.getElementById("quotes-loading");
    const quotesError      = document.getElementById("quotes-error");
    const quotesList       = document.getElementById("quotes-list");
    const quotesEmpty      = document.getElementById("quotes-empty");
    const quotesTotal      = document.getElementById("quotes-modal-total");
    const quotesLabel      = document.getElementById("quotes-modal-label");
    const quotesPagination = document.getElementById("quotes-pagination");
    const quotesPageInfo   = document.getElementById("quotes-page-info");
    const quotesPrev       = document.getElementById("quotes-prev");
    const quotesNext       = document.getElementById("quotes-next");
    const viewQuotesBtn    = document.getElementById("theme-details-view-quotes");

    if (!quotesModal || !viewQuotesBtn) return;

    const bsModal = new bootstrap.Modal(quotesModal);

    let activeQuoteThemeId = null;
    let activeQuotePage    = 1;
    const PAGE_SIZE        = 20;

    function quotesUrl(themeId, page) {
        return quotesUrlTemplate.replace("__THEME__", themeId)
            + "?page=" + page + "&page_size=" + PAGE_SIZE;
    }

    function interviewUrl(documentId) {
        return readUrlTemplate.replace("__DOC__", documentId);
    }

    function setQuotesLoading() {
        quotesLoading.classList.remove("d-none");
        quotesError.classList.add("d-none");
        quotesList.classList.add("d-none");
        quotesEmpty.classList.add("d-none");
        quotesPagination.classList.add("d-none");
    }

    function renderQuotes(data, themeName) {
        quotesLoading.classList.add("d-none");

        const items = data.items || [];
        const meta  = data.meta  || {};
        const total = meta.total  ?? 0;
        const page  = meta.page   ?? 1;
        const pages = meta.pages  ?? 0;

        quotesLabel.textContent = "Quotes for: " + (themeName || "Theme");
        quotesTotal.textContent = total === 1 ? "1 quote across corpus" : total + " quotes across corpus";

        if (items.length === 0) {
            quotesEmpty.classList.remove("d-none");
            quotesPagination.classList.add("d-none");
            return;
        }

        quotesList.replaceChildren();

        for (const item of items) {
            const card = document.createElement("div");
            card.className = "border rounded-2 p-3 mb-2";

            // Quote text — textContent prevents XSS on any user-supplied content.
            const quoteEl = document.createElement("blockquote");
            quoteEl.className = "mb-2 fst-italic text-body";
            quoteEl.textContent = "“" + item.quote + "”";
            card.appendChild(quoteEl);

            const meta = document.createElement("div");
            meta.className = "d-flex align-items-center justify-content-between flex-wrap gap-2";

            const interviewee = document.createElement("span");
            interviewee.className = "text-secondary small";
            interviewee.textContent = "Interviewee: " + (item.interviewee_id || "Unknown");
            meta.appendChild(interviewee);

            const link = document.createElement("a");
            link.href      = interviewUrl(item.document_id);
            link.className = "btn btn-sm btn-outline-secondary";
            link.textContent = "View Interview";
            meta.appendChild(link);

            card.appendChild(meta);
            quotesList.appendChild(card);
        }

        quotesList.classList.remove("d-none");

        // Pagination
        activeQuotePage = page;
        quotesPageInfo.textContent = "Page " + page + " of " + pages;
        quotesPrev.disabled = page <= 1;
        quotesNext.disabled = page >= pages;
        quotesPagination.classList.toggle("d-none", pages <= 1);
    }

    async function loadQuotes(themeId, page) {
        activeQuoteThemeId = themeId;
        activeQuotePage    = page;
        setQuotesLoading();

        const themeName = (currentThemeInfoById[themeId] || {}).theme_name || "";

        try {
            const response = await fetch(quotesUrl(themeId, page));
            const data = await response.json();
            if (!response.ok || data.error) throw new Error(data.error || "HTTP " + response.status);
            renderQuotes(data, themeName);
        } catch (err) {
            quotesLoading.classList.add("d-none");
            quotesError.textContent = "Could not load quotes: " + err.message;
            quotesError.classList.remove("d-none");
        }
    }

    viewQuotesBtn.addEventListener("click", () => {
        if (!selectedThemeId) return;
        bsModal.show();
        loadQuotes(selectedThemeId, 1);
    });

    quotesPrev.addEventListener("click", () => {
        if (activeQuoteThemeId && activeQuotePage > 1) {
            loadQuotes(activeQuoteThemeId, activeQuotePage - 1);
        }
    });

    quotesNext.addEventListener("click", () => {
        if (activeQuoteThemeId) {
            loadQuotes(activeQuoteThemeId, activeQuotePage + 1);
        }
    });
})();

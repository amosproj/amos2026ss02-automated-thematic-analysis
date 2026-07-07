(function () {
    const appRoot = document.getElementById("theme-app");
    if (!appRoot) return;

    // Data is embedded server-side as JSON — no extra HTTP requests needed.
    const frequencies       = JSON.parse(appRoot.dataset.frequencies || "[]");
    const tree              = JSON.parse(appRoot.dataset.tree        || "[]");

    // Surface the parent roll-up as the canonical occurrence/coverage so every
    // consumer displays it. Keep each node's own numbers under `own_*`.
    for (const f of frequencies) {
        f.own_occurrence_count = f.occurrence_count;
        f.own_interview_coverage_percentage = f.interview_coverage_percentage;
        if (typeof f.parent_occurrence_count === "number") {
            f.occurrence_count = f.parent_occurrence_count;
        }
        if (typeof f.parent_interview_coverage_percentage === "number") {
            f.interview_coverage_percentage = f.parent_interview_coverage_percentage;
        }
    }
    const codes             = JSON.parse(appRoot.dataset.codes       || "[]");
    const quotesUrlTemplate = appRoot.dataset.quotesUrlTemplate || "";
    const readUrlTemplate   = appRoot.dataset.readUrlTemplate   || "";
    const applicationRunId  = appRoot.dataset.applicationRunId || "";

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
                    label: c.name,
                    description: c.description,
                    type: "CODE"
                },
                children: []
            });
        } else {
            // It is nested in the tree. Tag it as a code without altering the label.
            const node = flatTree.find(n => n.theme.id === c.id);
            if (node) {
                node.theme.label = c.name;
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

    // Shared ordering: most frequent first, ties alphabetical. Used for the
    // frequency list (boot auto-select) and for every sibling level of the
    // merged theme table.
    function byFrequencyThenName(a, b) {
        if (b.occurrence_count !== a.occurrence_count) {
            return b.occurrence_count - a.occurrence_count;
        }
        return a.theme_name.localeCompare(b.theme_name);
    }

    function sortByFrequency(themes) {
        return [...themes].sort(byFrequencyThenName);
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

    // Broadcast theme selection so sibling modules (e.g. the demographic
    // breakdown panel) can react. A global mirror covers the boot race where a
    // listener attaches after the initial auto-selection has already fired.
    function announceSelectedTheme(detail) {
        window.__ataCurrentTheme = detail.themeId ? detail : null;
        document.dispatchEvent(new CustomEvent("theme:selected", { detail }));
    }

    function clearThemeDetails() {
        selectedThemeId = null;
        themeDetailsEmpty.classList.remove("d-none");
        themeDetailsContent.classList.add("d-none");
        themeDetailsName.textContent        = "";
        themeDetailsDescription.textContent = "";
        themeDetailsId.textContent          = "";
        themeDetailsOccur.textContent       = "";
        themeDetailsCoverage.textContent    = "";
        announceSelectedTheme({ themeId: null, themeName: "" });
    }

    // Sync the visual selection highlight across the merged theme table.
    function highlightSelectedRow() {
        themesTableBody.querySelectorAll("tr").forEach((row) => {
            row.classList.toggle("theme-row-selected", row.dataset.themeId === selectedThemeId);
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
        announceSelectedTheme({ themeId, themeName: info.theme_name ?? "" });
    }

    // ------------------------------------------------------------------
    // Merged theme table: frequency + hierarchy in one box (issue #226).
    //
    // Each tree node becomes one <tr>; the Theme cell carries the hierarchy
    // (depth indent + expand/collapse chevron) while the Occurrences and
    // Coverage columns keep the frequency view. Sibling rows are ordered by
    // frequency at every depth, so expanding a parent lists its children
    // most-frequent first.
    //
    // Built with the DOM API (createElement + textContent + setAttribute)
    // — never innerHTML on user data — so a malicious theme label like
    // `<img src=x onerror=alert(1)>` is rendered as literal text and cannot
    // execute as JS.
    // ------------------------------------------------------------------

    // child theme id -> parent theme id; lets revealTheme expand ancestors.
    const parentIdByThemeId = {};

    function isCodeNode(theme) {
        return theme.type === "CODE" || theme.node_type === "CODE";
    }

    function nodeInfo(node) {
        return currentThemeInfoById[node.theme.id] ?? {
            theme_id:                      node.theme.id,
            theme_name:                    node.theme.label,
            occurrence_count:              0,
            interview_coverage_percentage: 0,
        };
    }

    function renderThemeTreeTable(treeData) {
        themesTableBody.innerHTML = "";

        // `ancestorGuides` carries one flag per ancestor indent column:
        // true = that ancestor has further siblings below, so the column
        // draws a vertical pass-through line (classic file-explorer capping —
        // lines stop at each branch's last child instead of running on).
        (function renderLevel(nodes, depth, parentId, ancestorGuides) {
            const sorted = [...nodes].sort((a, b) => byFrequencyThenName(nodeInfo(a), nodeInfo(b)));
            sorted.forEach((node, index) => {
                const theme       = node.theme;
                const children    = node.children ?? [];
                const info        = nodeInfo(node);
                const code        = isCodeNode(theme);
                const isLast      = index === sorted.length - 1;
                if (parentId) parentIdByThemeId[theme.id] = parentId;

                const row = document.createElement("tr");
                row.dataset.themeId = theme.id;
                row.classList.add("theme-row-selectable");
                if (parentId) {
                    row.dataset.parentId = parentId;
                    row.classList.add("d-none"); // children start collapsed
                }

                // Theme cell: connector guides + chevron + label (+ badges).
                const nameCell = document.createElement("td");
                const nameWrap = document.createElement("div");
                nameWrap.className = "theme-name-flex";
                nameCell.appendChild(nameWrap);

                for (const hasLine of ancestorGuides) {
                    const guide = document.createElement("span");
                    guide.className = "tree-indent" + (hasLine ? " tree-indent--line" : "");
                    nameWrap.appendChild(guide);
                }
                if (depth > 0) {
                    // ├ for children with siblings below, └ for the last one.
                    const connector = document.createElement("span");
                    connector.className = "tree-indent " + (isLast ? "tree-indent--elbow" : "tree-indent--tee");
                    nameWrap.appendChild(connector);
                }
                if (children.length > 0) {
                    const toggle = document.createElement("button");
                    toggle.className = "tree-toggle";
                    toggle.setAttribute("aria-expanded", "false");
                    toggle.setAttribute("aria-label", "Toggle " + info.theme_name);
                    toggle.setAttribute("tabindex", "-1");
                    nameWrap.appendChild(toggle);
                } else {
                    const gap = document.createElement("span");
                    gap.className = "tree-toggle-gap";
                    nameWrap.appendChild(gap);
                }
                nameWrap.appendChild(document.createTextNode(info.theme_name));
                const matchedTopic = matchedTopicFor(info.theme_name);
                if (matchedTopic) {
                    row.classList.add("theme-row-matched");
                    nameWrap.appendChild(makeTopicBadge(matchedTopic));
                }
                if (code) {
                    const chip = document.createElement("span");
                    chip.className = "badge text-bg-light border ms-1";
                    chip.textContent = "code";
                    nameWrap.appendChild(chip);
                }

                // Occurrences — codes have no frequency data, so show a dash
                // rather than a misleading 0.
                const countCell = document.createElement("td");
                countCell.className = "text-end";
                if (code) {
                    countCell.textContent = "—";
                    countCell.classList.add("theme-zero");
                } else {
                    countCell.textContent = String(info.occurrence_count);
                    if (info.occurrence_count === 0) countCell.classList.add("theme-zero");
                }

                // Coverage with mini progress bar (values are numeric, so the
                // interpolated markup below contains no user-supplied strings).
                const coverageCell = document.createElement("td");
                coverageCell.style.paddingLeft = "3rem";
                if (code) {
                    coverageCell.innerHTML = '<span class="coverage-label theme-zero">—</span>';
                } else {
                    const pct      = info.interview_coverage_percentage || 0;
                    const barWidth = Math.min(Math.max(pct, 0), 100);
                    coverageCell.innerHTML = `
                        <div class="d-flex align-items-center gap-2">
                            <div class="theme-progress">
                                <div class="theme-progress-bar ${coverageBarClass(pct)}" style="width:${barWidth}%"></div>
                            </div>
                            <span class="coverage-label${pct === 0 ? " theme-zero" : ""}">${formatCoverage(pct)}</span>
                        </div>`;
                }

                row.append(nameCell, countCell, coverageCell);

                // Every row selects; rows with children also toggle their subtree.
                row.addEventListener("click", () => {
                    showThemeDetails(theme.id);
                    if (children.length > 0) toggleChildren(theme.id);
                });

                themesTableBody.appendChild(row);
                // Children inherit this level's columns plus one for this node:
                // a pass-through line while it still has siblings below.
                renderLevel(
                    children,
                    depth + 1,
                    theme.id,
                    depth === 0 ? [] : [...ancestorGuides, !isLast]
                );
            });
        })(treeData, 0, null, []);

        highlightSelectedRow();
    }

    function rowFor(themeId) {
        return themesTableBody.querySelector(`tr[data-theme-id="${CSS.escape(themeId)}"]`);
    }

    function directChildRows(themeId) {
        return [...themesTableBody.querySelectorAll(`tr[data-parent-id="${CSS.escape(themeId)}"]`)];
    }

    // Collapse a node's whole subtree and reset its toggle state, so a
    // re-expanded parent shows only its direct children.
    function collapseSubtree(themeId) {
        const toggle = rowFor(themeId)?.querySelector(".tree-toggle");
        if (toggle) toggle.setAttribute("aria-expanded", "false");
        for (const child of directChildRows(themeId)) {
            child.classList.add("d-none");
            collapseSubtree(child.dataset.themeId);
        }
    }

    function setExpanded(themeId, expanded) {
        const toggle = rowFor(themeId)?.querySelector(".tree-toggle");
        if (!toggle) return;
        toggle.setAttribute("aria-expanded", String(expanded));
        for (const child of directChildRows(themeId)) {
            if (expanded) {
                child.classList.remove("d-none");
            } else {
                child.classList.add("d-none");
                collapseSubtree(child.dataset.themeId);
            }
        }
    }

    function toggleChildren(themeId) {
        const toggle = rowFor(themeId)?.querySelector(".tree-toggle");
        if (!toggle) return;
        setExpanded(themeId, toggle.getAttribute("aria-expanded") !== "true");
    }

    // Expand every ancestor so the given theme's row is visible.
    function revealTheme(themeId) {
        const ancestors = [];
        let parent = parentIdByThemeId[themeId];
        while (parent) {
            ancestors.unshift(parent);
            parent = parentIdByThemeId[parent];
        }
        for (const id of ancestors) setExpanded(id, true);
    }

    // ------------------------------------------------------------------
    // Boot
    // ------------------------------------------------------------------

    currentThemeInfoById = buildThemeInfoMap(frequencies, tree);
    clearThemeDetails();
    updateMetricCards();
    renderThemeTreeTable(tree);

    // Auto-select the top theme by frequency on load and make its row visible.
    const top = sortByFrequency(frequencies)[0];
    if (top) {
        showThemeDetails(top.theme_id);
        revealTheme(top.theme_id);
    }

    // ------------------------------------------------------------------
    // Quotes panel (issue #227) — lives inside the merged themes box and
    // follows the selected theme. Reacts to the theme:selected event (the
    // same contract the demographic-breakdown panel uses), so it needs no
    // direct coupling to showThemeDetails and survives the boot race via
    // window.__ataCurrentTheme.
    // ------------------------------------------------------------------

    const quotesLoading    = document.getElementById("quotes-loading");
    const quotesError      = document.getElementById("quotes-error");
    const quotesList       = document.getElementById("quotes-list");
    const quotesEmpty      = document.getElementById("quotes-empty");
    const quotesNone       = document.getElementById("quotes-none");
    const quotesTotal      = document.getElementById("quotes-panel-total");
    const quotesLabel      = document.getElementById("quotes-panel-title");
    const quotesPagination = document.getElementById("quotes-pagination");
    const quotesPageInfo   = document.getElementById("quotes-page-info");
    const quotesPrev       = document.getElementById("quotes-prev");
    const quotesNext       = document.getElementById("quotes-next");
    // Quote-count stat in the Theme Details header; owned by this module
    // because the count only becomes known once the quotes fetch returns.
    const themeDetailsQuotes = document.getElementById("theme-details-quotes");

    if (!quotesList) return;

    let activeQuoteThemeId = null;
    let activeQuotePage    = 1;
    let quotesRequestToken = 0; // drops stale responses after quick re-selection
    const PAGE_SIZE        = 20;

    function quotesUrl(themeId, page) {
        const url = new URL(quotesUrlTemplate.replace("__THEME__", themeId), window.location.origin);
        url.searchParams.set("page", page);
        url.searchParams.set("page_size", PAGE_SIZE);
        if (applicationRunId) {
            url.searchParams.set("application_run_id", applicationRunId);
        }
        return url.toString();
    }

    function interviewUrl(documentId) {
        return readUrlTemplate.replace("__DOC__", documentId);
    }

    function setQuotesLoading() {
        quotesNone.classList.add("d-none");
        quotesLoading.classList.remove("d-none");
        quotesError.classList.add("d-none");
        quotesList.classList.add("d-none");
        quotesEmpty.classList.add("d-none");
        quotesPagination.classList.add("d-none");
    }

    // Blank panel shown while no theme is selected.
    function resetQuotesPanel() {
        activeQuoteThemeId = null;
        quotesRequestToken++;
        quotesLabel.textContent = "Quotes";
        quotesTotal.textContent = "";
        quotesNone.classList.remove("d-none");
        quotesLoading.classList.add("d-none");
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
        if (themeDetailsQuotes) themeDetailsQuotes.textContent = String(total);

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
            link.className = "btn btn-sm btn-outline-primary";
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
        const token = ++quotesRequestToken;
        setQuotesLoading();

        const themeName = (currentThemeInfoById[themeId] || {}).theme_name || "";

        try {
            const response = await fetch(quotesUrl(themeId, page));
            const data = await response.json();
            if (token !== quotesRequestToken) return; // superseded by a newer request
            if (!response.ok || data.error) throw new Error(data.error || "HTTP " + response.status);
            renderQuotes(data, themeName);
        } catch (err) {
            if (token !== quotesRequestToken) return;
            quotesLoading.classList.add("d-none");
            quotesError.textContent = "Could not load quotes: " + err.message;
            quotesError.classList.remove("d-none");
        }
    }

    function onQuotesThemeChange(themeId) {
        if (!themeId) {
            resetQuotesPanel();
            if (themeDetailsQuotes) themeDetailsQuotes.textContent = "";
            return;
        }
        if (themeId === activeQuoteThemeId) return; // same theme — keep page and stat
        // Placeholder until this theme's quote total arrives with the fetch.
        if (themeDetailsQuotes) themeDetailsQuotes.textContent = "—";
        loadQuotes(themeId, 1);
    }

    document.addEventListener("theme:selected", (event) => {
        onQuotesThemeChange((event.detail || {}).themeId);
    });

    // Boot race: the auto-selection above fires before this listener attaches.
    const initialTheme = window.__ataCurrentTheme;
    if (initialTheme && initialTheme.themeId) {
        onQuotesThemeChange(initialTheme.themeId);
    } else {
        resetQuotesPanel();
    }

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

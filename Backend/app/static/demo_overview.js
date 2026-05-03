(function () {
    const appRoot = document.getElementById("overview-app");
    if (!appRoot) {
        return;
    }

    const apiPrefix = appRoot.dataset.apiPrefix;
    const projectId = appRoot.dataset.projectId;
    const initialVersion = appRoot.dataset.selectedVersion;

    const runSelect = document.getElementById("analysis-run-select");
    const totalInterviewsValue = document.getElementById("total-interviews-value");
    const totalThemesValue = document.getElementById("total-themes-value");
    const themesTableBody = document.getElementById("themes-table-body");
    const overviewError = document.getElementById("overview-error");
    const themeDetailsEmpty = document.getElementById("theme-details-empty");
    const themeDetailsContent = document.getElementById("theme-details-content");
    const themeDetailsName = document.getElementById("theme-details-name");
    const themeDetailsId = document.getElementById("theme-details-id");
    const themeDetailsLevel = document.getElementById("theme-details-level");
    const themeDetailsOccurrences = document.getElementById("theme-details-occurrences");
    const themeDetailsCoverage = document.getElementById("theme-details-coverage");
    const themeDetailsDescription = document.getElementById("theme-details-description");

    let currentThemeInfoById = {};
    let selectedThemeId = null;

    function showError(message) {
        if (!message) {
            overviewError.textContent = "";
            overviewError.classList.add("d-none");
            return;
        }
        overviewError.textContent = message;
        overviewError.classList.remove("d-none");
    }

    function formatCoverage(value) {
        if (typeof value !== "number") {
            return "0.00%";
        }
        return `${value.toFixed(2)}%`;
    }

    function sortByFrequency(themes) {
        return [...themes].sort((left, right) => {
            if (right.occurrence_count !== left.occurrence_count) {
                return right.occurrence_count - left.occurrence_count;
            }
            return left.theme_name.localeCompare(right.theme_name);
        });
    }

    function clearThemeDetails() {
        selectedThemeId = null;
        themeDetailsEmpty.classList.remove("d-none");
        themeDetailsContent.classList.add("d-none");
        themeDetailsName.textContent = "";
        themeDetailsId.textContent = "";
        themeDetailsLevel.textContent = "";
        themeDetailsOccurrences.textContent = "";
        themeDetailsCoverage.textContent = "";
        themeDetailsDescription.textContent = "";
    }

    function highlightSelectedThemeRow() {
        const rows = themesTableBody.querySelectorAll("tr");
        rows.forEach((row) => {
            const isSelected = row.dataset.themeId === selectedThemeId;
            row.classList.toggle("theme-row-selected", isSelected);
        });
    }

    function showThemeDetails(themeId) {
        const themeInfo = currentThemeInfoById[themeId];
        if (!themeInfo) {
            clearThemeDetails();
            return;
        }

        selectedThemeId = themeId;
        themeDetailsEmpty.classList.add("d-none");
        themeDetailsContent.classList.remove("d-none");
        themeDetailsName.textContent = themeInfo.theme_name ?? "-";
        themeDetailsId.textContent = themeInfo.theme_id ?? "-";
        themeDetailsLevel.textContent = themeInfo.theme_level ?? "-";
        themeDetailsOccurrences.textContent = String(themeInfo.occurrence_count ?? 0);
        themeDetailsCoverage.textContent = formatCoverage(
            themeInfo.interview_coverage_percentage ?? 0
        );
        themeDetailsDescription.textContent = themeInfo.description ?? "No description available.";
        highlightSelectedThemeRow();
    }

    function flattenTreeNodes(nodes, output) {
        for (const node of nodes) {
            output.push(node);
            flattenTreeNodes(node.children ?? [], output);
        }
    }

    function buildThemeInfoMap(frequencyThemes, treeNodes) {
        const map = {};
        for (const frequencyTheme of frequencyThemes) {
            map[frequencyTheme.theme_id] = {
                ...frequencyTheme,
                description: null,
            };
        }

        const flatTreeNodes = [];
        flattenTreeNodes(treeNodes, flatTreeNodes);
        for (const treeNode of flatTreeNodes) {
            const theme = treeNode.theme;
            const existing = map[theme.id] ?? {};
            map[theme.id] = {
                theme_id: theme.id,
                theme_name: existing.theme_name ?? theme.label,
                theme_level: existing.theme_level ?? theme.level,
                occurrence_count: existing.occurrence_count ?? 0,
                interview_coverage_percentage:
                    existing.interview_coverage_percentage ?? 0,
                description: theme.description ?? null,
            };
        }
        return map;
    }

    function renderThemeTable(themes) {
        themesTableBody.innerHTML = "";
        const sortedThemes = sortByFrequency(themes);

        for (const theme of sortedThemes) {
            const row = document.createElement("tr");
            row.dataset.themeId = theme.theme_id;
            row.classList.add("theme-row-selectable");
            row.addEventListener("click", () => {
                showThemeDetails(theme.theme_id);
            });

            const nameCell = document.createElement("td");
            nameCell.textContent = theme.theme_name;

            const countCell = document.createElement("td");
            countCell.className = "text-end";
            countCell.textContent = String(theme.occurrence_count);
            if (theme.occurrence_count === 0) {
                countCell.classList.add("theme-zero");
            }

            const coverageCell = document.createElement("td");
            coverageCell.className = "text-end";
            coverageCell.textContent = formatCoverage(theme.interview_coverage_percentage);
            if (theme.interview_coverage_percentage === 0) {
                coverageCell.classList.add("theme-zero");
            }

            row.append(nameCell, countCell, coverageCell);
            themesTableBody.appendChild(row);
        }

        totalThemesValue.textContent = String(sortedThemes.length);
        highlightSelectedThemeRow();
    }

    function toJsTreeNodes(nodes) {
        return nodes.map((node) => ({
            id: node.theme.id,
            text: `${node.theme.label} (${node.theme.level})`,
            children: toJsTreeNodes(node.children ?? []),
        }));
    }

    function renderTree(treeData) {
        const treeElement = $("#theme-tree");
        const treeNodes = toJsTreeNodes(treeData);

        if (treeElement.jstree(true)) {
            treeElement.jstree(true).destroy();
        }

        treeElement.jstree({
            core: {
                data: treeNodes,
                themes: {
                    dots: true,
                    icons: false,
                },
            },
        });
        treeElement.on("select_node.jstree", (event, data) => {
            showThemeDetails(data.node.id);
        });
    }

    async function loadOverview(version) {
        const frequencyUrl =
            `${apiPrefix}/projects/${encodeURIComponent(projectId)}/themes` +
            `?version=${encodeURIComponent(version)}&include_candidate_nodes=true`;
        const treeUrl =
            `${apiPrefix}/projects/${encodeURIComponent(projectId)}/themes/tree` +
            `?version=${encodeURIComponent(version)}&include_candidate_nodes=true`;

        try {
            const [frequencyResponse, treeResponse] = await Promise.all([
                fetch(frequencyUrl),
                fetch(treeUrl),
            ]);

            if (!frequencyResponse.ok || !treeResponse.ok) {
                throw new Error("Failed to load overview data for the selected run.");
            }

            const [frequencyPayload, treePayload] = await Promise.all([
                frequencyResponse.json(),
                treeResponse.json(),
            ]);

            const frequencyData = frequencyPayload.data;
            const treeData = treePayload.data;
            const frequencyThemes = frequencyData?.themes ?? [];
            const treeNodes = treeData?.tree ?? [];

            currentThemeInfoById = buildThemeInfoMap(frequencyThemes, treeNodes);
            clearThemeDetails();

            totalInterviewsValue.textContent = String(
                frequencyData?.total_interviews_in_corpus ?? 0
            );
            renderThemeTable(frequencyThemes);
            renderTree(treeNodes);

            const sortedThemes = sortByFrequency(frequencyThemes);
            if (sortedThemes.length > 0) {
                showThemeDetails(sortedThemes[0].theme_id);
            }
            showError("");
        } catch (error) {
            showError(error.message);
        }
    }

    runSelect.addEventListener("change", () => {
        const selectedVersion = runSelect.value;
        const query = new URLSearchParams({
            project_id: projectId,
            version: selectedVersion,
        });
        window.history.replaceState({}, "", `/demo/overview?${query.toString()}`);
        loadOverview(selectedVersion);
    });

    loadOverview(initialVersion);
})();

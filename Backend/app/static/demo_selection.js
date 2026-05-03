(function () {
    const appRoot = document.getElementById("selection-app");
    if (!appRoot) {
        return;
    }

    const tableBody = document.getElementById("codebook-table-body");
    const openOverviewButton = document.getElementById("open-overview-btn");
    const errorElement = document.getElementById("selection-error");

    const apiPrefix = appRoot.dataset.apiPrefix;
    const initialCodebooks = JSON.parse(appRoot.dataset.initialCodebooks ?? "[]");
    let selectedCodebookId = null;

    function getCodebookId(codebook) {
        if (!codebook || typeof codebook !== "object") {
            return "";
        }
        return String(codebook.id ?? codebook.codebook_id ?? "").trim();
    }

    function setError(message) {
        if (!message) {
            errorElement.textContent = "";
            errorElement.classList.add("d-none");
            return;
        }
        errorElement.textContent = message;
        errorElement.classList.remove("d-none");
    }

    function renderCodebooks(codebooks) {
        tableBody.innerHTML = "";
        selectedCodebookId = null;
        openOverviewButton.disabled = true;

        for (const codebook of codebooks) {
            const codebookId = getCodebookId(codebook);
            if (!codebookId) {
                continue;
            }

            const row = document.createElement("tr");

            const selectCell = document.createElement("td");
            const radio = document.createElement("input");
            radio.type = "radio";
            radio.name = "codebook-selection";
            radio.value = codebookId;
            radio.className = "form-check-input";
            radio.addEventListener("change", () => {
                selectedCodebookId = codebookId;
                openOverviewButton.disabled = false;
            });
            selectCell.appendChild(radio);

            const projectCell = document.createElement("td");
            projectCell.textContent = codebook.project_id;

            const nameCell = document.createElement("td");
            nameCell.textContent = codebook.name;

            const versionCell = document.createElement("td");
            versionCell.textContent = String(codebook.version);

            row.append(selectCell, projectCell, nameCell, versionCell);
            tableBody.appendChild(row);
        }
    }

    async function loadCodebooks() {
        try {
            const response = await fetch(`${apiPrefix}/codebooks/`);
            if (!response.ok) {
                throw new Error(`Failed to load codebooks (${response.status})`);
            }
            const payload = await response.json();
            const codebooks = payload.data ?? [];
            renderCodebooks(codebooks);
            setError("");
        } catch (error) {
            renderCodebooks(initialCodebooks);
            setError(error.message);
        }
    }

    openOverviewButton.addEventListener("click", () => {
        if (!selectedCodebookId) {
            setError("Please select a valid codebook.");
            return;
        }
        const query = new URLSearchParams({
            codebook_id: selectedCodebookId,
        });
        window.location.href = `/demo/overview?${query.toString()}`;
    });

    loadCodebooks();
})();

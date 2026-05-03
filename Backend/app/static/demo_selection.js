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
    let selectedCodebook = null;

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
        selectedCodebook = null;
        openOverviewButton.disabled = true;

        for (const codebook of codebooks) {
            const row = document.createElement("tr");

            const selectCell = document.createElement("td");
            const radio = document.createElement("input");
            radio.type = "radio";
            radio.name = "codebook-selection";
            radio.className = "form-check-input";
            radio.addEventListener("change", () => {
                selectedCodebook = codebook;
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
        if (!selectedCodebook) {
            return;
        }
        const query = new URLSearchParams({
            project_id: selectedCodebook.project_id,
            version: String(selectedCodebook.version),
        });
        window.location.href = `/demo/overview?${query.toString()}`;
    });

    loadCodebooks();
})();

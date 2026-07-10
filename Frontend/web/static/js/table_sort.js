document.addEventListener("DOMContentLoaded", () => {
  const sortHeaders = document.querySelectorAll("th[data-sort-col]");

  sortHeaders.forEach(header => {
    header.addEventListener("click", () => {
      const table = header.closest("table");
      const tbody = table.querySelector("tbody");
      const colIndex = parseInt(header.dataset.sortCol, 10);
      
      // Determine new sort direction (default to asc on first click)
      const currentDirection = header.dataset.sortDir || "none";
      const newDirection = currentDirection === "asc" ? "desc" : "asc";
      
      // Reset all headers in this table
      table.querySelectorAll("th[data-sort-col]").forEach(th => {
        th.dataset.sortDir = "none";
        const icon = th.querySelector(".sort-icon");
        if (icon) {
          icon.className = "bi bi-arrow-down-up text-muted ms-1 sort-icon";
        }
      });
      
      // Update this header
      header.dataset.sortDir = newDirection;
      const icon = header.querySelector(".sort-icon");
      if (icon) {
        icon.className = newDirection === "asc" 
          ? "bi bi-arrow-up text-primary ms-1 sort-icon" 
          : "bi bi-arrow-down text-primary ms-1 sort-icon";
      }

      // Sort rows
      const rows = Array.from(tbody.querySelectorAll("tr"));
      
      rows.sort((rowA, rowB) => {
        const cellA = rowA.children[colIndex];
        const cellB = rowB.children[colIndex];
        
        if (!cellA || !cellB) return 0;
        
        const valA = (cellA.dataset.sortValue !== undefined ? cellA.dataset.sortValue : cellA.textContent).trim();
        const valB = (cellB.dataset.sortValue !== undefined ? cellB.dataset.sortValue : cellB.textContent).trim();
        
        // Try numeric sort first, fallback to localeCompare
        const numA = Number(valA);
        const numB = Number(valB);
        
        let comparison = 0;
        if (!isNaN(numA) && !isNaN(numB) && valA !== "" && valB !== "") {
          comparison = numA - numB;
        } else {
          comparison = valA.localeCompare(valB, undefined, {numeric: true});
        }
        
        return newDirection === "asc" ? comparison : -comparison;
      });
      
      // Append sorted rows back to tbody
      rows.forEach(row => tbody.appendChild(row));
    });
  });
});

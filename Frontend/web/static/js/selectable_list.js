(() => {
  class SelectableList {
    constructor(root) {
      this.root = root;
      this.selectAll = root.querySelector("[data-selectable-list-select-all]");
      this.selectedCount = root.querySelector("[data-selectable-list-selected-count]");
      this.checkboxes = Array.from(root.querySelectorAll("[data-selectable-list-checkbox]"));
      this.rows = Array.from(root.querySelectorAll("[data-selectable-list-row]"));
      this.forms = Array.from(root.querySelectorAll("[data-selectable-list-selected-form]"));
      this.actions = Array.from(root.querySelectorAll("[data-selectable-list-action]"));

      if (!this.selectAll || !this.selectedCount || this.checkboxes.length === 0) return;

      this.selectAll.addEventListener("change", () => {
        this.checkboxes.forEach((checkbox) => {
          checkbox.checked = this.selectAll.checked;
        });
        this.update();
      });

      this.checkboxes.forEach((checkbox) => {
        checkbox.addEventListener("change", () => this.update());
      });

      this.update();
    }

    getSelectedIds() {
      return this.checkboxes
        .filter((checkbox) => checkbox.checked)
        .map((checkbox) => checkbox.value);
    }

    update() {
      const selectedIdsList = this.getSelectedIds();
      const selectedIds = new Set(selectedIdsList);
      const selectedCount = selectedIdsList.length;
      const totalCount = this.checkboxes.length;

      this.selectAll.checked = selectedCount === totalCount;
      this.selectAll.indeterminate = selectedCount > 0 && selectedCount < totalCount;

      this.rows.forEach((row) => {
        row.classList.toggle("ata-selectable-list__row--selected", selectedIds.has(row.dataset.itemId));
      });

      const singular = this.selectedCount.dataset.countSingular || "item";
      const plural = this.selectedCount.dataset.countPlural || "items";
      const label = selectedCount === 1 ? singular : plural;
      this.selectedCount.textContent = `${selectedCount} ${label} selected`;

      this.updateForms(selectedIdsList);
      this.updateActions(selectedIdsList);
    }

    updateForms(selectedIds) {
      this.forms.forEach((form) => {
        form.querySelectorAll("input[data-selectable-list-selected-input]").forEach((input) => input.remove());
        selectedIds.forEach((id) => {
          const input = document.createElement("input");
          input.type = "hidden";
          input.name = "item_ids";
          input.value = id;
          input.setAttribute("data-selectable-list-selected-input", "");
          form.appendChild(input);
        });
      });
    }

    updateActions(selectedIds) {
      const selectedCount = selectedIds.length;
      this.actions.forEach((action) => {
        const min = Number(action.dataset.minSelected || 0);
        const max = action.dataset.maxSelected ? Number(action.dataset.maxSelected) : Infinity;
        const enabled = selectedCount >= min && selectedCount <= max;
        const disabledTitle = action.dataset.disabledTitle || "";

        if (action.tagName === "A") {
          if (enabled) {
            const template = action.dataset.selectedUrlTemplate;
            if (template) action.href = template.replace("__ITEM_ID__", encodeURIComponent(selectedIds[0]));
            action.classList.remove("disabled");
            action.removeAttribute("aria-disabled");
            action.removeAttribute("tabindex");
            action.removeAttribute("title");
          } else {
            action.href = "#";
            action.classList.add("disabled");
            action.setAttribute("aria-disabled", "true");
            action.setAttribute("tabindex", "-1");
            if (disabledTitle) action.setAttribute("title", disabledTitle);
          }
          return;
        }

        action.disabled = !enabled;
        if (enabled) {
          action.removeAttribute("title");
        } else if (disabledTitle) {
          action.setAttribute("title", disabledTitle);
        }
      });
    }
  }

  function initSelectableLists() {
    document.querySelectorAll("[data-selectable-list]").forEach((root) => {
      new SelectableList(root);
    });
  }

  document.addEventListener("DOMContentLoaded", initSelectableLists);
  window.ATASelectableList = { SelectableList, init: initSelectableLists };
})();

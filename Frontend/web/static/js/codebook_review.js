// Codebook review/create editor.
//
// Backs codebooks/review.html for both flows: reviewing a generated codebook
// (saved as a new version) and creating one from scratch / a CSV upload. The
// hierarchy is stored *relationally* (each row points at its parent element),
// and the visual indentation is derived from that tree, so renaming a row never
// breaks a parent link. Drag re-parents a row with its whole subtree.
(() => {
  const container = document.getElementById("codebook-rows");
  const form = document.getElementById("review-form");
  if (!container || !form) return;
  const parentsShadow = document.getElementById("row-parents-shadow");
  const typesShadow = document.getElementById("row-types-shadow");
  const nameInput = document.getElementById("codebook_name");

  // ---- Unsaved-changes guard ----------------------------------------
  let formSubmitting = false;
  const serialize = (f) => [...new FormData(f).entries()]
    .map(([k, v]) => k + "=" + v).sort().join("&");
  const originalData = serialize(form);
  nameInput.addEventListener("input", () => {
    if (nameInput.value.trim()) nameInput.classList.remove("is-invalid");
  });
  window.addEventListener("beforeunload", (e) => {
    if (formSubmitting) return;
    if (serialize(form) !== originalData) { e.preventDefault(); e.returnValue = ""; }
  });

  // ---- Row factories -------------------------------------------------
  function makeRow(isCode) {
    const row = document.createElement("div");
    row.className = "codebook-row" + (isCode ? " is-code" : "");
    row.draggable = true;
    row.setAttribute("data-is-code", isCode ? "1" : "0");
    row.innerHTML = `
      <div class="row-card d-flex align-items-start gap-2">
        <button type="button" class="chevron-btn" title="Collapse / expand subtree" aria-label="Collapse or expand">&vee;</button>
        <span class="drag-handle" aria-hidden="true">&#x2630;</span>
        <div class="indent-controls btn-group btn-group-sm" role="group">
          <button type="button" class="btn btn-outline-secondary outdent-btn" title="Outdent">&larr;</button>
          <button type="button" class="btn btn-outline-secondary indent-btn" title="Indent">&rarr;</button>
        </div>
        <div class="flex-grow-1">
          <div class="field-row">
            <label class="field-label">Name:</label>
            <input type="text" class="form-control form-control-sm fw-semibold"
                   name="row_names[]" placeholder="Name" required>
          </div>
          <div class="field-row">
            <label class="field-label">Description:</label>
            <textarea class="form-control form-control-sm"
                      name="row_descriptions[]" rows="1"
                      placeholder="Description" required></textarea>
          </div>
        </div>
        <div class="d-flex flex-column align-items-end gap-1">
          <button type="button" class="btn btn-sm btn-outline-secondary remove-row-btn"
                  aria-label="Remove">&times;</button>
          <label class="form-check form-switch mb-0 small" title="Mark this row as a code (leaf observation)">
            <input class="form-check-input code-toggle" type="checkbox"
                   aria-label="Code" ${isCode ? "checked" : ""}>
          </label>
        </div>
      </div>
      <div class="node-children"></div>`;
    return row;
  }

  // ---- Relational tree model ----------------------------------------
  // parentOf maps a row element -> its parent row element (absent = root).
  // This is the source of truth for the hierarchy; it is keyed on element
  // identity so renaming a node never breaks the relationship. The visual
  // indent is *derived* from this tree, not stored.
  const parentOf = new WeakMap();
  const rows = () => [...container.querySelectorAll(".codebook-row")];
  const parentRow = (row) => parentOf.get(row) || null;
  const childrenOf = (node) => rows().filter((r) => parentRow(r) === node);

  // Physically nest each row inside its parent's .node-children container,
  // so the parent's frame encloses its whole subtree. Moving a node carries
  // its subtree because descendants live inside its .node-children.
  const childBox = (row) => row.querySelector(":scope > .node-children");
  const setCode = (row, isCode) => {
    row.dataset.isCode = isCode ? "1" : "0";
    row.classList.toggle("is-code", isCode);
    const t = row.querySelector(":scope > .row-card .code-toggle");
    if (t) t.checked = isCode;
  };
  const isCodeRow = (row) => row.dataset.isCode === "1";
  // Shown when an edit would leave a code at the top level. We refuse the
  // move (leaving the row in place) rather than silently un-coding it.
  const CODE_NEEDS_PARENT_MSG =
    "A code can’t sit at the top level. A code captures a specific observed " +
    "phenomenon, so it must always live under a theme or subtheme. If you’ve " +
    "decided this entry should stand on its own as a theme, untoggle its " +
    "“Code” switch first, then you can move it to the top level.";
  // Shown when an edit would give a code a child. Codes are always leaves.
  const CODE_IS_LEAF_MSG =
    "A code must be a leaf, it can’t have entries nested under it. A code " +
    "captures a specific observed phenomenon; use a theme or subtheme when " +
    "something needs children. Untoggle the “Code” switch first if this entry " +
    "should contain others.";
  function renderTree() {
    const childMap = new Map();
    rows().forEach((r) => {
      const key = parentRow(r);
      if (!childMap.has(key)) childMap.set(key, []);
      childMap.get(key).push(r);
    });
    (function place(parent, host) {
      (childMap.get(parent) || []).forEach((r) => {
        host.appendChild(r);
        const hasKids = (childMap.get(r) || []).length > 0;
        r.classList.toggle("has-children", hasKids);
        if (!hasKids) r.classList.remove("collapsed");
        place(r, childBox(r));
      });
    })(null, container);
  }

  // ---- Description textarea auto-resize --------------------------------
  // Grows with content up to 5 lines, then enables vertical scroll.
  function autoResize(ta) {
    ta.style.height = "auto";
    const style = getComputedStyle(ta);
    const lineH = parseFloat(style.lineHeight) || 20;
    const padV = parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
    const minH = lineH * 1 + padV;
    const maxH = lineH * 5 + padV;
    const newH = Math.min(Math.max(ta.scrollHeight, minH), maxH);
    ta.style.height = newH + "px";
    ta.style.overflowY = ta.scrollHeight > maxH ? "auto" : "hidden";
  }
  container.addEventListener("input", (e) => {
    if (e.target.name === "row_descriptions[]") autoResize(e.target);
  });

  function addRow(isCode) {
    if (rows().length >= 50) {
      alert("A codebook cannot contain more than 50 rows.");
      return;
    }
    const row = makeRow(isCode);
    container.appendChild(row);
    row.querySelectorAll('textarea[name="row_descriptions[]"]').forEach(autoResize);
    renderTree();
  }

  document.getElementById("add-row-btn")
    .addEventListener("click", () => addRow(false));

  // ---- Indent / outdent / remove ------------------------------------
  // These change only the moved node's parent pointer; its descendants
  // keep pointing at it, so re-parenting cascades through the subtree and
  // renderTree() re-derives every indent from the new tree.
  function indentRow(row) {
    const siblings = childrenOf(parentRow(row));
    const i = siblings.indexOf(row);
    if (i <= 0) return;
    const newParent = siblings[i - 1];
    if (isCodeRow(newParent)) { alert(CODE_IS_LEAF_MSG); return; }
    parentOf.set(row, newParent);
    renderTree();
  }
  function outdentRow(row) {
    const parent = parentRow(row);
    if (!parent) return;
    const grand = parentRow(parent);
    if (!grand && isCodeRow(row)) { alert(CODE_NEEDS_PARENT_MSG); return; }
    if (grand) parentOf.set(row, grand); else parentOf.delete(row);
    renderTree();
  }

  container.addEventListener("click", (e) => {
    const row = e.target.closest(".codebook-row");
    if (!row) return;
    if (e.target.closest(".chevron-btn")) {
      row.classList.toggle("collapsed");
    } else if (e.target.classList.contains("remove-row-btn")) {
      if (rows().length <= 1) {
        alert("A codebook must contain at least 1 row.");
        return;
      }
      const grand = parentRow(row);
      if (!grand && childrenOf(row).some(isCodeRow)) {
        alert(CODE_NEEDS_PARENT_MSG);
        return;
      }
      childrenOf(row).forEach((c) => {
        if (grand) parentOf.set(c, grand); else parentOf.delete(c);
        container.appendChild(c);
      });
      row.remove();
      renderTree();
    } else if (e.target.classList.contains("indent-btn")) {
      indentRow(row);
    } else if (e.target.classList.contains("outdent-btn")) {
      outdentRow(row);
    }
  });

  container.addEventListener("change", (e) => {
    if (!e.target.classList.contains("code-toggle")) return;
    const row = e.target.closest(".codebook-row");
    if (e.target.checked && !parentRow(row)) {
      e.target.checked = false;
      alert("A code must sit under a theme or subtheme. Nest this row under " +
            "another one (drag it onto a row, or use →) before marking it as a code.");
      return;
    }
    if (e.target.checked && childrenOf(row).length) {
      e.target.checked = false;
      alert(CODE_IS_LEAF_MSG);
      return;
    }
    setCode(row, e.target.checked);
  });

  // ---- Drag / drop ---------------------------------------------------
  // Drag RE-PARENTS the node (with its whole subtree). Where you drop on a
  // target row decides what happens: top edge => place before it, bottom
  // edge => place after it (both as a sibling, adopting the target's
  // parent), middle => nest INTO it as a child. parentOf is updated, then
  // renderTree() normalises nesting, order and indents.
  let dragSrc = null;
  const DROP_ZONES = ["drop-before", "drop-inside", "drop-after"];
  const clearDropMarks = () =>
    container.querySelectorAll("." + DROP_ZONES.join(",."))
      .forEach((el) => el.classList.remove(...DROP_ZONES));
  function dropZone(row, clientY) {
    const card = row.querySelector(":scope > .row-card");
    const r = card.getBoundingClientRect();
    const t = (clientY - r.top) / r.height;
    if (t < 0.25) return "before";
    if (t > 0.75) return "after";
    return "inside";
  }
  const dropTarget = (e) => {
    const row = e.target.closest(".codebook-row");
    if (!row || !dragSrc || row === dragSrc || dragSrc.contains(row)) return null;
    return row;
  };

  container.addEventListener("dragstart", (e) => {
    const row = e.target.closest(".codebook-row");
    if (!row) return;
    dragSrc = row;
    row.classList.add("dragging");
    e.dataTransfer.effectAllowed = "move";
  });
  container.addEventListener("dragend", (e) => {
    const row = e.target.closest(".codebook-row");
    if (row) row.classList.remove("dragging");
    clearDropMarks();
    dragSrc = null;
    renderTree();
  });
  container.addEventListener("dragover", (e) => {
    const row = dropTarget(e);
    if (!row) return;
    e.preventDefault();
    clearDropMarks();
    row.classList.add("drop-" + dropZone(row, e.clientY));
  });
  container.addEventListener("drop", (e) => {
    e.preventDefault();
    const row = dropTarget(e);
    if (!row) return;
    const zone = dropZone(row, e.clientY);
    if (zone === "inside") {
      if (isCodeRow(row)) {
        alert(CODE_IS_LEAF_MSG);
        return;
      }
      parentOf.set(dragSrc, row);
      childBox(row).appendChild(dragSrc);
      row.classList.remove("collapsed");
    } else {
      const newParent = parentRow(row);
      if (!newParent && isCodeRow(dragSrc)) {
        alert(CODE_NEEDS_PARENT_MSG);
        return;
      }
      if (newParent) parentOf.set(dragSrc, newParent); else parentOf.delete(dragSrc);
      row.parentNode.insertBefore(dragSrc, zone === "after" ? row.nextSibling : row);
    }
  });

  // ---- Bootstrap: rebuild parent pointers from the server's indents --
  // The server still ships positional indents; convert them once into the
  // relational parentOf map, after which the tree is the source of truth.
  (function bootstrap() {
    const stack = [];
    rows().forEach((row) => {
      const indent = Math.max(0, parseInt(row.dataset.indent || "0", 10));
      if (indent > 0 && stack[indent - 1]) parentOf.set(row, stack[indent - 1]);
      stack[indent] = row;
      stack.length = indent + 1;
    });
    renderTree();
    container.querySelectorAll('textarea[name="row_descriptions[]"]').forEach(autoResize);
  })();

  // ---- Submit: encode hierarchy + type per row -----------------------
  form.addEventListener("submit", (e) => {
    if (!nameInput.value.trim()) {
      e.preventDefault();
      nameInput.value = "";
      nameInput.classList.add("is-invalid");
      nameInput.focus();
      return;
    }
    formSubmitting = true;
    parentsShadow.innerHTML = "";
    typesShadow.innerHTML = "";
    rows().forEach((row) => {
      const parent = parentRow(row);
      const parentName = parent
        ? (parent.querySelector(':scope > .row-card input[name="row_names[]"]').value || "").trim()
        : "";
      const isCode = row.dataset.isCode === "1";

      const pHidden = document.createElement("input");
      pHidden.type = "hidden";
      pHidden.name = "row_parents[]";
      pHidden.value = parentName;
      parentsShadow.appendChild(pHidden);

      const tHidden = document.createElement("input");
      tHidden.type = "hidden";
      tHidden.name = "row_is_codes[]";
      tHidden.value = isCode ? "1" : "0";
      typesShadow.appendChild(tHidden);
    });
  });
})();

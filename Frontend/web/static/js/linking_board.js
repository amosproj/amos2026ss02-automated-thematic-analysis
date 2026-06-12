(() => {
  "use strict";

  const board = document.querySelector("[data-linking-board]");
  const dataEl = document.getElementById("linking-data");
  if (!board || !dataEl) return;

  const linkUrl = board.dataset.linkUrl;
  const unlinkUrl = board.dataset.unlinkUrl;
  const transcriptsCol = board.querySelector("[data-transcripts-column]");
  const demographicCol = board.querySelector("[data-demographic-column]");
  const matchedCountEl = board.querySelector("[data-linking-matched-count]");
  const flashEl = document.getElementById("linking-flash");

  /** Mutable board state, refreshed from each server response. */
  let state = { transcripts: [], rows: [] };
  /** Transcript selected via click (keyboard / no-drag fallback). */
  let selectedDocId = null;

  try {
    const parsed = JSON.parse(dataEl.textContent || "{}");
    state.transcripts = parsed.transcripts || [];
    state.rows = parsed.demographic_rows || [];
  } catch (_err) {
    state = { transcripts: [], rows: [] };
  }

  // ---- helpers -------------------------------------------------------------

  const rowsById = () => {
    const map = new Map();
    state.rows.forEach((r) => map.set(r.row_id, r));
    return map;
  };

  const transcriptsById = () => {
    const map = new Map();
    state.transcripts.forEach((t) => map.set(t.document_id, t));
    return map;
  };

  const el = (tag, opts = {}) => {
    const node = document.createElement(tag);
    if (opts.className) node.className = opts.className;
    if (opts.text != null) node.textContent = opts.text;
    if (opts.attrs) {
      Object.entries(opts.attrs).forEach(([k, v]) => node.setAttribute(k, v));
    }
    return node;
  };

  const showFlash = (message, kind = "danger") => {
    if (!flashEl) return;
    flashEl.innerHTML = "";
    const alert = el("div", {
      className: `alert alert-${kind} alert-dismissible fade show mb-0`,
      attrs: { role: "alert" },
    });
    alert.appendChild(document.createTextNode(message));
    const btn = el("button", {
      className: "btn-close",
      attrs: { type: "button", "data-bs-dismiss": "alert", "aria-label": "Close" },
    });
    alert.appendChild(btn);
    flashEl.appendChild(alert);
  };

  const clearFlash = () => {
    if (flashEl) flashEl.innerHTML = "";
  };

  const dataPreview = (data) => {
    if (!data) return "";
    return Object.entries(data)
      .slice(0, 3)
      .map(([k, v]) => `${k}: ${v}`)
      .join(" · ");
  };

  // ---- rendering -----------------------------------------------------------

  const renderTranscriptCard = (t) => {
    const rows = rowsById();
    const linkedRow = t.demographic_row_id ? rows.get(t.demographic_row_id) : null;

    const card = el("div", {
      className: "ata-link-card border rounded-2 p-2 mb-2",
      attrs: {
        "data-transcript-card": "",
        "data-document-id": t.document_id,
        draggable: "true",
        tabindex: "0",
        role: "button",
        "aria-label": `Transcript ${t.document_title}`,
      },
    });
    if (selectedDocId === t.document_id) card.classList.add("ata-link-card--selected");

    const title = el("div", { className: "fw-semibold text-truncate", text: t.document_title });
    card.appendChild(title);

    const statusRow = el("div", { className: "d-flex align-items-center justify-content-between mt-1 gap-2" });
    if (linkedRow) {
      const badge = el("span", {
        className: "badge text-bg-success text-truncate",
        text: `Linked: ${linkedRow.interviewee_id}`,
      });
      statusRow.appendChild(badge);
      const unlinkBtn = el("button", {
        className: "btn btn-sm btn-outline-secondary",
        attrs: { type: "button", "data-unlink-document": t.document_id },
        text: "Unlink",
      });
      statusRow.appendChild(unlinkBtn);
    } else {
      const badge = el("span", {
        className: "badge text-bg-warning",
        text: "No demographic data",
      });
      statusRow.appendChild(badge);
    }
    card.appendChild(statusRow);
    return card;
  };

  const renderRowCard = (r) => {
    const transcripts = transcriptsById();
    const linkedDoc = r.linked_document_id ? transcripts.get(r.linked_document_id) : null;

    const card = el("div", {
      className: "ata-link-card ata-link-dropzone border rounded-2 p-2 mb-2",
      attrs: { "data-row-card": "", "data-row-id": r.row_id },
    });
    if (r.linked) card.classList.add("ata-link-card--linked");

    const name = el("div", { className: "fw-semibold text-truncate", text: r.interviewee_id });
    card.appendChild(name);

    const preview = dataPreview(r.data);
    if (preview) {
      card.appendChild(el("div", { className: "text-secondary small text-truncate", text: preview }));
    }

    if (linkedDoc) {
      const badge = el("span", {
        className: "badge text-bg-success mt-1 text-truncate",
        text: `↔ ${linkedDoc.document_title}`,
      });
      card.appendChild(badge);
    }
    return card;
  };

  const render = () => {
    transcriptsCol.innerHTML = "";
    demographicCol.innerHTML = "";

    if (state.transcripts.length === 0) {
      transcriptsCol.appendChild(el("p", { className: "text-secondary small mb-0 p-2", text: "No transcripts in this corpus." }));
    } else {
      state.transcripts.forEach((t) => transcriptsCol.appendChild(renderTranscriptCard(t)));
    }

    if (state.rows.length === 0) {
      demographicCol.appendChild(el("p", { className: "text-secondary small mb-0 p-2", text: "No demographic rows. Upload a demographic CSV first." }));
    } else {
      state.rows.forEach((r) => demographicCol.appendChild(renderRowCard(r)));
    }

    if (matchedCountEl) {
      matchedCountEl.textContent = `${state.matched ?? countMatched()} / ${state.transcripts.length} linked`;
    }
  };

  const countMatched = () => state.transcripts.filter((t) => t.demographic_row_id).length;

  // ---- server calls --------------------------------------------------------

  const applySummary = (payload) => {
    state.transcripts = payload.transcripts || [];
    state.rows = payload.demographic_rows || [];
    state.matched = payload.matched;
    selectedDocId = null;
    render();
  };

  const postJson = async (url, body) => {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    let payload = {};
    try {
      payload = await resp.json();
    } catch (_err) {
      payload = {};
    }
    if (!resp.ok || !payload.ok) {
      throw new Error(payload.error || "Could not update the link. Please try again.");
    }
    return payload;
  };

  const link = async (documentId, rowId) => {
    if (!documentId || !rowId) return;
    clearFlash();
    try {
      applySummary(await postJson(linkUrl, { document_id: documentId, demographic_row_id: rowId }));
    } catch (err) {
      showFlash(err.message);
      render();
    }
  };

  const unlink = async (documentId) => {
    if (!documentId) return;
    clearFlash();
    try {
      applySummary(await postJson(unlinkUrl, { document_id: documentId }));
    } catch (err) {
      showFlash(err.message);
      render();
    }
  };

  // ---- interaction: drag & drop -------------------------------------------

  transcriptsCol.addEventListener("dragstart", (e) => {
    const card = e.target.closest("[data-transcript-card]");
    if (!card) return;
    e.dataTransfer.setData("text/plain", card.dataset.documentId);
    e.dataTransfer.effectAllowed = "move";
    card.classList.add("ata-link-card--dragging");
  });

  transcriptsCol.addEventListener("dragend", (e) => {
    const card = e.target.closest("[data-transcript-card]");
    if (card) card.classList.remove("ata-link-card--dragging");
  });

  demographicCol.addEventListener("dragover", (e) => {
    const zone = e.target.closest("[data-row-card]");
    if (!zone) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    zone.classList.add("ata-link-dropzone--over");
  });

  demographicCol.addEventListener("dragleave", (e) => {
    const zone = e.target.closest("[data-row-card]");
    if (zone) zone.classList.remove("ata-link-dropzone--over");
  });

  demographicCol.addEventListener("drop", (e) => {
    const zone = e.target.closest("[data-row-card]");
    if (!zone) return;
    e.preventDefault();
    zone.classList.remove("ata-link-dropzone--over");
    const documentId = e.dataTransfer.getData("text/plain");
    link(documentId, zone.dataset.rowId);
  });

  // ---- interaction: click fallback (keyboard-friendly) --------------------

  transcriptsCol.addEventListener("click", (e) => {
    const unlinkBtn = e.target.closest("[data-unlink-document]");
    if (unlinkBtn) {
      e.stopPropagation();
      unlink(unlinkBtn.getAttribute("data-unlink-document"));
      return;
    }
    const card = e.target.closest("[data-transcript-card]");
    if (!card) return;
    const docId = card.dataset.documentId;
    selectedDocId = selectedDocId === docId ? null : docId;
    render();
  });

  transcriptsCol.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const card = e.target.closest("[data-transcript-card]");
    if (!card) return;
    e.preventDefault();
    const docId = card.dataset.documentId;
    selectedDocId = selectedDocId === docId ? null : docId;
    render();
  });

  demographicCol.addEventListener("click", (e) => {
    const card = e.target.closest("[data-row-card]");
    if (!card || !selectedDocId) return;
    link(selectedDocId, card.dataset.rowId);
  });

  // ---- init ----------------------------------------------------------------

  render();
})();

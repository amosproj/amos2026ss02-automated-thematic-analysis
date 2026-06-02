(() => {
  const form = document.getElementById("generate-codebook-form");
  if (!form) return;

  const textarea = document.getElementById("research_query");
  const counter = document.getElementById("rq-counter");
  const errorDiv = document.getElementById("rq-error");
  const liveRegion = document.getElementById("rq-live");
  const MIN = 10;
  const MAX = 500;

  function updateCounter() {
    const len = textarea.value.trim().length;
    counter.textContent = `${len} / ${MAX}`;

    if (len === 0 || len < MIN) {
      textarea.classList.add("is-invalid");
      textarea.classList.remove("is-valid");
      errorDiv.textContent =
        len === 0
          ? "Please enter a research question."
          : `Please enter at least ${MIN} characters (${MIN - len} more needed).`;
    } else if (len > MAX) {
      textarea.classList.add("is-invalid");
      textarea.classList.remove("is-valid");
      errorDiv.textContent = `Maximum ${MAX} characters reached. Please shorten your question.`;
    } else {
      textarea.classList.remove("is-invalid");
      textarea.classList.add("is-valid");
      errorDiv.textContent = "";
    }
  }

  textarea.addEventListener("input", updateCounter);

  form.addEventListener("submit", (event) => {
    const len = textarea.value.trim().length;
    if (len < MIN || len > MAX) {
      event.preventDefault();
      updateCounter();
      textarea.focus();
      liveRegion.textContent =
        len === 0
          ? "Research question is required."
          : len < MIN
          ? `Research question is too short. Please enter at least ${MIN} characters.`
          : `Research question is too long. Maximum ${MAX} characters allowed.`;
    }
  });
})();
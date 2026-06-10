import atexit
import logging

from flask import Flask, current_app, flash, redirect, render_template, url_for

from web.config import Config, get_config
from web.services.backend_client import BackendClient


def create_app(config: Config | None = None) -> Flask:
    app = Flask(__name__)
    cfg = config or get_config()
    app.config.from_object(cfg)

    # Cap raw request body size so oversized uploads short-circuit with a 413
    # before Werkzeug buffers the whole body into memory.
    app.config["MAX_CONTENT_LENGTH"] = cfg.MAX_CONTENT_LENGTH

    # Propagate the configured LOG_LEVEL to Flask's logger so backend_client
    # log lines actually appear (Flask defaults to WARNING in production).
    log_level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(level=log_level)
    app.logger.setLevel(log_level)

    # Shared across requests: pooled connections + memoised corpus id.
    # `atexit` (not `teardown_appcontext`) is the right hook here — the latter
    # fires on every request and would close the pooled client we want to reuse.
    backend_client = BackendClient(
        cfg.BACKEND_API_URL, timeout=cfg.BACKEND_TIMEOUT_S
    )
    app.extensions["backend_client"] = backend_client
    atexit.register(backend_client.close)

    from web.controllers.analysis import bp as analysis_bp
    from web.controllers.codebooks import bp as codebooks_bp
    from web.controllers.demographic import bp as demographic_bp
    from web.controllers.ingestion import bp as ingestion_bp
    from web.controllers.main import bp as main_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/transcripts")
    app.register_blueprint(demographic_bp, url_prefix="/demographic")
    app.register_blueprint(codebooks_bp, url_prefix="/codebooks")
    app.register_blueprint(analysis_bp, url_prefix="/analysis")

    @app.template_filter('highlight_speakers')
    def highlight_speakers_filter(text):
        """Add a Q/A role badge before every non-blank line of a transcript.

        Two modes, auto-detected:
        1. Labelled transcripts — lines that begin with a recognisable speaker
           label ("Interviewer:", "I:", "P:", …) get a badge derived from the
           label (Q for interviewers, A for interviewees).
        2. Plain transcripts — no speaker labels present.  Every non-blank
           paragraph is assigned Q/A by alternating, starting with Q.
        """
        if not text:
            return text
        import re
        from markupsafe import Markup, escape

        INTERVIEWER_KEYWORDS = {
            "interviewer", "moderator", "facilitator", "researcher",
            "investigator", "int", "i", "mod", "r",
        }

        # Matches a speaker label at the very start of a line: "Name:"
        SPEAKER_RE = re.compile(r"^([ \t]*)([A-Za-z0-9 _\.\-]{1,40}):", re.MULTILINE)

        escaped = str(escape(text))
        all_lines = escaped.splitlines(keepends=True)

        # ── Detect whether this transcript uses explicit speaker labels ───────
        labelled_count = sum(1 for ln in all_lines if SPEAKER_RE.match(ln))
        has_labels = labelled_count >= max(1, len([l for l in all_lines if l.strip()]) // 4)

        def make_badge(is_q):
            cls = "q" if is_q else "a"
            letter = "Q" if is_q else "A"
            return (
                f'<span class="transcript-badge transcript-badge--{cls}">{letter}</span>'
            )

        result = []

        if has_labels:
            # ── Mode 1: use explicit speaker labels ───────────────────────────
            for line in all_lines:
                m = SPEAKER_RE.match(line)
                if m:
                    indent = m.group(1)
                    speaker = m.group(2)
                    rest = line[m.end():]
                    normalised = speaker.strip().lower()
                    is_q = normalised in INTERVIEWER_KEYWORDS
                    cls = "q" if is_q else "a"
                    result.append(
                        f'{indent}{make_badge(is_q)}'
                        f'<span class="transcript-speaker transcript-speaker--{cls}">{speaker}</span>'
                        f'<span class="transcript-colon">:</span>'
                        f'{rest}'
                    )
                else:
                    result.append(line)
        else:
            # ── Mode 2: alternate Q/A on every non-blank paragraph ────────────
            # Group consecutive non-blank lines into paragraphs.
            paragraphs = []   # list of (start_idx, end_idx_exclusive, is_blank)
            i = 0
            while i < len(all_lines):
                if all_lines[i].strip():
                    j = i
                    while j < len(all_lines) and all_lines[j].strip():
                        j += 1
                    paragraphs.append((i, j, False))
                    i = j
                else:
                    # blank run
                    j = i
                    while j < len(all_lines) and not all_lines[j].strip():
                        j += 1
                    paragraphs.append((i, j, True))
                    i = j

            turn = 0  # counts non-blank paragraphs; even → Q, odd → A
            for start, end, is_blank in paragraphs:
                if is_blank:
                    result.extend(all_lines[start:end])
                else:
                    is_q = (turn % 2 == 0)
                    badge = make_badge(is_q)
                    for k, line in enumerate(all_lines[start:end]):
                        if k == 0:
                            # Badge only on the first line of the paragraph
                            result.append(f'{badge}{line}')
                        else:
                            # Continuation lines: indent to align with text
                            result.append(
                                f'<span class="transcript-badge-spacer"></span>{line}'
                            )
                    turn += 1

        return Markup("".join(result))

    _register_error_handlers(app)

    return app


def _register_error_handlers(app: Flask) -> None:
    """Three handlers cover every uncaught case:
    404 (unknown route), 413 (request body too large), and a catch-all
    Exception handler for anything view code raises and forgets to catch.
    """

    @app.errorhandler(404)
    def not_found(_exc):
        return render_template("errors/404.html"), 404

    @app.errorhandler(413)
    def request_too_large(_exc):
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"] * 10
        flash(
            f"Upload too large — the total request exceeded {max_mb} MB.",
            "danger",
        )
        # 303 forces a GET on the redirect, so the browser doesn't try to re-POST.
        # Always redirect to home — never to a user-controlled value (e.g.
        # request.referrer) — to eliminate the open-redirect risk (CWE-601).
        # CodeQL's py/url-redirection rule recognises only a small set of
        # sanitiser patterns (strict allowlist, empty-netloc-and-scheme
        # relative URLs, Django's url_has_allowed_host_and_scheme). A
        # hardcoded url_for() target sidesteps the dataflow entirely.
        return redirect(url_for("main.index")), 303

    @app.errorhandler(Exception)
    def unhandled(exc):
        # Re-raise HTTPException so Flask's built-in handlers (and our 404/413
        # above) still run — only catch genuinely unexpected exceptions here.
        from werkzeug.exceptions import HTTPException

        if isinstance(exc, HTTPException):
            return exc
        current_app.logger.exception("Unhandled view exception")
        return render_template("errors/500.html"), 500

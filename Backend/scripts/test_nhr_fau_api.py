"""
Quick connectivity test for the configured LLM endpoint.

Which provider is used is controlled by a single variable in Backend/.env:
  SELECTED_API=FAU       → NHR@FAU gateway  (needs LLM_API_KEY_FAU)
  SELECTED_API=ACADEMIC  → Academic Cloud   (needs LLM_API_KEY)

Usage:
    # From the Backend/ directory:
    python scripts/test_nhr_fau_api.py
"""

import sys

from app.config import get_settings
from app.llm.client import build_chat_model


def main() -> None:
    cfg = get_settings()

    print(f"SELECTED_API : {cfg.SELECTED_API}")
    if cfg.SELECTED_API.upper() == "FAU":
        print(f"Base URL     : {cfg.LLM_BASE_URL_FAU}")
        print(f"Model        : {cfg.LLM_MODEL_FAU}")
    else:
        print(f"Base URL     : {cfg.LLM_BASE_URL}")
        print(f"Model        : {cfg.LLM_MODEL}")

    print("Sending test request ...")

    try:
        model = build_chat_model()
    except RuntimeError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)

    response = model.invoke(
        "In one sentence, explain what thematic analysis is in qualitative research."
    )

    print("\n✅ API call succeeded!")
    print(f"\nResponse:\n  {response.content}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from collections import defaultdict

from pydantic import ValidationError

from app.generation.contracts import CodebookDraft, GenerationResult

MAX_GENERATED_THEMES = 200
MAX_GENERATED_CODES = 500
MAX_LABEL_LENGTH = 255
MAX_DESCRIPTION_LENGTH = 20_000
MAX_METADATA_JSON_BYTES = 1_000_000


class GenerationDraftValidationError(ValueError):
    """Raised when an algorithm returns a draft the core cannot persist."""


def coerce_generation_result(value: object, *, algorithm_id: str) -> GenerationResult:
    if isinstance(value, GenerationResult):
        return value
    try:
        return GenerationResult.model_validate(value)
    except ValidationError as exc:
        raise GenerationDraftValidationError(
            f"Algorithm '{algorithm_id}' returned an invalid GenerationResult: {exc}"
        ) from exc


def validate_generation_result(result: GenerationResult, *, algorithm_id: str) -> GenerationResult:
    errors: list[str] = []
    _validate_codebook(result.codebook, algorithm_id=algorithm_id, errors=errors)
    _validate_json_payload(
        result.provenance,
        algorithm_id=algorithm_id,
        field_name="provenance",
        errors=errors,
    )
    _validate_action_log(result.action_log, algorithm_id=algorithm_id, errors=errors)
    _validate_json_payload(
        result.token_usage,
        algorithm_id=algorithm_id,
        field_name="token_usage",
        errors=errors,
    )
    if errors:
        raise GenerationDraftValidationError(
            f"Algorithm '{algorithm_id}' returned an invalid codebook draft: " + "; ".join(errors)
        )
    return result


def _validate_codebook(
    draft: CodebookDraft,
    *,
    algorithm_id: str,
    errors: list[str],
) -> None:
    if not draft.themes or not draft.codes:
        errors.append("codebook must contain at least one theme and one code")
    if len(draft.themes) > MAX_GENERATED_THEMES:
        errors.append(f"too many themes ({len(draft.themes)} > {MAX_GENERATED_THEMES})")
    if len(draft.codes) > MAX_GENERATED_CODES:
        errors.append(f"too many codes ({len(draft.codes)} > {MAX_GENERATED_CODES})")

    theme_keys: set[str] = set()
    theme_label_keys: set[str] = set()
    parent_by_theme: dict[str, str | None] = {}
    for index, theme in enumerate(draft.themes, start=1):
        item = f"theme[{index}]"
        key = _clean_key(theme.key)
        label = _clean_label(theme.label)
        description = _clean_description(theme.description)
        parent_key = _clean_key(theme.parent_theme_key)
        if not key:
            errors.append(f"{item} has an empty key")
            continue
        if key in theme_keys:
            errors.append(f"{item} duplicates theme key '{key}'")
        theme_keys.add(key)
        if not label:
            errors.append(f"{item} has an empty label")
        elif len(label) > MAX_LABEL_LENGTH:
            errors.append(f"{item} label exceeds {MAX_LABEL_LENGTH} characters")
        else:
            label_key = _normalized_label_key(label)
            if label_key in theme_label_keys:
                errors.append(f"{item} duplicates normalized theme label '{label}'")
            theme_label_keys.add(label_key)
        if description is not None and len(description) > MAX_DESCRIPTION_LENGTH:
            errors.append(f"{item} description exceeds {MAX_DESCRIPTION_LENGTH} characters")
        if parent_key == key:
            errors.append(f"{item} cannot be its own parent")
        parent_by_theme[key] = parent_key

    for key, parent_key in parent_by_theme.items():
        if parent_key is not None and parent_key not in theme_keys:
            errors.append(f"theme '{key}' references unknown parent theme '{parent_key}'")

    cycle = _find_cycle(parent_by_theme)
    if cycle:
        errors.append(f"theme hierarchy contains a cycle: {' -> '.join(cycle)}")

    code_keys: set[str] = set()
    code_label_keys: set[str] = set()
    for index, code in enumerate(draft.codes, start=1):
        item = f"code[{index}]"
        label = _clean_label(code.label)
        key = _clean_key(code.key) or _normalized_label_key(label)
        description = _clean_description(code.description)
        theme_key = _clean_key(code.theme_key)
        if not key:
            errors.append(f"{item} has an empty key and label")
            continue
        if key in code_keys:
            errors.append(f"{item} duplicates code key '{key}'")
        code_keys.add(key)
        if not label:
            errors.append(f"{item} has an empty label")
        elif len(label) > MAX_LABEL_LENGTH:
            errors.append(f"{item} label exceeds {MAX_LABEL_LENGTH} characters")
        else:
            label_key = _normalized_label_key(label)
            if label_key in code_label_keys:
                errors.append(f"{item} duplicates normalized code label '{label}'")
            code_label_keys.add(label_key)
        if description is not None and len(description) > MAX_DESCRIPTION_LENGTH:
            errors.append(f"{item} description exceeds {MAX_DESCRIPTION_LENGTH} characters")
        if theme_key is not None and theme_key not in theme_keys:
            errors.append(f"{item} references unknown theme '{theme_key}'")

    if errors:
        return

    children_by_parent: dict[str, list[str]] = defaultdict(list)
    for child_key, parent_key in parent_by_theme.items():
        if parent_key is not None:
            children_by_parent[parent_key].append(child_key)

    # Keep an explicit traversal here so validation failures are attributable
    # before SQLAlchemy sees any generated rows.
    visited: set[str] = set()

    def walk(key: str) -> None:
        if key in visited:
            return
        visited.add(key)
        for child_key in children_by_parent.get(key, []):
            walk(child_key)

    for theme_key in theme_keys:
        walk(theme_key)
    if visited != theme_keys:
        missing = sorted(theme_keys - visited)
        errors.append(f"theme hierarchy for algorithm '{algorithm_id}' is disconnected: {missing}")


def _validate_action_log(
    action_log: tuple[object, ...],
    *,
    algorithm_id: str,
    errors: list[str],
) -> None:
    for index, action in enumerate(action_log, start=1):
        if not isinstance(action, dict):
            errors.append(f"action_log[{index}] is not a JSON object")
            continue
        action_name = action.get("action")
        if action_name is not None and (
            not isinstance(action_name, str) or not action_name.strip()
        ):
            errors.append(f"action_log[{index}] has an empty action")
    _validate_json_payload(
        action_log,
        algorithm_id=algorithm_id,
        field_name="action_log",
        errors=errors,
    )


def _validate_json_payload(
    value: object,
    *,
    algorithm_id: str,
    field_name: str,
    errors: list[str],
) -> None:
    try:
        encoded = json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        errors.append(f"{field_name} is not JSON-serializable: {exc}")
        return
    size = len(encoded.encode("utf-8"))
    if size > MAX_METADATA_JSON_BYTES:
        errors.append(
            f"{field_name} from algorithm '{algorithm_id}' is too large "
            f"({size} bytes > {MAX_METADATA_JSON_BYTES})"
        )


def _find_cycle(parent_by_theme: dict[str, str | None]) -> list[str]:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str, stack: list[str]) -> list[str]:
        if key in visited:
            return []
        if key in visiting:
            cycle_start = stack.index(key) if key in stack else 0
            return [*stack[cycle_start:], key]
        visiting.add(key)
        parent_key = parent_by_theme.get(key)
        if parent_key is not None:
            cycle = visit(parent_key, [*stack, key])
            if cycle:
                return cycle
        visiting.remove(key)
        visited.add(key)
        return []

    for key in parent_by_theme:
        cycle = visit(key, [])
        if cycle:
            return cycle
    return []


def _clean_key(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _clean_label(value: str) -> str:
    return " ".join(value.split()).strip()


def _clean_description(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split()).strip()
    return cleaned or None


def _normalized_label_key(value: str) -> str:
    return " ".join(value.casefold().split())

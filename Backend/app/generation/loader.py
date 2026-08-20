from __future__ import annotations

import hashlib
import importlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import cast

from app.generation.contracts import GenerationAlgorithm


class GenerationAlgorithmLoadError(ValueError):
    """Raised when a configured generation algorithm cannot be loaded."""


@dataclass(frozen=True, slots=True)
class LoadedGenerationAlgorithm:
    spec: str
    module_name: str
    factory_name: str
    algorithm: GenerationAlgorithm
    source_sha256: str | None


def load_generation_algorithm(spec: str) -> LoadedGenerationAlgorithm:
    module_name, factory_name = _parse_spec(spec)
    module = _import_module(module_name)
    factory = _get_factory(module, module_name=module_name, factory_name=factory_name)

    try:
        algorithm = factory()
    except Exception as exc:
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm factory '{factory_name}' in module '{module_name}' failed: {exc}"
        ) from exc

    _validate_algorithm_object(algorithm, spec=spec)
    return LoadedGenerationAlgorithm(
        spec=spec,
        module_name=module_name,
        factory_name=factory_name,
        algorithm=cast(GenerationAlgorithm, algorithm),
        source_sha256=_source_sha256(module),
    )


def _parse_spec(spec: str) -> tuple[str, str]:
    if not spec or ":" not in spec:
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm must use 'module.path:factory' format, got {spec!r}."
        )
    module_name, separator, factory_name = spec.partition(":")
    if separator != ":" or not module_name.strip() or not factory_name.strip():
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm must use 'module.path:factory' format, got {spec!r}."
        )
    module_name = module_name.strip()
    factory_name = factory_name.strip()
    if ":" in factory_name:
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm must contain exactly one ':' separator, got {spec!r}."
        )
    if not factory_name.isidentifier():
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm factory name '{factory_name}' is not a valid Python identifier."
        )
    return module_name, factory_name


def _import_module(module_name: str) -> ModuleType:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name or module_name.startswith(f"{exc.name}."):
            raise GenerationAlgorithmLoadError(
                f"Generation algorithm module '{module_name}' could not be imported. "
                "Check GENERATION_ALGORITHM and ensure the module is on PYTHONPATH."
            ) from exc
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm module '{module_name}' was found, but importing it "
            f"failed because dependency '{exc.name}' is missing."
        ) from exc
    except Exception as exc:
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm module '{module_name}' failed during import: {exc}"
        ) from exc


def _get_factory(
    module: ModuleType,
    *,
    module_name: str,
    factory_name: str,
) -> Callable[[], object]:
    try:
        factory = getattr(module, factory_name)
    except AttributeError as exc:
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm module '{module_name}' has no factory '{factory_name}'."
        ) from exc
    if not callable(factory):
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm factory '{factory_name}' in module '{module_name}' is not callable."
        )
    return cast(Callable[[], object], factory)


def _validate_algorithm_object(algorithm: object, *, spec: str) -> None:
    algorithm_id = getattr(algorithm, "algorithm_id", None)
    algorithm_version = getattr(algorithm, "algorithm_version", None)
    if not isinstance(algorithm_id, str) or not algorithm_id.strip():
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm '{spec}' returned an object with an empty algorithm_id."
        )
    if not isinstance(algorithm_version, str) or not algorithm_version.strip():
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm '{spec}' returned an object with an empty algorithm_version."
        )
    requires_llm = getattr(algorithm, "requires_llm", None)
    if not isinstance(requires_llm, bool):
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm '{spec}' must declare boolean requires_llm."
        )
    generate = getattr(algorithm, "generate", None)
    if not callable(generate) or not inspect.iscoroutinefunction(generate):
        raise GenerationAlgorithmLoadError(
            f"Generation algorithm '{spec}' must provide an async callable generate() method."
        )


def _source_sha256(module: ModuleType) -> str | None:
    source_file = inspect.getsourcefile(module)
    if not source_file:
        return None
    path = Path(source_file)
    if not path.is_file():
        return None
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None

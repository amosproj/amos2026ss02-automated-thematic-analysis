from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from loguru import logger

from app.config import Settings, get_settings
from app.schemas.traceable_llm import CodeRelationshipResult
from app.services.remote_embeddings import RemoteEmbeddingClient, cosine_similarity


@dataclass
class CodeCandidate:
    candidate_id: str
    label: str
    description: str | None
    quote_ids: list[str] = field(default_factory=list)

    @property
    def frequency(self) -> int:
        return len(self.quote_ids)


@dataclass
class ConsolidatedCode:
    label: str
    description: str | None
    candidate_ids: list[str]
    quote_ids: list[str]

    @property
    def frequency(self) -> int:
        return len(self.quote_ids)


PairClassifier = Callable[[CodeCandidate, CodeCandidate], Awaitable[CodeRelationshipResult]]
PairBatchClassifier = Callable[
    [list[tuple[int, CodeCandidate, CodeCandidate]]],
    Awaitable[dict[int, CodeRelationshipResult]],
]
ProgressCallback = Callable[[int, int], Awaitable[None]]


async def consolidate_code_candidates(
    candidates: list[CodeCandidate],
    *,
    classifier: PairClassifier,
    batch_classifier: PairBatchClassifier | None = None,
    embedding_client: RemoteEmbeddingClient | None = None,
    settings: Settings | None = None,
    on_pair_progress: ProgressCallback | None = None,
) -> tuple[list[ConsolidatedCode], list[dict[str, object]]]:
    cfg = settings or get_settings()
    if not candidates:
        return [], []

    # Cheap deterministic cleanup first: exact label duplicates do not need
    # embeddings or an LLM relationship call.
    exact_groups = _group_exact_labels(candidates)
    grouped_candidates = [_merge_exact_group(group) for group in exact_groups]
    logger.info(
        "Traceable consolidation started: raw_candidates={}, exact_groups={}",
        len(candidates),
        len(grouped_candidates),
    )
    if len(grouped_candidates) == 1:
        only = grouped_candidates[0]
        return [
            ConsolidatedCode(
                label=only.label,
                description=only.description,
                candidate_ids=[only.candidate_id],
                quote_ids=only.quote_ids,
            )
        ], []

    # Embeddings are used only as a prefilter. The LLM sees a much smaller set
    # of likely-related pairs instead of the full O(n^2) code-pair space.
    embeddings = await (embedding_client or RemoteEmbeddingClient()).embed(
        [_embedding_text(candidate) for candidate in grouped_candidates]
    )
    pair_scores = _candidate_pair_scores(
        embeddings,
        threshold=cfg.CODE_SIMILARITY_THRESHOLD,
        top_k=cfg.CODE_PAIR_TOP_K,
    )
    logger.info(
        "Traceable consolidation embedding prefilter complete: candidates={}, candidate_pairs={}, "
        "similarity_threshold={}, top_k={}",
        len(grouped_candidates),
        len(pair_scores),
        cfg.CODE_SIMILARITY_THRESHOLD,
        cfg.CODE_PAIR_TOP_K,
    )

    equivalent_parent = list(range(len(grouped_candidates)))
    subordinate_edges: set[tuple[int, int]] = set()
    action_log: list[dict[str, object]] = []

    async def classify_pair(
        sequence: int,
        left_index: int,
        right_index: int,
        score: float,
    ) -> tuple[int, int, int, float, CodeRelationshipResult]:
        left = grouped_candidates[left_index]
        right = grouped_candidates[right_index]
        result = await classifier(left, right)
        return sequence, left_index, right_index, score, result

    async def classify_batch(
        batch: list[tuple[int, int, int, float]],
    ) -> list[tuple[int, int, int, float, CodeRelationshipResult]]:
        if batch_classifier is None:
            results: list[tuple[int, int, int, float, CodeRelationshipResult]] = []
            for sequence, left_index, right_index, score in batch:
                results.append(await classify_pair(sequence, left_index, right_index, score))
            return results

        payload = [
            (sequence, grouped_candidates[left_index], grouped_candidates[right_index])
            for sequence, left_index, right_index, _score in batch
        ]
        try:
            batch_results = await batch_classifier(payload)
        except Exception as exc:
            # A single malformed JSON value in a batch should not fail the full
            # analysis. Fall back to pair-level classification; the pair
            # classifier itself has retries and a conservative final fallback.
            logger.warning(
                "Traceable consolidation batch classification failed; falling back to individual pairs: "
                "batch_size={}, error={}",
                len(batch),
                exc,
            )
            results: list[tuple[int, int, int, float, CodeRelationshipResult]] = []
            for sequence, left_index, right_index, score in batch:
                results.append(await classify_pair(sequence, left_index, right_index, score))
            return results
        results = []
        for sequence, left_index, right_index, score in batch:
            result = batch_results.get(sequence)
            if result is None:
                result = await classifier(grouped_candidates[left_index], grouped_candidates[right_index])
            results.append((sequence, left_index, right_index, score, result))
        return results

    concurrency = max(1, cfg.CODE_PAIR_CLASSIFICATION_CONCURRENCY)
    batch_size = max(1, cfg.CODE_PAIR_CLASSIFICATION_BATCH_SIZE)
    indexed_pair_scores = [
        (sequence, left_index, right_index, score)
        for sequence, (left_index, right_index, score) in enumerate(pair_scores)
    ]
    batches = [
        indexed_pair_scores[index : index + batch_size]
        for index in range(0, len(indexed_pair_scores), batch_size)
    ]
    logger.info(
        "Traceable consolidation classifying code pairs: pair_count={}, batch_count={}, batch_size={}, concurrency={}",
        len(pair_scores),
        len(batches),
        batch_size,
        concurrency,
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def classify_batch_limited(
        batch: list[tuple[int, int, int, float]],
    ) -> list[tuple[int, int, int, float, CodeRelationshipResult]]:
        async with semaphore:
            return await classify_batch(batch)

    pair_results: list[tuple[int, int, int, float, CodeRelationshipResult]] = []
    completed_pairs = 0
    tasks = [asyncio.create_task(classify_batch_limited(batch)) for batch in batches]
    if on_pair_progress is not None:
        await on_pair_progress(0, len(pair_scores))
    for task in asyncio.as_completed(tasks):
        batch_results = await task
        pair_results.extend(batch_results)
        completed_pairs += len(batch_results)
        if on_pair_progress is not None:
            await on_pair_progress(completed_pairs, len(pair_scores))
        logger.info(
            "Traceable consolidation pair classification progress: classified_pairs={}, total_pairs={}",
            completed_pairs,
            len(pair_scores),
        )

    # Apply LLM decisions after all calls complete. This keeps graph mutation
    # deterministic while still allowing the slow network calls to run in parallel.
    for _sequence, left_index, right_index, score, result in sorted(pair_results, key=lambda item: item[0]):
        left = grouped_candidates[left_index]
        right = grouped_candidates[right_index]
        action_log.append(
            {
                "action": "classify_code_pair",
                "code_a": left.label,
                "code_b": right.label,
                "embedding_similarity": score,
                "relationship": result.relationship,
                "confidence": result.confidence,
                "reason": result.reason,
            }
        )
        if result.confidence < 0.65:
            continue
        if result.relationship == "equivalent" and result.confidence >= cfg.CODE_EQUIVALENT_MIN_CONFIDENCE:
            # Equivalent codes are merged immediately with union-find.
            preferred, other = _preferred_candidate_pair(grouped_candidates, left_index, right_index)
            _union(equivalent_parent, preferred, other)
        elif (
            result.relationship == "a_subordinate_to_b"
            and result.confidence >= cfg.CODE_SUBORDINATE_MIN_CONFIDENCE
            and score >= cfg.CODE_SUBORDINATE_MIN_SIMILARITY
        ):
            # Subordinate relations are kept as graph edges first. High-frequency
            # child codes may survive as distinct codes later.
            subordinate_edges.add((left_index, right_index))
            action_log.append({"action": "subsumed_code", "source": left.label, "target": right.label})
        elif (
            result.relationship == "b_subordinate_to_a"
            and result.confidence >= cfg.CODE_SUBORDINATE_MIN_CONFIDENCE
            and score >= cfg.CODE_SUBORDINATE_MIN_SIMILARITY
        ):
            subordinate_edges.add((right_index, left_index))
            action_log.append({"action": "subsumed_code", "source": right.label, "target": left.label})

    equivalent_groups_by_root: dict[int, list[CodeCandidate]] = defaultdict(list)
    for index, candidate in enumerate(grouped_candidates):
        equivalent_groups_by_root[_find(equivalent_parent, index)].append(candidate)

    equivalent_roots = sorted(equivalent_groups_by_root)
    root_to_group_index = {root: index for index, root in enumerate(equivalent_roots)}
    group_parent = list(range(len(equivalent_roots)))
    grouped_edges: set[tuple[int, int]] = set()
    # Resolve equivalent groups before applying subordinate relationships.
    for child_index, parent_index in _transitive_edges(subordinate_edges):
        child_root = _find(equivalent_parent, child_index)
        parent_root = _find(equivalent_parent, parent_index)
        if child_root == parent_root:
            continue
        grouped_edges.add((root_to_group_index[child_root], root_to_group_index[parent_root]))

    merged_groups = [_merge_consolidation_group(equivalent_groups_by_root[root]) for root in equivalent_roots]
    child_count_by_parent: dict[int, int] = defaultdict(int)
    parents_by_child: dict[int, set[int]] = defaultdict(set)
    for child_group_index, parent_group_index in grouped_edges:
        child_count_by_parent[parent_group_index] += 1
        parents_by_child[child_group_index].add(parent_group_index)

    for child_group_index, parent_group_indexes in parents_by_child.items():
        child = merged_groups[child_group_index]
        if cfg.TRACEABLE_MIN_CODE_FREQUENCY <= 0:
            continue
        if child.frequency > cfg.TRACEABLE_MIN_CODE_FREQUENCY:
            continue
        # Only low-frequency child codes are folded upward. This approximates
        # the paper's hierarchy cleanup without erasing recurring subdimensions.
        best_parent_index = max(
            parent_group_indexes,
            key=lambda index: _merge_score(merged_groups[index], child_count_by_parent[index]),
        )
        _union(group_parent, best_parent_index, child_group_index)
        action_log.append(
            {
                "action": "subsumed_low_frequency_code",
                "source": child.label,
                "target": merged_groups[best_parent_index].label,
                "frequency": child.frequency,
            }
        )

    final_groups: dict[int, list[ConsolidatedCode]] = defaultdict(list)
    dropped_orphans = 0
    for group_index, code in enumerate(merged_groups):
        if (
            cfg.TRACEABLE_MIN_CODE_FREQUENCY > 0
            and code.frequency <= cfg.TRACEABLE_MIN_CODE_FREQUENCY
            and group_index not in parents_by_child
        ):
            dropped_orphans += 1
            # One-off concepts that are not attached to a broader parent are
            # treated as weak codebook candidates and retained only in the log.
            action_log.append(
                {
                    "action": "dropped_low_frequency_orphan_code",
                    "target": code.label,
                    "frequency": code.frequency,
                }
            )
            continue
        final_groups[_find(group_parent, group_index)].append(code)

    consolidated = [_merge_consolidated_codes(group) for group in final_groups.values()]
    if not consolidated:
        consolidated = merged_groups
    consolidated.sort(key=lambda code: (-code.frequency, code.label.lower()))
    logger.info(
        "Traceable consolidation finished: equivalent_groups={}, subordinate_edges={}, "
        "dropped_orphans={}, final_codes={}, actions={}",
        len(merged_groups),
        len(grouped_edges),
        dropped_orphans,
        len(consolidated),
        len(action_log),
    )
    return consolidated, action_log


def _group_exact_labels(candidates: list[CodeCandidate]) -> list[list[CodeCandidate]]:
    grouped: dict[str, list[CodeCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[_label_key(candidate.label)].append(candidate)
    return list(grouped.values())


def _merge_exact_group(group: list[CodeCandidate]) -> CodeCandidate:
    preferred = max(group, key=lambda candidate: (candidate.frequency, len(candidate.description or "")))
    quote_ids: list[str] = []
    for candidate in group:
        for quote_id in candidate.quote_ids:
            if quote_id not in quote_ids:
                quote_ids.append(quote_id)
    return CodeCandidate(
        candidate_id="|".join(candidate.candidate_id for candidate in group),
        label=preferred.label,
        description=preferred.description,
        quote_ids=quote_ids,
    )


def _merge_consolidation_group(group: list[CodeCandidate]) -> ConsolidatedCode:
    preferred = max(group, key=lambda candidate: (candidate.frequency, len(candidate.description or "")))
    candidate_ids: list[str] = []
    quote_ids: list[str] = []
    for candidate in group:
        candidate_ids.extend(candidate.candidate_id.split("|"))
        for quote_id in candidate.quote_ids:
            if quote_id not in quote_ids:
                quote_ids.append(quote_id)
    return ConsolidatedCode(
        label=preferred.label,
        description=preferred.description,
        candidate_ids=candidate_ids,
        quote_ids=quote_ids,
    )


def _merge_consolidated_codes(group: list[ConsolidatedCode]) -> ConsolidatedCode:
    preferred = max(group, key=lambda code: (code.frequency, len(code.description or "")))
    candidate_ids: list[str] = []
    quote_ids: list[str] = []
    for code in group:
        candidate_ids.extend(code.candidate_ids)
        for quote_id in code.quote_ids:
            if quote_id not in quote_ids:
                quote_ids.append(quote_id)
    return ConsolidatedCode(
        label=preferred.label,
        description=preferred.description,
        candidate_ids=candidate_ids,
        quote_ids=quote_ids,
    )


def _preferred_candidate_pair(candidates: list[CodeCandidate], left_index: int, right_index: int) -> tuple[int, int]:
    left = candidates[left_index]
    right = candidates[right_index]
    if (left.frequency, len(left.description or "")) >= (right.frequency, len(right.description or "")):
        return left_index, right_index
    return right_index, left_index


def _merge_score(code: ConsolidatedCode, child_count: int) -> float:
    return float(code.frequency * 2 + child_count)


def _transitive_edges(edges: set[tuple[int, int]]) -> set[tuple[int, int]]:
    closure = set(edges)
    changed = True
    while changed:
        changed = False
        additions: set[tuple[int, int]] = set()
        for left_child, left_parent in closure:
            for right_child, right_parent in closure:
                if left_parent == right_child and left_child != right_parent:
                    additions.add((left_child, right_parent))
        for edge in additions:
            if edge not in closure:
                closure.add(edge)
                changed = True
    return closure


def _candidate_pair_scores(
    embeddings: list[list[float]],
    *,
    threshold: float,
    top_k: int,
) -> list[tuple[int, int, float]]:
    # Keep only top-k neighbors above threshold for each code. This preserves
    # likely semantic overlaps while controlling LLM pair-classification cost.
    candidates: set[tuple[int, int]] = set()
    scored: list[tuple[int, int, float]] = []
    for left_index, left_vector in enumerate(embeddings):
        local_scores: list[tuple[int, float]] = []
        for right_index, right_vector in enumerate(embeddings):
            if left_index == right_index:
                continue
            score = cosine_similarity(left_vector, right_vector)
            local_scores.append((right_index, score))
        local_scores.sort(key=lambda item: item[1], reverse=True)
        for right_index, score in local_scores[:top_k]:
            if score < threshold:
                continue
            pair = tuple(sorted((left_index, right_index)))
            if pair in candidates:
                continue
            candidates.add(pair)
            scored.append((pair[0], pair[1], score))
    scored.sort(key=lambda item: item[2], reverse=True)
    return scored


def _embedding_text(candidate: CodeCandidate) -> str:
    return f"passage: {candidate.label}. {candidate.description or ''}".strip()


def _label_key(value: str) -> str:
    return " ".join(value.lower().split())


def _find(parent: list[int], index: int) -> int:
    while parent[index] != index:
        parent[index] = parent[parent[index]]
        index = parent[index]
    return index


def _union(parent: list[int], preferred_index: int, other_index: int) -> None:
    preferred_root = _find(parent, preferred_index)
    other_root = _find(parent, other_index)
    if preferred_root != other_root:
        parent[other_root] = preferred_root

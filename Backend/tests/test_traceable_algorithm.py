from __future__ import annotations

from app.schemas.traceable_llm import (
    CodebookReviewAction,
    CodebookReviewResult,
    CodebookSynthesisResult,
    CodeRelationshipResult,
    SynthesizedCode,
    SynthesizedThemeNode,
    SynthesizedThemePath,
)
from app.services.traceable_analysis import TraceableAnalysisService
from app.services.traceable_code_consolidation import (
    CodeCandidate,
    ConsolidatedCode,
    consolidate_code_candidates,
)


class _FakeEmbeddingClient:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0, 0.0] for _ in texts]


async def test_consolidation_subsumes_low_frequency_child_code() -> None:
    candidates = [
        CodeCandidate(
            candidate_id="child",
            label="Manual handoff delay",
            description="Manual handoffs delay a specific part of the workflow.",
            quote_ids=["q-child"],
        ),
        CodeCandidate(
            candidate_id="parent",
            label="Workflow delays",
            description="Workflow delays slow the overall process.",
            quote_ids=["q-parent-1", "q-parent-2"],
        ),
    ]

    async def _classifier(left: CodeCandidate, right: CodeCandidate) -> CodeRelationshipResult:
        assert left.label == "Manual handoff delay"
        assert right.label == "Workflow delays"
        return CodeRelationshipResult(
            relationship="a_subordinate_to_b",
            confidence=0.95,
            reason="Manual handoffs are one source of workflow delays.",
        )

    consolidated, action_log = await consolidate_code_candidates(
        candidates,
        classifier=_classifier,
        embedding_client=_FakeEmbeddingClient(),  # type: ignore[arg-type]
    )

    assert len(consolidated) == 1
    assert consolidated[0].label == "Workflow delays"
    assert set(consolidated[0].quote_ids) == {"q-child", "q-parent-1", "q-parent-2"}
    assert any(action["action"] == "subsumed_low_frequency_code" for action in action_log)


def test_reviewer_actions_revise_and_move_code_paths() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="Workflow Issues"),
                    SynthesizedThemeNode(label="Manual Work"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Manual handoffs slow work",
                code_description="Manual handoffs slow review work.",
                theme_path=["Workflow Issues", "Manual Work"],
            )
        ],
    )
    review = CodebookReviewResult(
        actions=[
            CodebookReviewAction(
                action="revise",
                target="Workflow Issues",
                replacement="Workflow Friction",
                artifact_type="theme",
                reason="More precise label.",
            ),
            CodebookReviewAction(
                action="move",
                target="Manual handoffs slow work",
                new_parent_path=["Workflow Friction", "Coordination Breakdowns"],
                artifact_type="code",
                reason="The code is about coordination.",
            ),
        ]
    )

    refined, action_log = service._apply_review_actions(synthesis, review, round_index=1)

    assert refined.codes[0].theme_path == ["Workflow Friction", "Coordination Breakdowns"]
    assert ("Workflow Friction", "Coordination Breakdowns") in {
        tuple(node.label for node in theme.path) for theme in refined.themes
    }
    assert [action["action"] for action in action_log] == ["revise", "move"]
    assert all(action["applied"] for action in action_log)


def test_provenance_payload_links_theme_to_quote_and_application() -> None:
    quote = type(
        "Quote",
        (),
        {
            "quote_id": "quote-1",
            "document_id": "00000000-0000-0000-0000-000000000001",
            "candidate_id": "candidate-1",
            "code_label": "Manual handoffs slow work",
            "start_char": 5,
            "end_char": 25,
            "quote_match_status": "exact",
        },
    )()
    applied = type(
        "Applied",
        (),
        {
            "document_id": "00000000-0000-0000-0000-000000000001",
            "code_label": "Manual handoffs slow work",
            "quote": "manual handoffs slow",
            "start_char": 5,
            "end_char": 25,
            "quote_match_status": "exact",
        },
    )()
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="Workflow Friction"),
                    SynthesizedThemeNode(label="Coordination Breakdowns"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Manual handoffs slow work",
                code_description="Manual handoffs slow review work.",
                theme_path=["Workflow Friction", "Coordination Breakdowns"],
            )
        ],
    )
    consolidated = [
        ConsolidatedCode(
            label="Manual handoffs slow work",
            description="Manual handoffs slow review work.",
            candidate_ids=["candidate-1"],
            quote_ids=["quote-1"],
        )
    ]

    payload = TraceableAnalysisService._build_provenance_payload(
        quote_evidence=[quote],  # type: ignore[list-item]
        consolidated_codes=consolidated,
        synthesis=synthesis,
        applied_evidence=[applied],  # type: ignore[list-item]
    )
    action_log = TraceableAnalysisService._with_action_ids([
        {"action": "extract_quote_code_pairs", "outputs": {"quote_ids": ["quote-1"]}},
    ])

    assert payload["themes"][0]["subtheme_ids"] == ["subtheme_coordination_breakdowns"]
    assert payload["subthemes"][0]["code_ids"] == ["code_manual_handoffs_slow_work"]
    assert payload["codes"][0]["quote_ids"] == ["quote-1"]
    assert payload["applications"][0]["code_id"] == "code_manual_handoffs_slow_work"
    assert payload["metrics"]["code_reusability"] == 1.0
    assert action_log[0]["action_id"] == "act_0001"

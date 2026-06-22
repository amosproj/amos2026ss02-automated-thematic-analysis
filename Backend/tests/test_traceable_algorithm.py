from __future__ import annotations

from app.config import Settings
from app.schemas.traceable_llm import (
    CodebookMissingConcept,
    CodebookPolishResult,
    CodebookReviewAction,
    CodebookReviewResult,
    CodebookSplitChild,
    CodebookSynthesisResult,
    CodebookQualityEvaluationResult,
    CodeRelationshipResult,
    SynthesizedCode,
    SynthesizedThemeNode,
    SynthesizedThemePath,
)
from app.services.traceable_analysis import TraceableAnalysisService, _AppliedEvidence, _DocumentText, _QuoteEvidence
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
        settings=Settings(
            DATABASE_URL="sqlite+aiosqlite:///:memory:",
            TRACEABLE_MIN_CODE_FREQUENCY=1,
        ),
    )

    assert len(consolidated) == 1
    assert consolidated[0].label == "Workflow delays"
    assert set(consolidated[0].quote_ids) == {"q-child", "q-parent-1", "q-parent-2"}
    assert any(action["action"] == "subsumed_low_frequency_code" for action in action_log)


async def test_consolidation_falls_back_when_batch_relationship_classification_fails() -> None:
    candidates = [
        CodeCandidate(
            candidate_id="a",
            label="AI privacy concern",
            description="Concern that AI systems expose private information.",
            quote_ids=["q-a"],
        ),
        CodeCandidate(
            candidate_id="b",
            label="Fear of AI data leaks",
            description="Fear that AI tools leak personal data.",
            quote_ids=["q-b"],
        ),
    ]

    async def _classifier(left: CodeCandidate, right: CodeCandidate) -> CodeRelationshipResult:
        assert left.label == "AI privacy concern"
        assert right.label == "Fear of AI data leaks"
        return CodeRelationshipResult(
            relationship="equivalent",
            confidence=0.9,
            reason="Both codes describe privacy/data-leak concern.",
        )

    async def _batch_classifier(
        pairs: list[tuple[int, CodeCandidate, CodeCandidate]],
    ) -> dict[int, CodeRelationshipResult]:
        raise ValueError("Invalid json output: malformed confidence")

    consolidated, action_log = await consolidate_code_candidates(
        candidates,
        classifier=_classifier,
        batch_classifier=_batch_classifier,
        embedding_client=_FakeEmbeddingClient(),  # type: ignore[arg-type]
    )

    assert len(consolidated) == 1
    assert set(consolidated[0].quote_ids) == {"q-a", "q-b"}
    assert any(action["action"] == "classify_code_pair" for action in action_log)


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


def test_reviewer_code_merge_combines_source_codes() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Governance"),
                    SynthesizedThemeNode(label="Privacy"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Calls for government data privacy protection",
                code_description="Government should protect personal data.",
                theme_path=["AI Governance", "Privacy"],
            ),
            SynthesizedCode(
                code_label="Policy suggestion: mandatory AI content watermarking",
                code_description="AI content should be disclosed.",
                theme_path=["AI Governance", "Privacy"],
            ),
            SynthesizedCode(
                code_label="Fear of job loss due to technology",
                code_description="Automation may eliminate work.",
                theme_path=["Labor", "Job Security"],
            ),
        ],
    )
    review = CodebookReviewResult(
        actions=[
            CodebookReviewAction(
                action="merge",
                source_labels=[
                    "Calls for government data privacy protection",
                    "Policy suggestion: mandatory AI content watermarking",
                ],
                replacement="Privacy and transparency safeguards",
                new_parent_path=["AI Governance", "Privacy"],
                artifact_type="code",
                reason="Both are transparency/privacy safeguards.",
            )
        ]
    )

    refined, action_log = service._apply_review_actions(synthesis, review, round_index=1)

    assert action_log[0]["applied"] is True
    assert sorted(code.code_label for code in refined.codes) == [
        "Fear of job loss due to technology",
        "Privacy and transparency safeguards",
    ]
    merged = next(code for code in refined.codes if code.code_label == "Privacy and transparency safeguards")
    assert merged.theme_path == ["AI Governance", "Privacy"]


def test_reviewer_code_split_creates_grounded_child_codes() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Governance"),
                    SynthesizedThemeNode(label="Policy Responses"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Policy and governance recommendations for AI",
                code_description="Funding, labeling, company policy, and consumer protection proposals.",
                theme_path=["AI Governance", "Policy Responses"],
            )
        ],
    )
    review = CodebookReviewResult(
        actions=[
            CodebookReviewAction(
                action="split",
                target="Policy and governance recommendations for AI",
                artifact_type="code",
                split_children=[
                    CodebookSplitChild(
                        code_label="AI content labeling and disclosure",
                        code_description="Mandates visible disclosures for AI-generated content.",
                        source_quote_ids=["q-label"],
                    ),
                    CodebookSplitChild(
                        code_label="Company-level AI usage policies",
                        code_description="Calls for workplace policies governing AI use.",
                        source_quote_ids=["q-policy"],
                    ),
                ],
                reason="The parent combines distinct policy mechanisms.",
            )
        ]
    )
    consolidated = [
        ConsolidatedCode(
            label="Policy and governance recommendations for AI",
            description="Funding, labeling, company policy, and consumer protection proposals.",
            candidate_ids=["parent"],
            quote_ids=["q-label", "q-policy"],
        )
    ]

    refined, action_log = service._apply_review_actions(synthesis, review, round_index=1)
    refined_codes = service._apply_code_split_actions_to_consolidated_codes(consolidated, action_log)

    assert action_log[0]["applied"] is True
    assert sorted(code.code_label for code in refined.codes) == [
        "AI content labeling and disclosure",
        "Company-level AI usage policies",
    ]
    assert sorted(code.label for code in refined_codes) == [
        "AI content labeling and disclosure",
        "Company-level AI usage policies",
    ]
    assert {tuple(code.quote_ids) for code in refined_codes} == {("q-label",), ("q-policy",)}


def test_reviewer_split_children_accept_label_alias_from_llm() -> None:
    review = CodebookReviewResult(
        actions=[
            {
                "action": "split",
                "target": "Policy and governance recommendations for AI",
                "artifact_type": "code",
                "split_children": [
                    {
                        "label": "AI content labeling and disclosure",
                        "description": "Mandates visible disclosures for AI-generated content.",
                        "quote_ids": ["q-label"],
                    }
                ],
            }
        ]
    )

    child = review.actions[0].split_children[0]
    assert child.code_label == "AI content labeling and disclosure"
    assert child.code_description == "Mandates visible disclosures for AI-generated content."
    assert child.source_quote_ids == ["q-label"]


def test_review_result_coercion_skips_malformed_actions_without_crashing() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    review = service._coerce_review_result(
        {
            "actions": [
                {
                    "action": "split",
                    "target": "Policy and governance recommendations for AI",
                    "artifact_type": "theme",
                    "split_children": [
                        {
                            "label": "AI content labeling and disclosure",
                            "source_quote_ids": ["q-label"],
                        },
                        {"source_quote_ids": ["q-missing-label"]},
                    ],
                },
                {
                    "action": "unsupported",
                    "target": "Invalid action",
                },
            ]
        }
    )

    assert len(review.actions) == 1
    assert review.actions[0].action == "split"
    assert review.actions[0].artifact_type == "code"
    assert len(review.actions[0].split_children) == 1
    assert review.actions[0].split_children[0].code_label == "AI content labeling and disclosure"


def test_reviewer_applies_code_split_even_when_llm_marks_theme() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Governance"),
                    SynthesizedThemeNode(label="Policy Responses"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Advocates increased AI regulation",
                code_description="General regulation code.",
                theme_path=["AI Governance", "Policy Responses"],
            )
        ],
    )
    review = CodebookReviewResult(
        actions=[
            CodebookReviewAction(
                action="split",
                target="Advocates increased AI regulation",
                artifact_type="theme",
                split_children=[
                    CodebookSplitChild(code_label="Regulation to protect employment", source_quote_ids=["q1"]),
                    CodebookSplitChild(code_label="Regulation to mitigate AI environmental impact", source_quote_ids=["q2"]),
                ],
            )
        ]
    )

    refined, action_log = service._apply_review_actions(synthesis, review, round_index=1)

    assert action_log[0]["applied"] is True
    assert action_log[0]["artifact_type"] == "code"
    assert sorted(code.code_label for code in refined.codes) == [
        "Regulation to mitigate AI environmental impact",
        "Regulation to protect employment",
    ]


def test_heldout_coverage_gaps_become_grounded_codes() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    document_id = "00000000-0000-0000-0000-000000000001"
    codes, evidence, actions = service._ground_coverage_gap_codes(
        coverage_gaps=[
            CodebookMissingConcept(
                label="Environmental impact concerns of AI",
                description="Participant mentions AI server pollution.",
                evidence_quotes=["AI servers cause so much pollution"],
            )
        ],
        evaluation_documents=[
            _DocumentText(
                id=document_id,  # type: ignore[arg-type]
                title="Doc",
                content="AI servers cause so much pollution and that needs regulation.",
            )
        ],
        existing_codes=[],
        round_index=2,
    )

    assert [code.label for code in codes] == ["Environmental impact concerns of AI"]
    assert len(evidence) == 1
    assert evidence[0].quote_match_status == "exact"
    assert evidence[0].document_id == document_id  # type: ignore[comparison-overlap]
    assert actions[0]["action"] == "generate_heldout_gap_code"


def test_reviewer_rejects_broad_cross_theme_code_merge() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(path=[SynthesizedThemeNode(label="Employment"), SynthesizedThemeNode(label="Job Loss")]),
            SynthesizedThemePath(path=[SynthesizedThemeNode(label="Privacy"), SynthesizedThemeNode(label="Data Risk")]),
        ],
        codes=[
            SynthesizedCode(
                code_label="Concern about AI-driven job loss",
                code_description="Participant worries that AI will eliminate work.",
                theme_path=["Employment", "Job Loss"],
            ),
            SynthesizedCode(
                code_label="Fear of personal data misuse",
                code_description="Participant worries AI tools may expose private data.",
                theme_path=["Privacy", "Data Risk"],
            ),
        ],
    )
    review = CodebookReviewResult(
        actions=[
            CodebookReviewAction(
                action="merge",
                source_labels=[
                    "Concern about AI-driven job loss",
                    "Fear of personal data misuse",
                ],
                replacement="AI concerns",
                artifact_type="code",
                reason="Both are concerns about AI.",
            )
        ]
    )

    refined, action_log = service._apply_review_actions(synthesis, review, round_index=1)

    assert [code.code_label for code in refined.codes] == [
        "Concern about AI-driven job loss",
        "Fear of personal data misuse",
    ]
    assert action_log[0]["applied"] is False
    assert "Rejected broad code merge" in action_log[0]["rejected_reason"]


def test_iteration_metrics_include_descriptive_quality_scores() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[SynthesizedThemePath(path=[SynthesizedThemeNode(label="Workflow")])],
        codes=[
            SynthesizedCode(
                code_label="Manual handoffs slow work",
                code_description="Manual handoffs slow review.",
                theme_path=["Workflow"],
            )
        ],
    )
    consolidated = [
        ConsolidatedCode(
            label="Manual handoffs slow work",
            description="Manual handoffs slow review.",
            candidate_ids=["candidate-1"],
            quote_ids=["quote-1"],
        )
    ]
    document_id = "00000000-0000-0000-0000-000000000001"
    metrics = service._compute_iteration_metrics(
        synthesis=synthesis,
        consolidated_codes=consolidated,
        quote_evidence=[],
        evaluation_documents=[
            _DocumentText(
                id=document_id,  # type: ignore[arg-type]
                title="Doc",
                content="manual handoffs slow",
            )
        ],
        evaluation_evidence=[
            _AppliedEvidence(
                document_id=document_id,  # type: ignore[arg-type]
                code_label="Manual handoffs slow work",
                theme_label="Workflow",
                quote="manual handoffs slow",
                start_char=0,
                end_char=20,
                quote_match_status="exact",
                confidence=0.9,
                rationale=None,
            )
        ],
        used_heldout=True,
        quality_evaluation=CodebookQualityEvaluationResult(
            fitness_score=0.88,
            coverage_score=0.77,
        ),
    )

    assert metrics["descriptive_fitness_score"] == 0.88
    assert metrics["descriptive_coverage_score"] == 0.77
    assert metrics["missing_concept_count"] == 0
    assert 0.0 < metrics["composite_score"] <= 1.0


def test_iteration_metrics_penalize_bloated_low_reuse_codebooks() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    codes = [
        SynthesizedCode(
            code_label=f"Single quote code {index}",
            code_description="Narrow one-off code.",
            theme_path=["Theme", "Subtheme"],
        )
        for index in range(66)
    ]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[SynthesizedThemeNode(label="Theme"), SynthesizedThemeNode(label="Subtheme")]
            )
        ],
        codes=codes,
    )
    consolidated = [
        ConsolidatedCode(
            label=code.code_label,
            description=code.code_description,
            candidate_ids=[f"candidate-{index}"],
            quote_ids=[f"quote-{index}"],
        )
        for index, code in enumerate(codes)
    ]
    document_id = "00000000-0000-0000-0000-000000000001"
    metrics = service._compute_iteration_metrics(
        synthesis=synthesis,
        consolidated_codes=consolidated,
        quote_evidence=[object() for _ in range(71)],  # type: ignore[list-item]
        evaluation_documents=[
            _DocumentText(
                id=document_id,  # type: ignore[arg-type]
                title="Doc",
                content="important quote",
            )
        ],
        evaluation_evidence=[
            _AppliedEvidence(
                document_id=document_id,  # type: ignore[arg-type]
                code_label="Single quote code 1",
                theme_label="Subtheme",
                quote="important quote",
                start_char=0,
                end_char=15,
                quote_match_status="exact",
                confidence=0.95,
                rationale=None,
            )
        ],
        used_heldout=True,
        quality_evaluation=CodebookQualityEvaluationResult(
            fitness_score=0.92,
            coverage_score=0.75,
        ),
    )

    assert metrics["target_max_codes"] == 36
    assert metrics["bloat_penalty"] < 0.5
    assert metrics["composite_score"] < 0.4


def test_ensure_synthesis_covers_codes_removes_duplicate_labels() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(path=[SynthesizedThemeNode(label="Theme A")]),
            SynthesizedThemePath(path=[SynthesizedThemeNode(label="Theme B")]),
        ],
        codes=[
            SynthesizedCode(
                code_label="Repeated code",
                code_description="First path.",
                theme_path=["Theme A"],
            ),
            SynthesizedCode(
                code_label="Repeated code",
                code_description="Second path.",
                theme_path=["Theme B"],
            ),
        ],
    )
    consolidated = [
        ConsolidatedCode(
            label="Repeated code",
            description="Canonical description.",
            candidate_ids=["candidate-1"],
            quote_ids=["quote-1"],
        )
    ]

    repaired = service._ensure_synthesis_covers_codes(synthesis, consolidated)

    assert [code.code_label for code in repaired.codes] == ["Repeated code"]


def test_compaction_merges_low_frequency_sibling_codes_toward_target() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI use"),
                    SynthesizedThemeNode(label="Productivity and task automation"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label=f"One-off productivity example {index}",
                code_description="Narrow productivity detail.",
                theme_path=["AI use", "Productivity and task automation"],
            )
            for index in range(12)
        ],
    )
    consolidated = [
        ConsolidatedCode(
            label=code.code_label,
            description=code.code_description,
            candidate_ids=[f"candidate-{index}"],
            quote_ids=[f"quote-{index}"],
        )
        for index, code in enumerate(synthesis.codes)
    ]

    compacted, compacted_codes, actions = service._compact_codebook_before_evaluation(
        synthesis=synthesis,
        consolidated_codes=consolidated,
        target_max=6,
        round_index=1,
    )

    assert len(compacted.codes) <= 6
    assert len(compacted_codes) <= 6
    assert any(action["action"] == "compact_near_duplicate_codes" for action in actions)
    assert sum(len(code.quote_ids) for code in compacted_codes) == 12


def test_final_polish_renames_mechanical_labels_without_changing_evidence_links() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    document_id = "00000000-0000-0000-0000-000000000001"
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="Trust, safety, privacy and regulatory concerns"),
                    SynthesizedThemeNode(label="Grounded Evidence Patterns"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Specific Trust, safety, privacy and regulatory concerns patterns 2",
                code_description="AI server pollution / consumer fraud protection / expert oversight",
                theme_path=["Trust, safety, privacy and regulatory concerns", "Grounded Evidence Patterns"],
            ),
            SynthesizedCode(
                code_label="Community-wide negative attitude toward AI",
                code_description="Participants report broad local skepticism toward AI.",
                theme_path=["Trust, safety, privacy and regulatory concerns", "Grounded Evidence Patterns"],
            ),
        ],
    )
    consolidated = [
        ConsolidatedCode(
            label="Specific Trust, safety, privacy and regulatory concerns patterns 2",
            description="AI server pollution / consumer fraud protection / expert oversight",
            candidate_ids=["c1", "c2"],
            quote_ids=["q1", "q2"],
        ),
        ConsolidatedCode(
            label="Community-wide negative attitude toward AI",
            description="Participants report broad local skepticism toward AI.",
            candidate_ids=["c3"],
            quote_ids=["q3"],
        ),
    ]
    quote_evidence = [
        _QuoteEvidence(
            quote_id="q1",
            document_id=document_id,  # type: ignore[arg-type]
            quote="AI servers cause so much pollution",
            start_char=None,
            end_char=None,
            quote_match_status="exact",
            candidate_id="c1",
            code_label="Specific Trust, safety, privacy and regulatory concerns patterns 2",
            code_description="AI server pollution / consumer fraud protection / expert oversight",
            confidence=0.9,
            rationale=None,
        )
    ]
    polish = CodebookPolishResult(
        codes=[
            {
                "original_label": "Specific Trust, safety, privacy and regulatory concerns patterns 2",
                "polished_label": "AI governance risks and safeguards",
                "polished_description": "Concerns about AI harms and safeguards, including environmental impact, consumer protection, and oversight.",
            }
        ],
        themes=[
            {
                "original_label": "Grounded Evidence Patterns",
                "polished_label": "AI governance concerns",
                "polished_description": "Evidence about AI risks, safeguards, and public trust.",
            }
        ],
    )

    polished, polished_codes, polished_evidence, actions = service._apply_codebook_polish(
        synthesis=synthesis,
        consolidated_codes=consolidated,
        quote_evidence=quote_evidence,  # type: ignore[list-item]
        polish=polish,
    )

    assert len(polished.codes) == 2
    assert polished.codes[0].code_label == "AI governance risks and safeguards"
    assert polished.codes[0].theme_path[-1] == "AI governance concerns"
    assert polished_codes[0].label == "AI governance risks and safeguards"
    assert polished_codes[0].quote_ids == ["q1", "q2"]
    assert polished_evidence[0].code_label == "AI governance risks and safeguards"
    assert any(action["artifact_type"] == "code" for action in actions)
    assert any(action["artifact_type"] == "subtheme" for action in actions)


def test_final_polish_rejects_duplicate_code_label_merge() -> None:
    service = TraceableAnalysisService(session=None)  # type: ignore[arg-type]
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Work"),
                    SynthesizedThemeNode(label="Skills"),
                ]
            )
        ],
        codes=[
            SynthesizedCode(
                code_label="Seeks skill beyond AI capabilities",
                code_description=None,
                theme_path=["AI Work", "Skills"],
            ),
            SynthesizedCode(
                code_label="Community-wide negative attitude toward AI",
                code_description=None,
                theme_path=["AI Work", "Skills"],
            ),
        ],
    )
    consolidated = [
        ConsolidatedCode(label=code.code_label, description=None, candidate_ids=[code.code_label], quote_ids=[])
        for code in synthesis.codes
    ]
    polish = CodebookPolishResult(
        codes=[
            {
                "original_label": "Seeks skill beyond AI capabilities",
                "polished_label": "Community-wide negative attitude toward AI",
            }
        ]
    )

    polished, polished_codes, _polished_evidence, _actions = service._apply_codebook_polish(
        synthesis=synthesis,
        consolidated_codes=consolidated,
        quote_evidence=[],
        polish=polish,
    )

    assert [code.code_label for code in polished.codes] == [
        "Seeks skill beyond AI capabilities",
        "Community-wide negative attitude toward AI",
    ]
    assert [code.label for code in polished_codes] == [
        "Seeks skill beyond AI capabilities",
        "Community-wide negative attitude toward AI",
    ]


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


def test_provenance_keeps_multiple_subthemes_for_same_root_theme() -> None:
    synthesis = CodebookSynthesisResult(
        themes=[
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Governance"),
                    SynthesizedThemeNode(label="Privacy"),
                ]
            ),
            SynthesizedThemePath(
                path=[
                    SynthesizedThemeNode(label="AI Governance"),
                    SynthesizedThemeNode(label="Employment Protection"),
                ]
            ),
        ],
        codes=[
            SynthesizedCode(
                code_label="Privacy safeguards",
                code_description=None,
                theme_path=["AI Governance", "Privacy"],
            ),
            SynthesizedCode(
                code_label="Job protection rules",
                code_description=None,
                theme_path=["AI Governance", "Employment Protection"],
            ),
        ],
    )
    consolidated = [
        ConsolidatedCode(label="Privacy safeguards", description=None, candidate_ids=["c1"], quote_ids=["q1"]),
        ConsolidatedCode(label="Job protection rules", description=None, candidate_ids=["c2"], quote_ids=["q2"]),
    ]

    payload = TraceableAnalysisService._build_provenance_payload(
        quote_evidence=[],  # type: ignore[list-item]
        consolidated_codes=consolidated,
        synthesis=synthesis,
        applied_evidence=[],  # type: ignore[list-item]
    )

    assert len(payload["themes"]) == 1
    assert set(payload["themes"][0]["subtheme_ids"]) == {
        "subtheme_privacy",
        "subtheme_employment_protection",
    }

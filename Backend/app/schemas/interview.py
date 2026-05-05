import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from app.schemas.common import BaseSchema

EventType = Literal["chatbot_response", "human_response"]


class InterviewMessage(BaseSchema):
    timestamp: float
    event_type: EventType
    duration_seconds: float = Field(ge=0)
    message_index: int = Field(ge=0)
    message_length_chars: int = Field(ge=0)
    message_content: str
    username: str


class InterviewTranscript(BaseSchema):
    messages: list[InterviewMessage]

    @classmethod
    def from_jsonl(cls, path: Path) -> "InterviewTranscript":
        messages: list[InterviewMessage] = []
        with path.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                messages.append(InterviewMessage.model_validate(json.loads(line)))
        messages.sort(key=lambda m: m.message_index)
        return cls(messages=messages)

    def to_dialog_text(self) -> str:
        # Render the transcript as a plain Interviewer/Participant dialog. --> TODO: Only use "Participant" section for the LLM query?
        # strips the per-turn metadata and keeps only speaker + content.

        lines: list[str] = []
        for msg in self.messages:
            speaker = "Interviewer" if msg.event_type == "chatbot_response" else "Participant"
            lines.append(f"{speaker}: {msg.message_content.strip()}")
        return "\n\n".join(lines)

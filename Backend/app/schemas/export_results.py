from __future__ import annotations

from enum import Enum


class RunExportFormat(str, Enum):
    THEME_BASED = "theme-based"
    PARTICIPANT_BASED = "participant-based"


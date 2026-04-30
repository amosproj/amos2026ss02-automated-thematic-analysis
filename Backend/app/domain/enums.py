from enum import StrEnum


class ActorType(StrEnum):
    LLM = "llm"
    HUMAN = "human"
    SYSTEM = "system"


class CodebookStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    FROZEN = "frozen"
    ARCHIVED = "archived"


class NodeStatus(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    MERGED = "merged"
    DEPRECATED = "deprecated"
    DELETED = "deleted"


class RelationshipStatus(StrEnum):
    ACTIVE = "active"
    REMOVED = "removed"


class CodeRelationshipType(StrEnum):
    SUBORDINATE_TO = "subordinate_to"
    EQUIVALENT_TO = "equivalent_to"
    ORTHOGONAL_TO = "orthogonal_to"


class CodebookCodeRelationshipType(StrEnum):
    CONTAINS = "contains"


class ThemeLevel(StrEnum):
    THEME = "theme"
    SUBTHEME = "subtheme"


class ThemeRelationshipType(StrEnum):
    CHILD_OF = "child_of"
    EQUIVALENT_TO = "equivalent_to"
    RELATED_TO = "related_to"


class CodeThemeRelationshipType(StrEnum):
    MEMBER_OF = "member_of"
    SUPPORTS = "supports"


class CodebookThemeRelationshipType(StrEnum):
    CONTAINS = "contains"


class IngestionRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentStatus(StrEnum):
    ACTIVE = "active"
    DUPLICATE = "duplicate"
    EMPTY = "empty"
    REJECTED = "rejected"


class SourceType(StrEnum):
    MANUAL = "manual"
    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"
    TEXT = "text"
    UPLOAD = "upload"

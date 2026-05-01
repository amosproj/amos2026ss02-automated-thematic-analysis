from dataclasses import dataclass


@dataclass(frozen=True)
class ChunkSpan:
    """One output chunk from the splitter. start_word/end_word are word-level indices into the original text (end exclusive)."""

    chunk_index: int
    text: str
    start_word: int
    end_word: int  # exclusive

def chunk_text_by_words(
    text: str,
    chunk_size_words: int = 2048,
    overlap_words: int = 200,
) -> list[ChunkSpan]:
    """Split text into overlapping word-window chunks.

    Consecutive chunks share overlap_words words of context. stride = chunk_size_words - overlap_words.
    Returns an empty list for empty input.
    """
    if chunk_size_words <= 0:
        raise ValueError(f"chunk_size_words must be > 0, got {chunk_size_words}")
    if overlap_words < 0:
        raise ValueError(f"overlap_words must be >= 0, got {overlap_words}")
    if overlap_words >= chunk_size_words:
        raise ValueError(
            f"overlap_words ({overlap_words}) must be < chunk_size_words ({chunk_size_words})"
        )

    words = text.split()
    if not words:
        return []

    stride = chunk_size_words - overlap_words
    chunks: list[ChunkSpan] = []
    chunk_index = 0
    start = 0

    while start < len(words):
        end = min(start + chunk_size_words, len(words))
        chunks.append(
            ChunkSpan(
                chunk_index=chunk_index,
                text=" ".join(words[start:end]),
                start_word=start,
                end_word=end,
            )
        )
        if end == len(words):
            break
        start += stride
        chunk_index += 1

    return chunks

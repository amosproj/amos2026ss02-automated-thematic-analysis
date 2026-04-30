import pytest

from app.services.text_chunking import (
    ChunkSpan,
    chunk_text_by_words,
    count_words,
    normalize_text_for_hash,
    sha256_text,
)


# ---------------------------------------------------------------------------
# count_words
# ---------------------------------------------------------------------------


def test_count_words_basic():
    assert count_words("hello world") == 2


def test_count_words_extra_whitespace():
    assert count_words("  hello   world  ") == 2


def test_count_words_empty():
    assert count_words("") == 0


def test_count_words_whitespace_only():
    assert count_words("   ") == 0


# ---------------------------------------------------------------------------
# normalize_text_for_hash / sha256_text
# ---------------------------------------------------------------------------


def test_normalize_collapses_whitespace():
    assert normalize_text_for_hash("hello   world\n\t!") == "hello world !"


def test_sha256_same_text_same_hash():
    h1 = sha256_text("hello world")
    h2 = sha256_text("hello  world")  # extra space is normalized away
    assert h1 == h2


def test_sha256_different_text_different_hash():
    assert sha256_text("hello world") != sha256_text("hello earth")


def test_sha256_returns_hex_string():
    h = sha256_text("test")
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# chunk_text_by_words — validation
# ---------------------------------------------------------------------------


def test_chunk_raises_on_zero_size():
    with pytest.raises(ValueError, match="chunk_size_words"):
        chunk_text_by_words("hello world", chunk_size_words=0)


def test_chunk_raises_on_negative_size():
    with pytest.raises(ValueError):
        chunk_text_by_words("hello world", chunk_size_words=-1)


def test_chunk_raises_on_negative_overlap():
    with pytest.raises(ValueError, match="overlap_words"):
        chunk_text_by_words("hello world", chunk_size_words=5, overlap_words=-1)


def test_chunk_raises_when_overlap_equals_size():
    with pytest.raises(ValueError, match="overlap_words"):
        chunk_text_by_words("hello world", chunk_size_words=5, overlap_words=5)


def test_chunk_raises_when_overlap_exceeds_size():
    with pytest.raises(ValueError):
        chunk_text_by_words("hello world", chunk_size_words=5, overlap_words=6)


# ---------------------------------------------------------------------------
# chunk_text_by_words — empty / short text
# ---------------------------------------------------------------------------


def test_chunk_empty_text_returns_empty_list():
    assert chunk_text_by_words("") == []


def test_chunk_whitespace_only_returns_empty_list():
    assert chunk_text_by_words("   ") == []


def test_chunk_short_text_returns_one_chunk():
    words = "one two three"
    chunks = chunk_text_by_words(words, chunk_size_words=10, overlap_words=2)
    assert len(chunks) == 1
    assert chunks[0].chunk_index == 0
    assert chunks[0].start_word == 0
    assert chunks[0].end_word == 3
    assert chunks[0].text == words


def test_chunk_exactly_chunk_size_returns_one_chunk():
    words = " ".join(str(i) for i in range(5))
    chunks = chunk_text_by_words(words, chunk_size_words=5, overlap_words=1)
    assert len(chunks) == 1
    assert chunks[0].end_word == 5


# ---------------------------------------------------------------------------
# chunk_text_by_words — overlapping windows
# ---------------------------------------------------------------------------


def test_chunk_produces_correct_windows():
    # 10 words, chunk_size=5, overlap=2 → stride=3
    # chunk 0: [0..5), chunk 1: [3..8), chunk 2: [6..10)
    text = " ".join(str(i) for i in range(10))
    chunks = chunk_text_by_words(text, chunk_size_words=5, overlap_words=2)

    assert len(chunks) == 3

    assert chunks[0].chunk_index == 0
    assert chunks[0].start_word == 0
    assert chunks[0].end_word == 5

    assert chunks[1].chunk_index == 1
    assert chunks[1].start_word == 3
    assert chunks[1].end_word == 8

    assert chunks[2].chunk_index == 2
    assert chunks[2].start_word == 6
    assert chunks[2].end_word == 10


def test_second_chunk_starts_at_stride():
    text = " ".join(str(i) for i in range(20))
    chunk_size = 6
    overlap = 2
    stride = chunk_size - overlap
    chunks = chunk_text_by_words(text, chunk_size_words=chunk_size, overlap_words=overlap)
    assert chunks[1].start_word == stride


def test_chunk_text_content_matches_words():
    text = "alpha beta gamma delta epsilon"
    chunks = chunk_text_by_words(text, chunk_size_words=3, overlap_words=1)
    words = text.split()
    for c in chunks:
        expected = " ".join(words[c.start_word : c.end_word])
        assert c.text == expected


def test_chunk_index_is_sequential():
    text = " ".join(str(i) for i in range(15))
    chunks = chunk_text_by_words(text, chunk_size_words=5, overlap_words=1)
    for i, c in enumerate(chunks):
        assert c.chunk_index == i


def test_no_overlap_chunks_are_contiguous():
    text = " ".join(str(i) for i in range(9))
    chunks = chunk_text_by_words(text, chunk_size_words=3, overlap_words=0)
    assert len(chunks) == 3
    assert chunks[0].end_word == chunks[1].start_word
    assert chunks[1].end_word == chunks[2].start_word


def test_last_chunk_covers_remainder():
    # 11 words, chunk_size=5, overlap=1 → stride=4
    # [0..5), [4..9), [8..11)
    text = " ".join(str(i) for i in range(11))
    chunks = chunk_text_by_words(text, chunk_size_words=5, overlap_words=1)
    assert chunks[-1].end_word == 11

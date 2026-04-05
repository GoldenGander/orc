from __future__ import annotations

import io

import pytest

from orchestrator.server.tee_logger import TeeStream


def _make_tee() -> tuple[TeeStream, io.StringIO, list[str]]:
    buf = io.StringIO()
    lines: list[str] = []
    tee = TeeStream(buf, lines.append)
    return tee, buf, lines


# ---------------------------------------------------------------------------
# write — line splitting
# ---------------------------------------------------------------------------


def test_single_complete_line() -> None:
    tee, _, lines = _make_tee()
    tee.write("hello\n")
    assert lines == ["hello"]


def test_two_lines_in_one_write() -> None:
    tee, _, lines = _make_tee()
    tee.write("a\nb\n")
    assert lines == ["a", "b"]


def test_partial_line_accumulates() -> None:
    tee, _, lines = _make_tee()
    tee.write("hel")
    assert lines == []
    tee.write("lo\n")
    assert lines == ["hello"]


def test_multi_chunk_partial() -> None:
    tee, _, lines = _make_tee()
    tee.write("a\nbc")
    assert lines == ["a"]
    tee.write("d\n")
    assert lines == ["a", "bcd"]


def test_mixed_complete_and_partial() -> None:
    tee, _, lines = _make_tee()
    tee.write("line1\nline2\npar")
    assert lines == ["line1", "line2"]
    tee.write("tial\n")
    assert lines == ["line1", "line2", "partial"]


# ---------------------------------------------------------------------------
# close — flush partial remainder
# ---------------------------------------------------------------------------


def test_close_flushes_partial_no_newline() -> None:
    tee, _, lines = _make_tee()
    tee.write("no newline")
    assert lines == []
    tee.close()
    assert lines == ["no newline"]


def test_close_no_partial_does_not_emit_empty_line() -> None:
    tee, _, lines = _make_tee()
    tee.write("complete\n")
    tee.close()
    assert lines == ["complete"]


# ---------------------------------------------------------------------------
# underlying stream receives full text
# ---------------------------------------------------------------------------


def test_underlying_stream_receives_all_text() -> None:
    tee, buf, _ = _make_tee()
    tee.write("hello\n")
    tee.write("wor")
    tee.write("ld\n")
    assert buf.getvalue() == "hello\nworld\n"


# ---------------------------------------------------------------------------
# fileno raises UnsupportedOperation
# ---------------------------------------------------------------------------


def test_fileno_raises_unsupported_operation() -> None:
    tee, _, _ = _make_tee()
    with pytest.raises(io.UnsupportedOperation):
        tee.fileno()


# ---------------------------------------------------------------------------
# write return value
# ---------------------------------------------------------------------------


def test_write_returns_length() -> None:
    tee, _, _ = _make_tee()
    assert tee.write("abc\n") == 4
    assert tee.write("xy") == 2

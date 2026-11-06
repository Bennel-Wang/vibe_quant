"""westock CLI 输出解析：Markdown 表格 → 结构化 JSON。"""
from __future__ import annotations

import json
import re

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
BOLD_RE = re.compile(r"^\*\*(.+?)\*\*\s*(.*)$")
NUM_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$")
EMPTY_CELLS = {"", "-", "--", "—", "N/A", "n/a", "null", "None", "nil"}


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)


def _coerce(cell: str):
    s = cell.strip()
    if s in EMPTY_CELLS:
        return None
    if NUM_RE.match(s):
        try:
            return int(s) if "." not in s and "e" not in s.lower() else float(s)
        except ValueError:
            return s
    if s[:1] in "{[":
        # 宏观指标等命令把 JSON 对象嵌在单元格里（如 macro indicator）
        try:
            return json.loads(s)
        except ValueError:
            return s
    return s


def _is_table_line(line: str) -> bool:
    s = line.strip()
    return s.startswith("|") and s.endswith("|") and s.count("|") >= 3


def _is_separator(line: str) -> bool:
    if not _is_table_line(line):
        return False
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    return all(re.fullmatch(r":?-{2,}:?", c) for c in cells) and bool(cells)


def _split_row(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def parse_output(text: str) -> dict:
    """解析 westock 输出为 {"tables": [...], "text": str|None}。

    tables: [{"title", "meta", "columns", "rows"}]，rows 为 dict 列表，数值自动转数字。
    无表格的输出（如 macro list）原样放入 text。
    """
    text = strip_ansi(text).replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    tables: list[dict] = []
    text_parts: list[str] = []
    pending_title: str | None = None
    pending_meta: str | None = None

    i, n = 0, len(lines)
    while i < n:
        line = lines[i].rstrip()

        m = BOLD_RE.match(line.strip())
        if m:
            # **标题** 后允许空行再接表格 → 作为表格标题；否则按正文保留
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            if j + 1 < n and _is_table_line(lines[j]) and _is_separator(lines[j + 1]):
                pending_title = m.group(1).strip()
                meta = m.group(2).strip()
                if meta.startswith("—"):
                    meta = meta[1:].strip()
                pending_meta = meta or None
                i = j
                continue
            text_parts.append(line.strip())
            i += 1
            continue

        if _is_table_line(line) and i + 1 < n and _is_separator(lines[i + 1]):
            cols = _split_row(line)
            i += 2
            rows = []
            while i < n and _is_table_line(lines[i]):
                cells = _split_row(lines[i])
                rows.append(
                    {
                        cols[j]: _coerce(cells[j]) if j < len(cells) else None
                        for j in range(len(cols))
                    }
                )
                i += 1
            tables.append(
                {
                    "title": pending_title,
                    "meta": pending_meta,
                    "columns": cols,
                    "rows": rows,
                }
            )
            pending_title = pending_meta = None
            continue

        if line.strip():
            text_parts.append(line)
        i += 1

    return {"tables": tables, "text": "\n".join(text_parts).strip() or None}

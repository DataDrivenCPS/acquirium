"""Notebook/terminal-friendly rendering for apps API responses.

:class:`AppsResponse` is a plain dict — same keys, values, and equality as
the raw JSON — that only adds a tabular ``__repr__`` (terminal) and
``_repr_html_`` (Jupyter). Columns are whatever keys the server returns, so
new response fields show up without client changes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any

# Keys holding row lists that render as their own table, in display order.
_SECTIONS = ("apps", "runs")

_STATUS_COLORS = {
    "running": "#1565c0",
    "done": "#2e7d32",
    "ready": "#2e7d32",
    "stopped": "#757575",
    "pending": "#757575",
    "failed": "#c62828",
}

_CELL_STYLE = (
    "padding:3px 14px 3px 0;text-align:left;"
    "border-bottom:1px solid rgba(128,128,128,.25)"
)


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _age(dt: datetime) -> str:
    seconds = max(0, int((datetime.now(dt.tzinfo or timezone.utc) - dt).total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"


def _fmt(key: str, value: Any) -> str:
    """Plain-text cell."""
    if value is None or value == "":
        return "—"
    if key.endswith("_at"):
        dt = _parse_dt(value)
        if dt is not None:
            return f"{dt:%Y-%m-%d %H:%M:%S} ({_age(dt)})"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value) or "—"
    return str(value)


def _fmt_html(key: str, value: Any) -> str:
    text = _fmt(key, value)
    if isinstance(value, bool):
        dot, color = ("●", "#2e7d32") if value else ("○", "#9e9e9e")
        return f'<span style="color:{color}">{dot}</span> {escape(text)}'
    if key in ("status", "build"):
        # Failure statuses arrive as "failed: <error>"; color by the prefix.
        color = _STATUS_COLORS.get(str(value).split(":")[0], "inherit")
        return f'<span style="color:{color};font-weight:600">{escape(text)}</span>'
    return escape(text)


def _columns(rows: list[dict]) -> list[str]:
    cols: list[str] = []
    for row in rows:
        for key in row:
            if key not in cols:
                cols.append(key)
    return cols


def _text_table(rows: list[dict]) -> str:
    cols = _columns(rows)
    cells = [[_fmt(c, row.get(c)) for c in cols] for row in rows]
    widths = [
        max(len(c), max((len(row[i]) for row in cells), default=0))
        for i, c in enumerate(cols)
    ]
    lines = ["  ".join(c.ljust(w) for c, w in zip(cols, widths)).rstrip()]
    for row in cells:
        lines.append("  ".join(v.ljust(w) for v, w in zip(row, widths)).rstrip())
    return "\n".join(lines)


def _html_table(rows: list[dict]) -> str:
    cols = _columns(rows)
    head = "".join(
        f'<th style="{_CELL_STYLE};opacity:.6;font-weight:600">{escape(c)}</th>' for c in cols
    )
    body = "".join(
        "<tr>" + "".join(f'<td style="{_CELL_STYLE}">{_fmt_html(c, row.get(c))}</td>' for c in cols) + "</tr>"
        for row in rows
    )
    return (
        '<table style="border-collapse:collapse;font-size:.9em">'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
    )


class AppsResponse(dict):
    """Apps API response with tabular terminal/notebook rendering."""

    def _sections(self) -> list[tuple[str | None, list[dict]]]:
        """Split into (label, rows) chunks: scalar fields first, then each
        row-list section (apps, runs). Scalars render as a one-row table."""
        scalars = {k: v for k, v in self.items() if k != "ok" and k not in _SECTIONS}
        out: list[tuple[str | None, list[dict]]] = []
        if scalars:
            out.append((None, [scalars]))
        for key in _SECTIONS:
            if key in self:
                out.append((key, self[key]))
        return out

    def __repr__(self) -> str:
        parts = []
        for label, rows in self._sections():
            if label is not None:
                parts.append(f"{len(rows)} {label[:-1]}(s)")
            if rows:
                parts.append(_text_table(rows))
        return "\n".join(parts) if parts else "(empty response)"

    def _repr_html_(self) -> str:
        parts = []
        for label, rows in self._sections():
            if label is not None:
                parts.append(
                    f'<div style="opacity:.6;font-size:.85em;margin:6px 0 2px">'
                    f"{len(rows)} {escape(label[:-1])}(s)</div>"
                )
            if rows:
                parts.append(_html_table(rows))
        return "".join(parts) if parts else "(empty response)"

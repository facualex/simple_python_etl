"""
Utility functions for SCHEMA validation
"""

from pandera.errors import SchemaError, SchemaErrors


class SchemaValidationFailed(Exception):
    """Pandera failed validation; message is a short summary (see ``_summarize_pandera_exception``)."""


def _summarize_schema_error_components(schema_errors: list) -> list[str]:
    """Summarize each Pandera ``SchemaError``"""
    lines: list[str] = []
    for i, err in enumerate(schema_errors, start=1):
        col = getattr(err, "column_name", None)
        reason = getattr(err, "reason_code", None)
        label = repr(col) if col is not None else "(non-column / dataframe-level)"
        lines.append(f"  [{i}] column={label} reason={reason}")
        fc = getattr(err, "failure_cases", None)
        if fc is not None and hasattr(fc, "shape") and fc.shape[0] > 0:
            lines.append(f"       failing rows in failure_cases: {fc.shape[0]}")
            if "failure_case" in getattr(fc, "columns", []):
                vc = fc["failure_case"].value_counts(dropna=False).head(8)
                lines.append(
                    "       invalid value counts (top 8):\n       "
                    + vc.to_string().replace("\n", "\n       ")
                )
            lines.append(
                "       sample:\n       "
                + fc.head(6).to_string().replace("\n", "\n       ")
            )
        else:
            msg = getattr(err, "message", None) or (
                err.args[0] if getattr(err, "args", None) else ""
            )
            msg_s = str(msg)[:800]
            lines.append(
                "       " + msg_s + (" ... [truncated]" if len(str(msg)) > 800 else "")
            )
    return lines


def _summarize_pandera_exception(exc: BaseException) -> str:
    """Return a bounded, readable summary of a Pandera error.

    Pandera's default ``str(SchemaError)`` can list *every* failing value, which is
    megabytes on large dataframes and looks like an endless stream in logs/terminal.
    """
    if isinstance(exc, SchemaErrors):
        lines = [
            f"Pandera reported {len(exc.schema_errors)} schema error(s).",
            f"error_counts: {exc.error_counts!r}",
        ]
        fc = getattr(exc, "failure_cases", None)
        if fc is not None and hasattr(fc, "shape") and fc.shape[0] > 0:
            lines.append(f"failure_cases rows (total): {fc.shape[0]}")
            if "column" in fc.columns:
                present = fc["column"].notna()
                col_names = sorted(
                    fc.loc[present, "column"].astype(str).unique().tolist()
                )
                missing_col_mask = ~present
                labels: list[str] = list(col_names)
                if missing_col_mask.any():
                    labels.append("(no column label - dataframe/other)")
                lines.append(
                    f"Columns with errors ({len(labels)}): {', '.join(labels)}"
                )
                for col_key in labels:
                    if col_key == "(no column label - dataframe/other)":
                        sub = fc.loc[missing_col_mask]
                        header = "[dataframe-level / unparsed column]"
                    else:
                        sub = fc.loc[present & (fc["column"].astype(str) == col_key)]
                        header = repr(col_key)
                    lines.append(f"--- {header}: {len(sub)} row(s) in failure_cases")
                    if "check" in sub.columns:
                        chk = sub["check"].dropna().astype(str).unique().tolist()
                        lines.append(
                            "    checks: "
                            + ", ".join(chk[:6])
                            + (" ..." if len(chk) > 6 else "")
                        )
                    if "failure_case" in sub.columns:
                        vc = sub["failure_case"].value_counts(dropna=False).head(8)
                        lines.append(
                            "    invalid value counts (top 8):\n    "
                            + vc.to_string().replace("\n", "\n    ")
                        )
                    lines.append(
                        "    sample:\n    "
                        + sub.head(6).to_string().replace("\n", "\n    ")
                    )
            else:
                lines.append("Per-error breakdown:")
                lines.extend(_summarize_schema_error_components(exc.schema_errors))
        else:
            lines.append("Per-error breakdown:")
            lines.extend(_summarize_schema_error_components(exc.schema_errors))
        return "\n".join(lines)

    if isinstance(exc, SchemaErrors):
        lines = []
        col = getattr(exc, "column_name", None)
        if col:
            lines.append(f"Column {col!r} failed validation.")
        fc = getattr(exc, "failure_cases", None)
        if fc is not None and hasattr(fc, "shape") and fc.shape[0] > 0:
            lines.append(f"Failing rows: {fc.shape[0]}")
            if "failure_case" in fc.columns:
                vc = fc["failure_case"].value_counts().head(10)
                lines.append("Invalid value counts (top 10):\n" + vc.to_string())
            lines.append("Sample:\n" + fc.head(8).to_string())
        else:
            msg = str(exc)
            lines.append(msg[:2000] + (" ... [truncated]" if len(msg) > 2000 else ""))
        return "\n".join(lines)

    msg = str(exc)
    return msg[:2000] + (" ... [truncated]" if len(msg) > 2000 else "")

from __future__ import annotations

"""Tabular transformation engine used by /pipelines/apply."""

import io
import json
from typing import Any, Dict, Iterable, Literal, Optional, Tuple

import pandas as pd


# NOTE: pandas is used for convenience and correctness across mixed types.

DatasetFormat = Literal["csv", "json"]


def _df_from_bytes(data: bytes, fmt: DatasetFormat) -> pd.DataFrame:
    """Deserialize incoming bytes into a pandas DataFrame."""
    if fmt == "json":
        raw = json.loads(data.decode("utf-8"))
        if isinstance(raw, dict) and "records" in raw:
            raw = raw["records"]
        if not isinstance(raw, list):
            raise ValueError("JSON dataset must be a list of objects (or {records:[...]})")
        return pd.DataFrame(raw)
    else:
        # Robust CSV parsing via pandas; treat everything as string initially.
        return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def _df_to_bytes(df: pd.DataFrame, fmt: DatasetFormat) -> bytes:
    """Serialize DataFrame back to bytes in requested format."""
    if fmt == "json":
        records = df.to_dict(orient="records")
        return json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8")
    else:
        out = io.StringIO()
        df.to_csv(out, index=False)
        return out.getvalue().encode("utf-8")


def transform_deduplicate(df: pd.DataFrame, parameters: Dict[str, Any]) -> pd.DataFrame:
    """Remove duplicate rows, optionally scoped to selected columns."""
    subset = parameters.get("subset")
    keep = parameters.get("keep", "first")
    if subset is not None and not isinstance(subset, list):
        raise ValueError("deduplicate.subset must be a list of column names")
    if keep not in ("first", "last", False):
        raise ValueError("deduplicate.keep must be 'first', 'last', or false")
    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)


def transform_null_handling(df: pd.DataFrame, parameters: Dict[str, Any]) -> pd.DataFrame:
    """Drop or fill null-like values on selected columns."""
    strategy = parameters.get("strategy", "remove")
    columns = parameters.get("columns")
    if columns is not None and not isinstance(columns, list):
        raise ValueError("null_handling.columns must be a list of column names")
    cols = columns if columns else list(df.columns)

    working = df.copy()
    # Treat empty strings as nulls for cleaning; keep other strings intact.
    for c in cols:
        if c in working.columns:
            working[c] = working[c].replace("", pd.NA)

    if strategy == "remove":
        return working.dropna(subset=cols).reset_index(drop=True)
    if strategy == "fill":
        value = parameters.get("value", "")
        return working.fillna({c: value for c in cols}).reset_index(drop=True)
    raise ValueError("null_handling.strategy must be 'remove' or 'fill'")


def transform_normalize(df: pd.DataFrame, parameters: Dict[str, Any]) -> pd.DataFrame:
    """Trim and/or normalize string casing for selected columns."""
    columns = parameters.get("columns")
    if columns is not None and not isinstance(columns, list):
        raise ValueError("normalize.columns must be a list of column names")
    cols = columns if columns else list(df.columns)

    trim = bool(parameters.get("trim", True))
    case = parameters.get("case")  # "lower" | "upper" | "title" | None

    working = df.copy()
    for c in cols:
        if c not in working.columns:
            continue
        s = working[c]
        # normalize only string-like values
        s = s.astype("string")
        if trim:
            s = s.str.strip()
        if case == "lower":
            s = s.str.lower()
        elif case == "upper":
            s = s.str.upper()
        elif case == "title":
            s = s.str.title()
        working[c] = s
    return working


def apply_pipeline(
    input_bytes: bytes,
    input_format: DatasetFormat,
    steps: Iterable[Tuple[str, Dict[str, Any]]],
    output_format: Optional[DatasetFormat] = None,
) -> Tuple[bytes, DatasetFormat, int]:
    """Apply step sequence, then serialize final DataFrame."""
    df = _df_from_bytes(input_bytes, input_format)

    for step_type, params in steps:
        if step_type == "deduplicate":
            df = transform_deduplicate(df, params)
        elif step_type == "null_handling":
            df = transform_null_handling(df, params)
        elif step_type == "normalize":
            df = transform_normalize(df, params)
        elif step_type == "convert_format":
            # No DataFrame mutation needed. Format conversion happens on output serialization.
            pass
        else:
            raise ValueError(f"unknown transformation type: {step_type}")

    fmt_out = output_format or input_format
    data_out = _df_to_bytes(df, fmt_out)
    return data_out, fmt_out, int(len(df.index))


def apply_single_step(
    input_bytes: bytes,
    input_format: DatasetFormat,
    step_type: str,
    parameters: Dict[str, Any],
    output_format: Optional[DatasetFormat] = None,
) -> Tuple[bytes, DatasetFormat, int]:
    """
    Convenience wrapper for one-step pipelines.
    Backend uses this so each requested step maps to one new DatasetVersion.
    """
    out_bytes, out_fmt, count = apply_pipeline(
        input_bytes=input_bytes,
        input_format=input_format,
        steps=[(step_type, parameters)],
        output_format=output_format,
    )
    return out_bytes, out_fmt, count


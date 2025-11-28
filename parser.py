import json
import os
import pathlib
import tempfile
from typing import IO

import blackboxprotobuf
import ccl_segb.ccl_segb2 as ccl_segb2
import pandas as pd

from helper import _coerce_jsonable, flatten_records


def _resolve_input_to_path(
    input_path: str | bytes | bytearray | IO[bytes],
    original_name: str | None = None,
) -> tuple[str, str | None]:
    """
    Normalize supported inputs to a filesystem path for the SEGB reader.
    Returns (resolved_path, tmp_path). Caller should delete tmp_path if present.
    """
    # Bytes-like -> write to temp file
    if isinstance(input_path, (bytes, bytearray, memoryview)):
        raw = bytes(input_path)
        suffix = "".join(pathlib.Path(original_name or "upload.bin").suffixes)
        tmp = tempfile.NamedTemporaryFile(mode="wb", suffix=suffix, delete=False)
        try:
            tmp.write(raw)
        finally:
            tmp.close()
        return tmp.name, tmp.name

    # File-like -> read and write to temp file
    if hasattr(input_path, "read"):
        data = input_path.read()
        if isinstance(data, str):
            data = data.encode("utf-8", errors="ignore")
        suffix = "".join(pathlib.Path(original_name or "upload.bin").suffixes)
        tmp = tempfile.NamedTemporaryFile(mode="wb", suffix=suffix, delete=False)
        try:
            tmp.write(data)
        finally:
            tmp.close()
        return tmp.name, tmp.name

    # String path (absolute or relative)
    if isinstance(input_path, (str, os.PathLike)):
        input_str = os.fspath(input_path)
        return os.path.normpath(input_str), None

    raise TypeError(f"Unsupported input type for SEGB: {type(input_path)}")


def parse_segb_file(
    input_path: str | bytes | bytearray | IO[bytes],
    original_name: str | None = None,
) -> pd.DataFrame:
    """Parse a SEGB file and return a combined DataFrame of metadata + payload."""
    resolved_path, tmp_to_cleanup = _resolve_input_to_path(
        input_path=input_path,
        original_name=original_name,
    )

    meta_rows = []
    payload_rows = []
    typedef_rows = []

    try:
        for record in ccl_segb2.read_segb2_file(resolved_path):
            offset = getattr(record, "data_start_offset", None)
            metadata_offset = getattr(record.metadata, "metadata_offset", None)
            state = getattr(record.metadata, "state", None)
            state = getattr(state, "name", state)
            ts = getattr(record.metadata, "creation", None)

            try:
                json_str, typedef = blackboxprotobuf.protobuf_to_json(record.data)
                msg = json.loads(json_str)
            except Exception:
                msg = {}
                typedef = {}

            msg = _coerce_jsonable(msg)

            meta_rows.append(
                {
                    "offset": offset,
                    "metadata_offset": metadata_offset,
                    "state": state,
                    "ts": ts,
                }
            )
            payload_rows.append(msg)
            typedef_rows.append({"offset": offset, "typedef": typedef})
    finally:
        if tmp_to_cleanup:
            try:
                os.unlink(tmp_to_cleanup)
            except OSError:
                pass

    meta_df = pd.DataFrame(meta_rows)
    meta_df["ts"] = pd.to_datetime(meta_df["ts"], errors="coerce")

    payload_df = flatten_records(payload_rows, sep=".")
    typedef_df = pd.DataFrame(typedef_rows)
    typedef_df["typedef_json"] = typedef_df["typedef"].apply(
        lambda x: json.dumps(x, ensure_ascii=False)
    )

    df = pd.concat(
        [meta_df.reset_index(drop=True), payload_df.reset_index(drop=True)],
        axis=1,
    )

    df.columns = [
        f"f{c}"
        if isinstance(c, (int, float)) or (isinstance(c, str) and c.isdigit())
        else c
        for c in df.columns
    ]

    return df

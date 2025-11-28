import json
import base64
import pandas as pd
from collections.abc import Mapping, Sequence
import blackboxprotobuf


def _is_base64(s: str) -> bool:
    # blackboxprotobuf emits bytes as base64 strings; be permissive
    if not isinstance(s, str) or len(s) == 0:
        return False
    try:
        base64.b64decode(s, validate=True)
        return True
    except Exception:
        return False


def _maybe_decode_embedded_protobuf(b: bytes):
    """Try to decode a bytes blob as an embedded protobuf using blackboxprotobuf.
    Returns (decoded_obj, typedef) on success, else (None, None)."""
    try:
        j, td = blackboxprotobuf.protobuf_to_json(b)
        return json.loads(j), td
    except Exception:
        return None, None


def _coerce_jsonable(obj):
    """Make values JSON/pandas friendly; detect base64->bytes; try recursive decode."""
    # Mapping: recurse
    if isinstance(obj, Mapping):
        return {k: _coerce_jsonable(v) for k, v in obj.items()}

    # Sequences (but not str/bytes): recurse itemwise
    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        return [_coerce_jsonable(v) for v in obj]

    # Bytes from python: represent as hex for readability (also keep raw)
    if isinstance(obj, (bytes, bytearray)):
        decoded, _ = _maybe_decode_embedded_protobuf(bytes(obj))
        return _coerce_jsonable(decoded) if decoded is not None else obj.hex()

    # Base64 strings from blackboxprotobuf: try to decode, maybe recursively parse
    if _is_base64(obj):
        try:
            raw = base64.b64decode(obj)
            decoded, _ = _maybe_decode_embedded_protobuf(raw)
            return (
                _coerce_jsonable(decoded) if decoded is not None else obj
            )  # keep base64 if not a protobuf
        except Exception:
            return obj

    return obj


def flatten_records(records, sep="."):
    """records = list of dicts. Returns a single pandas.DataFrame with outer-join on columns."""
    # pandas.json_normalize handles nested dicts/lists reasonably well
    frames = []
    for rec in records:
        try:
            frames.append(pd.json_normalize(rec, sep=sep, max_level=None))
        except Exception:
            # fallback: keep as one column if something is wildly irregular
            frames.append(
                pd.DataFrame({"payload_raw": [json.dumps(rec, ensure_ascii=False)]})
            )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).convert_dtypes()
